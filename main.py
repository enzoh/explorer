#!/usr/bin/env python3

import http.server
import socketserver
import urllib.parse
import json
import os
import argparse
from pathlib import Path
from datetime import datetime, timedelta

# Only support the formats actually used by the system.
VIDEO_EXTENSIONS = {'.mp4'}
IMAGE_EXTENSIONS = {'.jpg'}

CONTENT_TYPE_MAP = {
    '.mp4': 'video/mp4',
    '.jpg': 'image/jpeg',
}


class PathResolver:
    """Utility class for resolving and validating file system paths."""

    # Set once at startup; all path requests are validated against this.
    _configured_data_path: Path = None

    @classmethod
    def set_configured_path(cls, data_path: str):
        """Resolve and store the single allowed data root at startup."""
        explorer_dir = Path(__file__).parent
        raw = Path(data_path)
        if raw.is_absolute():
            resolved = raw.resolve()
        else:
            resolved = (explorer_dir / raw).resolve()
        cls._configured_data_path = resolved

    @classmethod
    def is_path_within_directory(path: Path, base: Path) -> bool:
        """Return True only if *path* is base itself or a descendant of base."""
        try:
            path.resolve().relative_to(base.resolve())
            return True
        except ValueError:
            return False

    @staticmethod
    def safe_path_component(component: str) -> bool:
        """Return True if a single path component contains no traversal or separators."""
        return (
            bool(component)
            and '..' not in component
            and '/' not in component
            and '\\' not in component
        )


class ThumbnailService:
    """Service for thumbnail operations."""
    
    @staticmethod
    def get_thumbnail_path(data_dir: Path, hash_hex: str, file_limit: int = 1000) -> Path:
        """Get the thumbnail path with recursive directory structure."""
        from generate_thumbnails import get_thumbnail_path as get_thumbnail_path_util
        return get_thumbnail_path_util(data_dir, hash_hex, file_limit)
    
    @staticmethod
    def generate_thumbnail_for_video(video_path: Path, data_dir: Path, timestamp: float = 1.0) -> str:
        """Generate thumbnail for a video file."""
        from generate_thumbnails import generate_thumbnail_for_video as generate_thumbnail_util
        return generate_thumbnail_util(video_path, data_dir, timestamp)


class DataService:
    """Service for data operations (days, videos, etc.)."""
    
    def __init__(self, thumbnail_service: ThumbnailService):
        self.thumbnail_service = thumbnail_service
    
    def list_days(self):
        """List all days in the configured data directory."""
        full_path = PathResolver._configured_data_path
        
        if not full_path.exists() or not full_path.is_dir():
            raise FileNotFoundError(f"Data directory not found: {full_path}")
        
        # List all directories (days with events)
        days_with_events = set()
        for item in sorted(full_path.iterdir()):
            if item.is_dir() and not item.name.startswith('.'):
                days_with_events.add(item.name)
        
        # Generate all dates from first day to last day (inclusive)
        all_days = []
        if days_with_events:
            # Parse first and last day
            sorted_days = sorted(days_with_events)
            first_day_str = sorted_days[0]
            last_day_str = sorted_days[-1]
            
            try:
                first_date = datetime.strptime(first_day_str, '%Y-%m-%d')
                last_date = datetime.strptime(last_day_str, '%Y-%m-%d')
                
                # Generate all dates in range (inclusive)
                current_date = first_date
                while current_date <= last_date:
                    date_str = current_date.strftime('%Y-%m-%d')
                    all_days.append({
                        'date': date_str,
                        'hasEvents': date_str in days_with_events
                    })
                    current_date += timedelta(days=1)
                
            except ValueError:
                # If date parsing fails, fall back to just days with events
                all_days = [{'date': day, 'hasEvents': True} for day in sorted_days]
        else:
            # No days with events, return empty list
            all_days = []
        
        return {'days': all_days}
    
    def get_day_data(self, day: str):
        """Get hourly stats and videos for a day."""
        base_path = PathResolver._configured_data_path
        day_path = base_path / day
        
        if not day_path.exists() or not day_path.is_dir():
            raise FileNotFoundError(f"Day directory not found: {day_path}")
        
        # Initialize hourly counts (0-23 hours) and event type breakdown
        hourly_counts = {str(i).zfill(2): 0 for i in range(24)}
        hourly_by_event_type = {str(i).zfill(2): {} for i in range(24)}
        videos_by_hour = {str(i).zfill(2): [] for i in range(24)}

        # Get all event type directories (excluding test)
        for event_dir in day_path.iterdir():
            if event_dir.is_dir() and not event_dir.name.startswith('.') and event_dir.name.lower() != 'test':
                event_type = event_dir.name
                
                for file_item in event_dir.iterdir():
                    if file_item.is_file():
                        if 'deadbeef' in file_item.name.lower():
                            continue
                        if (file_item.suffix.lower() in VIDEO_EXTENSIONS or 
                            file_item.suffix.lower() in IMAGE_EXTENSIONS):
                            try:
                                if file_item.stat().st_size > 0:
                                    # Extract hour from filename (format: HH-MM-SS-...)
                                    parts = file_item.name.split('-')
                                    if len(parts) >= 3:
                                        try:
                                            file_hour = int(parts[0])
                                            if 0 <= file_hour <= 23:
                                                hour_str = str(file_hour).zfill(2)
                                                
                                                # Update hourly counts
                                                hourly_counts[hour_str] += 1
                                                
                                                # Track by event type
                                                if event_type not in hourly_by_event_type[hour_str]:
                                                    hourly_by_event_type[hour_str][event_type] = 0
                                                hourly_by_event_type[hour_str][event_type] += 1
                                                
                                                    # Extract full timestamp for video entry
                                                    try:
                                                        minutes = int(parts[1])
                                                        seconds = int(parts[2].split('-')[0])
                                                        timestamp = file_hour * 3600 + minutes * 60 + seconds
                                                        
                                                        videos_by_hour[hour_str].append({
                                                            'filename': file_item.name,
                                                            'eventType': event_type,
                                                            'hour': file_hour,
                                                            'minutes': minutes,
                                                            'seconds': seconds,
                                                            'timestamp': timestamp,
                                                        })
                                                    except (ValueError, IndexError):
                                                        pass
                                        except ValueError:
                                            pass
                            except OSError:
                                pass
        
        # Sort videos by timestamp within each hour
        for hour_str in videos_by_hour:
            videos_by_hour[hour_str].sort(key=lambda x: x['timestamp'])
        
        # Convert to sorted format for charting
        result = dict(sorted(hourly_counts.items()))
        result_by_type = {k: dict(sorted(v.items())) for k, v in sorted(hourly_by_event_type.items())}
        videos_by_hour_sorted = {k: videos_by_hour[k] for k in sorted(videos_by_hour.keys())}
        
        return {
            'hourlyStats': result, 
            'hourlyStatsByType': result_by_type,
            'videosByHour': videos_by_hour_sorted,
            'day': day
        }


class ExplorerHandler(http.server.SimpleHTTPRequestHandler):
    """HTTP request handler for the explorer server."""

    @property
    def thumbnail_service(self):
        if not hasattr(self, '_thumbnail_service'):
            self._thumbnail_service = ThumbnailService()
        return self._thumbnail_service

    @property
    def data_service(self):
        if not hasattr(self, '_data_service'):
            self._data_service = DataService(self.thumbnail_service)
        return self._data_service
    
    def log_message(self, format, *args):
        # Log API requests for debugging
        if self.path.startswith('/api/'):
            print(f"API Request: {self.path}")
        # Suppress other HTTP request logs
        pass
    
    def end_headers(self):
        # Add CORS headers to all responses (only GET is used, but CORS helps with browser requests)
        self.send_header('Access-Control-Allow-Origin', '*')
        super().end_headers()
    
    def send_error(self, code, message=None):
        """Override to send JSON errors for API endpoints."""
        if self.path.startswith('/api/'):
            self.send_response(code)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            error_data = {'error': message or self.responses[code][0], 'code': code}
            self.wfile.write(json.dumps(error_data).encode('utf-8'))
        else:
            super().send_error(code, message)
    
    def send_json_response(self, data):
        """Send a JSON response."""
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode('utf-8'))
    
    def do_GET(self):
        """Handle GET requests and route to appropriate handlers."""
        parsed_path = urllib.parse.urlparse(self.path)
        query_params = urllib.parse.parse_qs(parsed_path.query)
        
        # API endpoint: list days
        if parsed_path.path == '/api/list-days':
            self.handle_list_days(query_params)
        # API endpoint: serve video file
        elif parsed_path.path == '/api/video':
            self.handle_serve_video(query_params)
        # API endpoint: get hourly stats and videos for a day
        elif parsed_path.path == '/api/day-data':
            self.handle_day_data(query_params)
            return
        # API endpoint: serve thumbnail
        elif parsed_path.path == '/api/thumbnail':
            self.handle_serve_thumbnail(query_params)
            return
        else:
            # Serve static files normally
            super().do_GET()
    
    def handle_list_days(self, query_params):
        """Handle /api/list-days endpoint."""
        try:
            result = self.data_service.list_days()
            self.send_json_response(result)
        except PermissionError as e:
            self.send_error(403, str(e))
        except FileNotFoundError as e:
            self.send_error(404, str(e))
        except Exception as e:
            import traceback
            print(f"Error listing days: {str(e)}\n{traceback.format_exc()}")
            self.send_error(500, f"Error listing days: {str(e)}")
    
    def handle_serve_video(self, query_params):
        """Handle /api/video endpoint."""
        try:
            day = query_params.get('day', [''])[0]
            event_type = query_params.get('eventType', [''])[0]
            filename = query_params.get('file', [''])[0]

            if not day or not event_type or not filename:
                self.send_error(400, "Missing required parameters")
                return

            # Validate each path component to prevent traversal
            if not all(PathResolver.safe_path_component(c) for c in (day, event_type, filename)):
                self.send_error(400, "Invalid path component")
                return

            base_path = PathResolver._configured_data_path
            file_path = base_path / day / event_type / filename
            
            # Security check: ensure file is within the data directory
            if not PathResolver.is_path_within_directory(file_path, base_path):
                self.send_error(403, "Access denied")
                return

            file_path = file_path.resolve()
            if not file_path.exists() or not file_path.is_file():
                self.send_error(404, "File not found")
                return

            suffix = file_path.suffix.lower()
            content_type = CONTENT_TYPE_MAP.get(suffix, 'application/octet-stream')
            
            # Serve file with range support for video streaming
            file_size = file_path.stat().st_size
            
            range_header = self.headers.get('Range')
            if range_header:
                # Parse range header
                range_match = range_header.replace('bytes=', '').split('-')
                start = int(range_match[0]) if range_match[0] else 0
                end = int(range_match[1]) if range_match[1] else file_size - 1
                
                if start >= file_size or end >= file_size:
                    self.send_error(416, "Range Not Satisfiable")
                    return
                
                # Send partial content
                self.send_response(206)
                self.send_header('Content-Type', content_type)
                self.send_header('Content-Range', f'bytes {start}-{end}/{file_size}')
                self.send_header('Accept-Ranges', 'bytes')
                self.send_header('Content-Length', str(end - start + 1))
                self.end_headers()
                
                with open(file_path, 'rb') as f:
                    f.seek(start)
                    self.wfile.write(f.read(end - start + 1))
            else:
                # Send full file
                self.send_response(200)
                self.send_header('Content-Type', content_type)
                self.send_header('Content-Length', str(file_size))
                self.send_header('Accept-Ranges', 'bytes')
                self.end_headers()
                
                with open(file_path, 'rb') as f:
                    self.wfile.write(f.read())
        except PermissionError as e:
            self.send_error(403, str(e))
        except Exception as e:
            self.send_error(500, f"Error serving video: {str(e)}")
    
    def handle_day_data(self, query_params):
        """Handle /api/day-data endpoint."""
        try:
            day = query_params.get('day', [''])[0]

            if not day:
                self.send_error(400, "Missing day parameter")
                return

            result = self.data_service.get_day_data(day)
            self.send_json_response(result)
        except PermissionError as e:
            self.send_error(403, str(e))
        except FileNotFoundError as e:
            self.send_error(404, str(e))
        except Exception as e:
            self.send_error(500, f"Error getting day data: {str(e)}")
    
    def handle_serve_thumbnail(self, query_params):
        """Handle /api/thumbnail endpoint using only the video path."""
        try:
            video_path_str = query_params.get('video', [''])[0]
            base_path = PathResolver._configured_data_path

            if not video_path_str:
                self.send_error(400, "Missing video parameter")
                return

            # Parse video path: day/eventType/filename
            parts = video_path_str.split('/')
            if len(parts) != 3:
                self.send_error(400, "Invalid video path format")
                return

            day, event_type, filename = parts

            # Validate each component before constructing the path
            if not all(PathResolver.safe_path_component(c) for c in (day, event_type, filename)):
                self.send_error(400, "Invalid path component")
                return

            video_path = base_path / day / event_type / filename

            # Ensure video is within the data directory and exists
            if not PathResolver.is_path_within_directory(video_path, base_path) or not video_path.exists():
                self.send_error(404, "Video not found")
                return

            # If it's a video, generate/use thumbnail and serve it
            if video_path.suffix.lower() in VIDEO_EXTENSIONS:
                hash_hex = self.thumbnail_service.generate_thumbnail_for_video(video_path, base_path)
                if not hash_hex:
                    self.send_error(500, "Failed to generate thumbnail")
                    return

                thumbnail_path = self.thumbnail_service.get_thumbnail_path(base_path, hash_hex)
                if not thumbnail_path.exists() or not thumbnail_path.is_file():
                    self.send_error(500, "Thumbnail file missing after generation")
                    return

                self.send_response(200)
                self.send_header('Content-Type', 'image/jpeg')
                self.send_header('Cache-Control', 'public, max-age=31536000')
                file_size = thumbnail_path.stat().st_size
                self.send_header('Content-Length', str(file_size))
                self.end_headers()
                with open(thumbnail_path, 'rb') as f:
                    self.wfile.write(f.read())
                return

            # If it's an image, serve the image directly
            if video_path.suffix.lower() in IMAGE_EXTENSIONS:
                self.send_response(200)
                content_type = 'image/jpeg' if video_path.suffix.lower() in {'.jpg', '.jpeg'} else 'image/png'
                self.send_header('Content-Type', content_type)
                file_size = video_path.stat().st_size
                self.send_header('Content-Length', str(file_size))
                self.end_headers()
                with open(video_path, 'rb') as f:
                    self.wfile.write(f.read())
                return

            # Unsupported file type for thumbnailing
            self.send_error(400, "Unsupported file type for thumbnail")

        except Exception as e:
            self.send_error(500, f"Error serving thumbnail: {str(e)}")


def run_server(port=8080, data_path=None):
    if data_path:
        PathResolver.set_configured_path(data_path)

    with socketserver.TCPServer(("0.0.0.0", port), ExplorerHandler) as httpd:
        print(f"Explorer server running on http://localhost:{port}")
        if data_path:
            print(f"Data path: {PathResolver._configured_data_path}")
        print(f"Open http://localhost:{port}/index.html in your browser")
        httpd.serve_forever()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Video Explorer Server')
    parser.add_argument('--port', type=int, default=8080, help='Port to run the server on (default: 8080)')
    parser.add_argument('--data-path', type=str, default='/data', 
                       help='Path to the data directory (relative to workspace root or absolute, default: /data)')
    
    args = parser.parse_args()
    
    os.chdir(Path(__file__).parent)
    run_server(port=args.port, data_path=args.data_path)
