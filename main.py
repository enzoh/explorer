#!/usr/bin/env python3

import http.server
import socketserver
import urllib.parse
import json
import os
import argparse
from pathlib import Path
from datetime import datetime, timedelta


class PathResolver:
    """Utility class for resolving data paths."""
    
    @staticmethod
    def resolve_data_path(data_path):
        """Resolve data path relative to the workspace root."""
        # Get the workspace root (parent of explorer directory)
        workspace_root = Path(__file__).parent.parent
        explorer_dir = Path(__file__).parent
        
        # Normalize the path - handle both relative and absolute paths
        if data_path.startswith('/'):
            # Absolute path
            full_path = Path(data_path)
        elif data_path.startswith('../'):
            # Path is relative to explorer directory (e.g., ../SOVEREIGN/data)
            # Resolve from explorer directory
            full_path = (explorer_dir / data_path).resolve()
        else:
            # Relative path from workspace root
            full_path = (workspace_root / data_path).resolve()
        return full_path


class ThumbnailService:
    """Service for thumbnail operations."""
    
    @staticmethod
    def extract_frame(video_path: Path, output_path: Path, timestamp: float = 1.0) -> bool:
        """Extract a frame from video at specified timestamp using ffmpeg."""
        from generate_thumbnails import extract_frame as extract_frame_util
        return extract_frame_util(video_path, output_path, timestamp)
    
    @staticmethod
    def get_thumbnail_path(data_dir: Path, hash_hex: str, file_limit: int = 1000) -> Path:
        """Get the thumbnail path with recursive directory structure."""
        from generate_thumbnails import get_thumbnail_path as get_thumbnail_path_util
        return get_thumbnail_path_util(data_dir, hash_hex, file_limit)
    
    @staticmethod
    def find_thumbnail_path(data_dir: Path, hash_hex: str) -> Path:
        """Find existing thumbnail path by checking the expected location."""
        thumbnail_path = ThumbnailService.get_thumbnail_path(data_dir, hash_hex)
        return thumbnail_path if thumbnail_path.exists() else None
    
    @staticmethod
    def generate_thumbnail_for_video(video_path: Path, data_dir: Path, timestamp: float = 1.0) -> str:
        """Generate thumbnail for a video file."""
        from generate_thumbnails import generate_thumbnail_for_video as generate_thumbnail_util
        return generate_thumbnail_util(video_path, data_dir, timestamp)


class DataService:
    """Service for data operations (days, videos, etc.)."""
    
    def __init__(self, path_resolver: PathResolver, thumbnail_service: ThumbnailService):
        self.path_resolver = path_resolver
        self.thumbnail_service = thumbnail_service
    
    def list_days(self, data_path: str):
        """List all days in the data directory."""
        full_path = self.path_resolver.resolve_data_path(data_path)
        
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
    
    def get_day_data(self, data_path: str, day: str):
        """Get hourly stats and videos for a day."""
        base_path = self.path_resolver.resolve_data_path(data_path)
        day_path = base_path / day
        
        if not day_path.exists() or not day_path.is_dir():
            raise FileNotFoundError(f"Day directory not found: {day_path}")
        
        # Initialize hourly counts (0-23 hours) and event type breakdown
        hourly_counts = {str(i).zfill(2): 0 for i in range(24)}
        hourly_by_event_type = {str(i).zfill(2): {} for i in range(24)}
        videos_by_hour = {str(i).zfill(2): [] for i in range(24)}
        
        # File extensions
        VIDEO_EXTENSIONS = {'.mp4', '.avi', '.mov', '.mkv', '.webm'}
        IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif'}
        
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
                                                    
                                                    # Generate thumbnail hash for this video (only for video files, not images)
                                                    video_hash = None
                                                    if file_item.suffix.lower() in VIDEO_EXTENSIONS:
                                                        try:
                                                            video_hash = self.thumbnail_service.generate_thumbnail_for_video(file_item, base_path)
                                                        except Exception as e:
                                                            # Log error but continue - thumbnail will be generated on-demand
                                                            print(f"Warning: Failed to generate thumbnail for {file_item.name}: {e}")
                                                            pass
                                                    
                                                    videos_by_hour[hour_str].append({
                                                        'filename': file_item.name,
                                                        'eventType': event_type,
                                                        'hour': file_hour,
                                                        'minutes': minutes,
                                                        'seconds': seconds,
                                                        'timestamp': timestamp,
                                                        'thumbnailHash': video_hash
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
    default_data_path = None
    
    @property
    def path_resolver(self):
        """Lazy initialization of path resolver."""
        if not hasattr(self, '_path_resolver'):
            self._path_resolver = PathResolver()
        return self._path_resolver
    
    @property
    def thumbnail_service(self):
        """Lazy initialization of thumbnail service."""
        if not hasattr(self, '_thumbnail_service'):
            self._thumbnail_service = ThumbnailService()
        return self._thumbnail_service
    
    @property
    def data_service(self):
        """Lazy initialization of data service."""
        if not hasattr(self, '_data_service'):
            self._data_service = DataService(self.path_resolver, self.thumbnail_service)
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
        
        # API endpoint: get default data path
        if parsed_path.path == '/api/default-data-path':
            self.send_json_response({'defaultDataPath': self.default_data_path or ''})
            return
        
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
            data_path = query_params.get('path', [''])[0]
            if not data_path:
                self.send_error(400, "Missing path parameter")
                return
            
            result = self.data_service.list_days(data_path)
            self.send_json_response(result)
        except FileNotFoundError as e:
            self.send_error(404, str(e))
        except Exception as e:
            import traceback
            error_msg = f"Error listing days: {str(e)}\n{traceback.format_exc()}"
            print(error_msg)  # Log full error for debugging
            self.send_error(500, f"Error listing days: {str(e)}")
    
    def handle_serve_video(self, query_params):
        """Handle /api/video endpoint."""
        try:
            data_path = query_params.get('path', [''])[0]
            day = query_params.get('day', [''])[0]
            event_type = query_params.get('eventType', [''])[0]
            filename = query_params.get('file', [''])[0]
            
            if not data_path or not day or not event_type or not filename:
                self.send_error(400, "Missing required parameters")
                return
            
            # Resolve path relative to workspace root
            base_path = self.path_resolver.resolve_data_path(data_path)
            file_path = base_path / day / event_type / filename
            
            # Security check: ensure file is within the data directory
            data_dir = base_path.resolve()
            file_path_resolved = file_path.resolve()
            if not str(file_path_resolved).startswith(str(data_dir)):
                self.send_error(403, "Access denied")
                return
            
            if not file_path_resolved.exists() or not file_path_resolved.is_file():
                self.send_error(404, "File not found")
                return
            
            file_path = file_path_resolved
            
            # Determine content type
            suffix = file_path.suffix.lower()
            content_type_map = {
                '.mp4': 'video/mp4',
                '.avi': 'video/x-msvideo',
                '.mov': 'video/quicktime',
                '.mkv': 'video/x-matroska',
                '.webm': 'video/webm',
                '.jpg': 'image/jpeg',
                '.jpeg': 'image/jpeg',
                '.png': 'image/png',
                '.gif': 'image/gif'
            }
            content_type = content_type_map.get(suffix, 'application/octet-stream')
            
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
        except Exception as e:
            self.send_error(500, f"Error serving video: {str(e)}")
    
    def handle_day_data(self, query_params):
        """Handle /api/day-data endpoint."""
        try:
            data_path = query_params.get('path', [''])[0]
            day = query_params.get('day', [''])[0]
            
            if not data_path or not day:
                self.send_error(400, "Missing path or day parameter")
                return
            
            result = self.data_service.get_day_data(data_path, day)
            self.send_json_response(result)
        except FileNotFoundError as e:
            self.send_error(404, str(e))
        except Exception as e:
            self.send_error(500, f"Error getting day data: {str(e)}")
    
    def handle_serve_thumbnail(self, query_params):
        """Handle /api/thumbnail endpoint."""
        try:
            data_path = query_params.get('path', [''])[0]
            hash_hex = query_params.get('hash', [''])[0]
            video_path_str = query_params.get('video', [''])[0]  # Optional: path to video for on-demand generation
            
            if not data_path:
                self.send_error(400, "Missing path parameter")
                return
            
            base_path = self.path_resolver.resolve_data_path(data_path)
            
            # If hash is provided, try to serve existing thumbnail
            if hash_hex:
                # Validate hash format
                if len(hash_hex) != 64 or not all(c in '0123456789abcdefABCDEF' for c in hash_hex):
                    self.send_error(400, "Invalid hash format")
                    return
                
                # Try to find existing thumbnail
                thumbnail_path = self.thumbnail_service.get_thumbnail_path(base_path, hash_hex)
                
                if thumbnail_path.exists() and thumbnail_path.is_file():
                    # Security check
                    data_dir = base_path.resolve()
                    thumbnail_resolved = thumbnail_path.resolve()
                    if str(thumbnail_resolved).startswith(str(data_dir)):
                        self.send_response(200)
                        self.send_header('Content-Type', 'image/jpeg')
                        self.send_header('Cache-Control', 'public, max-age=31536000')
                        file_size = thumbnail_path.stat().st_size
                        self.send_header('Content-Length', str(file_size))
                        self.end_headers()
                        with open(thumbnail_path, 'rb') as f:
                            self.wfile.write(f.read())
                        return
            
            # If video path is provided and thumbnail doesn't exist, generate it
            if video_path_str:
                # Parse video path: day/eventType/filename
                parts = video_path_str.split('/')
                if len(parts) == 3:
                    day, event_type, filename = parts
                    video_path = base_path / day / event_type / filename
                    
                    # Security check: ensure file is within the data directory
                    data_dir = base_path.resolve()
                    video_resolved = video_path.resolve()
                    if str(video_resolved).startswith(str(data_dir)) and video_path.exists():
                        # Check if it's actually a video file (not an image)
                        VIDEO_EXTENSIONS = {'.mp4', '.avi', '.mov', '.mkv', '.webm'}
                        if video_path.suffix.lower() in VIDEO_EXTENSIONS:
                            # Generate thumbnail
                            hash_hex = self.thumbnail_service.generate_thumbnail_for_video(video_path, base_path)
                            if hash_hex:
                                # Serve the newly generated thumbnail
                                thumbnail_path = self.thumbnail_service.get_thumbnail_path(base_path, hash_hex)
                                if thumbnail_path.exists():
                                    self.send_response(200)
                                    self.send_header('Content-Type', 'image/jpeg')
                                    self.send_header('Cache-Control', 'public, max-age=31536000')
                                    file_size = thumbnail_path.stat().st_size
                                    self.send_header('Content-Length', str(file_size))
                                    self.end_headers()
                                    with open(thumbnail_path, 'rb') as f:
                                        self.wfile.write(f.read())
                                    return
                        else:
                            # It's an image, serve it directly
                            self.send_response(200)
                            content_type = 'image/jpeg' if video_path.suffix.lower() in {'.jpg', '.jpeg'} else 'image/png'
                            self.send_header('Content-Type', content_type)
                            file_size = video_path.stat().st_size
                            self.send_header('Content-Length', str(file_size))
                            self.end_headers()
                            with open(video_path, 'rb') as f:
                                self.wfile.write(f.read())
                            return
            
            # Thumbnail not found and couldn't generate
            self.send_error(404, "Thumbnail not found")
                
        except Exception as e:
            self.send_error(500, f"Error serving thumbnail: {str(e)}")


def run_server(port=8080, data_path=None):
    # Set the default data path for the handler
    ExplorerHandler.default_data_path = data_path
    
    with socketserver.TCPServer(("0.0.0.0", port), ExplorerHandler) as httpd:
        print(f"Explorer server running on http://localhost:{port}")
        if data_path:
            print(f"Default data path: {data_path}")
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
