#!/usr/bin/env python3

import http.server
import socketserver
import urllib.parse
import json
import os
import argparse
import subprocess
import hashlib
from pathlib import Path
from datetime import datetime, timedelta

class ExplorerHandler(http.server.SimpleHTTPRequestHandler):
    default_data_path = None
    
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
    
    def do_GET(self):
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
    
    def resolve_data_path(self, data_path):
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
    
    def handle_list_days(self, query_params):
        try:
            data_path = query_params.get('path', [''])[0]
            if not data_path:
                self.send_error(400, "Missing path parameter")
                return
            
            # Resolve path relative to workspace root
            full_path = self.resolve_data_path(data_path)
            
            if not full_path.exists() or not full_path.is_dir():
                self.send_error(404, f"Data directory not found: {full_path}")
                return
            
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
            
            self.send_json_response({'days': all_days})
        except Exception as e:
            self.send_error(500, f"Error listing days: {str(e)}")
    
    def handle_serve_video(self, query_params):
        try:
            data_path = query_params.get('path', [''])[0]
            day = query_params.get('day', [''])[0]
            event_type = query_params.get('eventType', [''])[0]
            filename = query_params.get('file', [''])[0]
            
            if not data_path or not day or not event_type or not filename:
                self.send_error(400, "Missing required parameters")
                return
            
            # Resolve path relative to workspace root
            base_path = self.resolve_data_path(data_path)
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
        """Combined handler that returns hourly stats and all videos for a day."""
        try:
            data_path = query_params.get('path', [''])[0]
            day = query_params.get('day', [''])[0]
            
            if not data_path or not day:
                self.send_error(400, "Missing path or day parameter")
                return
            
            # Resolve path relative to workspace root
            base_path = self.resolve_data_path(data_path)
            day_path = base_path / day
            
            if not day_path.exists() or not day_path.is_dir():
                self.send_error(404, f"Day directory not found: {day_path}")
                return
            
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
                                                                video_hash = self.generate_thumbnail_for_video(file_item, base_path)
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
            
            self.send_json_response({
                'hourlyStats': result, 
                'hourlyStatsByType': result_by_type,
                'videosByHour': videos_by_hour_sorted,
                'day': day
            })
        except Exception as e:
            self.send_error(500, f"Error getting day data: {str(e)}")
    
    def extract_frame(self, video_path: Path, output_path: Path, timestamp: float = 1.0) -> bool:
        """Extract a frame from video at specified timestamp using ffmpeg."""
        try:
            cmd = [
                'ffmpeg',
                '-ss', str(timestamp),
                '-i', str(video_path),
                '-vframes', '1',
                '-q:v', '2',
                '-y',
                str(output_path)
            ]
            
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=30
            )
            
            if result.returncode != 0:
                # Log ffmpeg error for debugging
                stderr_output = result.stderr.decode('utf-8', errors='ignore')
                print(f"ffmpeg error for {video_path.name}: {stderr_output[:200]}")
                return False
            
            return output_path.exists()
        except FileNotFoundError:
            print("Error: ffmpeg not found. Please place ffmpeg binary in the explorer folder or install it system-wide.")
            return False
        except subprocess.TimeoutExpired:
            print(f"Timeout extracting frame from {video_path.name}")
            return False
        except Exception as e:
            print(f"Error extracting frame from {video_path.name}: {e}")
            return False
    
    def find_thumbnail_path(self, data_dir: Path, hash_hex: str) -> Path:
        """
        Find existing thumbnail path by checking the expected location.
        Uses get_thumbnail_path to determine where it should be, then checks if it exists.
        
        Args:
            data_dir: Base data directory
            hash_hex: SHA256 hash in hexadecimal
        
        Returns:
            Path object if found, None otherwise
        """
        thumbnail_path = self.get_thumbnail_path(data_dir, hash_hex)
        return thumbnail_path if thumbnail_path.exists() else None
    
    def get_thumbnail_path(self, data_dir: Path, hash_hex: str, file_limit: int = 1000) -> Path:
        """
        Get the thumbnail path with recursive directory structure.
        Starts flat, then splits into subdirectories based on hash when file limit is reached.
        
        Args:
            data_dir: Base data directory
            hash_hex: SHA256 hash in hexadecimal
            file_limit: Maximum number of files per directory before splitting
        
        Returns:
            Path object for the thumbnail
        """
        thumbnails_base = data_dir / '.thumbnails'
        thumbnails_base.mkdir(parents=True, exist_ok=True)
        
        # Start with flat structure
        current_dir = thumbnails_base
        hash_index = 0
        hash_length = len(hash_hex)
        
        # Recursively check and split directories based on file count
        while hash_index < hash_length - 2:  # Need at least 2 chars for next level
            # Count files (not directories) in current directory
            try:
                file_count = sum(1 for item in current_dir.iterdir() if item.is_file())
            except (OSError, FileNotFoundError):
                file_count = 0
            
            # If under limit, store here
            if file_count < file_limit:
                return current_dir / f"{hash_hex}.jpg"
            
            # Over limit, need to split - use next 2 characters of hash
            next_chars = hash_hex[hash_index:hash_index + 2].upper()
            current_dir = current_dir / next_chars
            current_dir.mkdir(parents=True, exist_ok=True)
            hash_index += 2
        
        # Fallback: if we've exhausted hash characters, store in deepest directory
        return current_dir / f"{hash_hex}.jpg"
    
    def generate_thumbnail_for_video(self, video_path: Path, data_dir: Path, timestamp: float = 1.0) -> str:
        """
        Generate thumbnail for a video file.
        Returns the hash based on filename, or None if generation failed.
        
        Uses filename hash for simpler lookup - filename is sufficient to identify the video.
        """
        # Hash the filename to create a unique identifier
        filename_hash = hashlib.sha256(video_path.name.encode('utf-8')).hexdigest()
        
        # Check if thumbnail already exists
        thumbnail_path = self.get_thumbnail_path(data_dir, filename_hash)
        if thumbnail_path.exists():
            return filename_hash
        
        # Thumbnail doesn't exist - extract frame and generate it
        import tempfile
        
        # Create temporary directory for extraction
        temp_dir = Path(tempfile.gettempdir())
        temp_thumbnail = temp_dir / f"temp_thumb_{video_path.stem}.jpg"
        
        try:
            # Extract frame
            if not self.extract_frame(video_path, temp_thumbnail, timestamp):
                return None
            
            # Get destination path for new thumbnail (uses recursive structure based on file count)
            thumbnail_path = self.get_thumbnail_path(data_dir, filename_hash)
            
            # Thumbnail doesn't exist, create directory and save it
            thumbnail_path.parent.mkdir(parents=True, exist_ok=True)
            temp_thumbnail.rename(thumbnail_path)
            
            return filename_hash
            
        except Exception as e:
            # Clean up temp file on error
            if temp_thumbnail.exists():
                try:
                    temp_thumbnail.unlink()
                except OSError:
                    pass
            return None
    
    def handle_serve_thumbnail(self, query_params):
        """Serve a thumbnail image by hash, generating it on-demand if needed."""
        try:
            data_path = query_params.get('path', [''])[0]
            hash_hex = query_params.get('hash', [''])[0]
            video_path_str = query_params.get('video', [''])[0]  # Optional: path to video for on-demand generation
            
            if not data_path:
                self.send_error(400, "Missing path parameter")
                return
            
            base_path = self.resolve_data_path(data_path)
            
            # If hash is provided, try to serve existing thumbnail
            if hash_hex:
                # Validate hash format
                if len(hash_hex) != 64 or not all(c in '0123456789abcdefABCDEF' for c in hash_hex):
                    self.send_error(400, "Invalid hash format")
                    return
                
                # Try to find existing thumbnail
                thumbnail_path = self.get_thumbnail_path(base_path, hash_hex)
                
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
                            hash_hex = self.generate_thumbnail_for_video(video_path, base_path)
                            if hash_hex:
                                # Serve the newly generated thumbnail
                                thumbnail_path = self.get_thumbnail_path(base_path, hash_hex)
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
    
    def send_json_response(self, data):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode('utf-8'))


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
