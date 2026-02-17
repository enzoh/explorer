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
from collections import defaultdict

class ExplorerHandler(http.server.SimpleHTTPRequestHandler):
    default_data_path = None
    
    def log_message(self, format, *args):
        # Log API requests for debugging
        if self.path.startswith('/api/'):
            print(f"API Request: {self.path}")
        # Suppress other HTTP request logs
        pass
    
    def end_headers(self):
        # Add CORS headers to all responses
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
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
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()
    
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
        # API endpoint: list event types
        elif parsed_path.path == '/api/list-event-types':
            self.handle_list_event_types(query_params)
        # API endpoint: list videos
        elif parsed_path.path == '/api/list-videos':
            self.handle_list_videos(query_params)
        # API endpoint: serve video file
        elif parsed_path.path == '/api/video':
            self.handle_serve_video(query_params)
        # API endpoint: get event statistics
        elif parsed_path.path == '/api/event-stats':
            self.handle_event_stats(query_params)
        # API endpoint: get hourly stats for a day
        elif parsed_path.path == '/api/hourly-stats':
            self.handle_hourly_stats(query_params)
        # API endpoint: get videos for a specific hour
        elif parsed_path.path == '/api/videos-by-hour':
            self.handle_videos_by_hour(query_params)
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
            from datetime import datetime, timedelta
            
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
                    
                    print(f"[DEBUG] Generated {len(all_days)} days from {first_day_str} to {last_day_str}")
                    print(f"[DEBUG] Days with events: {len(days_with_events)}, Days without events: {len(all_days) - len(days_with_events)}")
                except ValueError as e:
                    # If date parsing fails, fall back to just days with events
                    print(f"[DEBUG] Date parsing error: {e}, falling back to days with events only")
                    all_days = [{'date': day, 'hasEvents': True} for day in sorted_days]
            else:
                # No days with events, return empty list
                all_days = []
            
            self.send_json_response({'days': all_days})
        except Exception as e:
            self.send_error(500, f"Error listing days: {str(e)}")
    
    def handle_list_event_types(self, query_params):
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
            
            # List all directories (event types), excluding 'test'
            event_types = []
            for item in sorted(day_path.iterdir()):
                if item.is_dir() and not item.name.startswith('.') and item.name.lower() != 'test':
                    event_types.append(item.name)
            
            self.send_json_response({'eventTypes': event_types})
        except Exception as e:
            self.send_error(500, f"Error listing event types: {str(e)}")
    
    def handle_list_videos(self, query_params):
        try:
            data_path = query_params.get('path', [''])[0]
            day = query_params.get('day', [''])[0]
            event_type = query_params.get('eventType', [''])[0]
            
            if not data_path or not day or not event_type:
                self.send_error(400, "Missing path, day, or eventType parameter")
                return
            
            # Resolve path relative to workspace root
            base_path = self.resolve_data_path(data_path)
            event_path = base_path / day / event_type
            
            if not event_path.exists() or not event_path.is_dir():
                self.send_error(404, f"Event type directory not found: {event_path}")
                return
            
            # List video and image files, filtering out empty files and dummy files
            videos = []
            video_extensions = {'.mp4', '.avi', '.mov', '.mkv', '.webm'}
            image_extensions = {'.jpg', '.jpeg', '.png', '.gif'}
            
            for item in sorted(event_path.iterdir()):
                if item.is_file() and (item.suffix.lower() in video_extensions or item.suffix.lower() in image_extensions):
                    # Filter out dummy files (contain "deadbeef" in name) and empty files
                    if 'deadbeef' in item.name.lower():
                        continue
                    
                    # Filter out empty files (size > 0)
                    try:
                        if item.stat().st_size > 0:
                            videos.append(item.name)
                    except OSError:
                        # Skip files we can't stat
                        pass
            
            self.send_json_response({'videos': videos})
        except Exception as e:
            self.send_error(500, f"Error listing videos: {str(e)}")
    
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
    
    def handle_event_stats(self, query_params):
        try:
            data_path = query_params.get('path', [''])[0]
            period = query_params.get('period', ['day'])[0]  # day, week, or month
            
            if not data_path:
                self.send_error(400, "Missing path parameter")
                return
            
            # Resolve path relative to workspace root
            base_path = self.resolve_data_path(data_path)
            
            if not base_path.exists() or not base_path.is_dir():
                self.send_error(404, f"Data directory not found: {base_path}")
                return
            
            # Get all day directories
            days = []
            for item in sorted(base_path.iterdir()):
                if item.is_dir() and not item.name.startswith('.'):
                    try:
                        # Try to parse as date
                        datetime.strptime(item.name, '%Y-%m-%d')
                        days.append(item.name)
                    except ValueError:
                        pass
            
            if not days:
                self.send_json_response({'stats': {}})
                return
            
            # Determine date range based on period
            latest_date = datetime.strptime(days[-1], '%Y-%m-%d')
            
            if period == 'day':
                # Last 30 days
                start_date = latest_date - timedelta(days=30)
                date_key_func = lambda d: d.strftime('%Y-%m-%d')
            elif period == 'week':
                # Last 12 weeks
                start_date = latest_date - timedelta(weeks=12)
                def week_key(d):
                    # Get ISO week number
                    year, week, _ = d.isocalendar()
                    return f"{year}-W{week:02d}"
                date_key_func = week_key
            elif period == 'month':
                # Last 12 months
                start_date = latest_date - timedelta(days=365)
                date_key_func = lambda d: d.strftime('%Y-%m')
            else:
                self.send_error(400, "Invalid period. Must be 'day', 'week', or 'month'")
                return
            
            # Collect statistics
            stats = defaultdict(lambda: defaultdict(int))
            event_types = set()
            
            for day_str in days:
                day_date = datetime.strptime(day_str, '%Y-%m-%d')
                
                if day_date < start_date:
                    continue
                
                day_path = base_path / day_str
                if not day_path.exists():
                    continue
                
                # Get all event type directories, excluding 'test'
                for event_dir in day_path.iterdir():
                    if event_dir.is_dir() and not event_dir.name.startswith('.') and event_dir.name.lower() != 'test':
                        event_type = event_dir.name
                        event_types.add(event_type)
                        
                        # Count valid files (excluding deadbeef and empty files)
                        file_count = 0
                        video_extensions = {'.mp4', '.avi', '.mov', '.mkv', '.webm'}
                        image_extensions = {'.jpg', '.jpeg', '.png', '.gif'}
                        
                        for file_item in event_dir.iterdir():
                            if file_item.is_file():
                                if 'deadbeef' in file_item.name.lower():
                                    continue
                                if (file_item.suffix.lower() in video_extensions or 
                                    file_item.suffix.lower() in image_extensions):
                                    try:
                                        if file_item.stat().st_size > 0:
                                            file_count += 1
                                    except OSError:
                                        pass
                        
                        if file_count > 0:
                            date_key = date_key_func(day_date)
                            stats[event_type][date_key] += file_count
            
            # Convert to list format for easier charting
            result = {}
            for event_type in sorted(event_types):
                result[event_type] = dict(sorted(stats[event_type].items()))
            
            self.send_json_response({'stats': result, 'period': period})
        except Exception as e:
            self.send_error(500, f"Error getting event stats: {str(e)}")
    
    def handle_hourly_stats(self, query_params):
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
            
            # Get all event type directories (excluding test)
            for event_dir in day_path.iterdir():
                if event_dir.is_dir() and not event_dir.name.startswith('.') and event_dir.name.lower() != 'test':
                    event_type = event_dir.name
                    # Count valid files and extract hour from filename
                    video_extensions = {'.mp4', '.avi', '.mov', '.mkv', '.webm'}
                    image_extensions = {'.jpg', '.jpeg', '.png', '.gif'}
                    
                    for file_item in event_dir.iterdir():
                        if file_item.is_file():
                            if 'deadbeef' in file_item.name.lower():
                                continue
                            if (file_item.suffix.lower() in video_extensions or 
                                file_item.suffix.lower() in image_extensions):
                                try:
                                    if file_item.stat().st_size > 0:
                                        # Extract hour from filename (format: HH-MM-SS-...)
                                        # Example: "14-38-00-615161000-deadbeef.mp4"
                                        parts = file_item.name.split('-')
                                        if len(parts) >= 3:
                                            try:
                                                hour = int(parts[0])
                                                if 0 <= hour <= 23:
                                                    hour_str = str(hour).zfill(2)
                                                    hourly_counts[hour_str] += 1
                                                    # Track by event type
                                                    if event_type not in hourly_by_event_type[hour_str]:
                                                        hourly_by_event_type[hour_str][event_type] = 0
                                                    hourly_by_event_type[hour_str][event_type] += 1
                                            except ValueError:
                                                pass
                                except OSError:
                                    pass
            
            # Convert to sorted list format for charting
            result = dict(sorted(hourly_counts.items()))
            result_by_type = {k: dict(sorted(v.items())) for k, v in sorted(hourly_by_event_type.items())}
            
            self.send_json_response({
                'hourlyStats': result, 
                'hourlyStatsByType': result_by_type,
                'day': day
            })
        except Exception as e:
            self.send_error(500, f"Error getting hourly stats: {str(e)}")
    
    def handle_videos_by_hour(self, query_params):
        try:
            data_path = query_params.get('path', [''])[0]
            day = query_params.get('day', [''])[0]
            hour = query_params.get('hour', [''])[0]
            
            if not data_path or not day or not hour:
                self.send_error(400, "Missing path, day, or hour parameter")
                return
            
            try:
                hour_int = int(hour)
                if not (0 <= hour_int <= 23):
                    self.send_error(400, "Hour must be between 0 and 23")
                    return
            except ValueError:
                self.send_error(400, "Invalid hour format")
                return
            
            # Resolve path relative to workspace root
            base_path = self.resolve_data_path(data_path)
            day_path = base_path / day
            
            if not day_path.exists() or not day_path.is_dir():
                self.send_error(404, f"Day directory not found: {day_path}")
                return
            
            # Collect all videos from all event types for this hour and nearby hours
            videos = []
            video_extensions = {'.mp4', '.avi', '.mov', '.mkv', '.webm'}
            image_extensions = {'.jpg', '.jpeg', '.png', '.gif'}
            
            # Get videos from hour-1, hour, and hour+1 for context
            for target_hour in [hour_int - 1, hour_int, hour_int + 1]:
                if target_hour < 0 or target_hour > 23:
                    continue
                    
                # Check all event type directories (excluding test)
                for event_dir in day_path.iterdir():
                    if event_dir.is_dir() and not event_dir.name.startswith('.') and event_dir.name.lower() != 'test':
                        for file_item in event_dir.iterdir():
                            if file_item.is_file():
                                if 'deadbeef' in file_item.name.lower():
                                    continue
                                if (file_item.suffix.lower() in video_extensions or 
                                    file_item.suffix.lower() in image_extensions):
                                    try:
                                        if file_item.stat().st_size > 0:
                                            # Extract hour from filename (format: HH-MM-SS-...)
                                            parts = file_item.name.split('-')
                                            if len(parts) >= 3:
                                                try:
                                                    file_hour = int(parts[0])
                                                    if file_hour == target_hour:
                                                        # Extract full timestamp for sorting
                                                        if len(parts) >= 3:
                                                            try:
                                                                minutes = int(parts[1])
                                                                seconds = int(parts[2].split('-')[0])
                                                                timestamp = file_hour * 3600 + minutes * 60 + seconds
                                                                # Generate thumbnail hash for this video (only for video files, not images)
                                                                video_hash = None
                                                                video_extensions = {'.mp4', '.avi', '.mov', '.mkv', '.webm'}
                                                                if file_item.suffix.lower() in video_extensions:
                                                                    try:
                                                                        video_hash = self.generate_thumbnail_for_video(file_item, base_path)
                                                                    except Exception as e:
                                                                        # Log error but continue - thumbnail will be generated on-demand
                                                                        print(f"Warning: Failed to generate thumbnail for {file_item.name}: {e}")
                                                                        pass
                                                                
                                                                videos.append({
                                                                    'filename': file_item.name,
                                                                    'eventType': event_dir.name,
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
            
            # Sort by timestamp (chronologically)
            videos.sort(key=lambda x: x['timestamp'])
            
            self.send_json_response({'videos': videos, 'day': day, 'hour': hour_int})
        except Exception as e:
            self.send_error(500, f"Error getting videos by hour: {str(e)}")
    
    def get_ffmpeg_path(self) -> str:
        """Get the path to ffmpeg, checking local directory first, then system PATH."""
        # Get the directory where this script is located
        script_dir = Path(__file__).parent.resolve()
        
        # Check for ffmpeg in the explorer directory
        # Try different platform-specific names
        import platform
        system = platform.system()
        
        if system == 'Windows':
            local_ffmpeg = script_dir / 'ffmpeg.exe'
        elif system == 'Darwin':  # macOS
            local_ffmpeg = script_dir / 'ffmpeg'
        else:  # Linux
            local_ffmpeg = script_dir / 'ffmpeg'
        
        if local_ffmpeg.exists() and local_ffmpeg.is_file():
            return str(local_ffmpeg)
        
        # Fall back to system ffmpeg
        return 'ffmpeg'
    
    def extract_frame(self, video_path: Path, output_path: Path, timestamp: float = 1.0) -> bool:
        """Extract a frame from video at specified timestamp using ffmpeg."""
        try:
            ffmpeg_path = self.get_ffmpeg_path()
            
            cmd = [
                ffmpeg_path,
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
    
    def hash_file(self, file_path: Path) -> str:
        """Calculate SHA256 hash of a file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    def find_thumbnail_path(self, data_dir: Path, hash_hex: str) -> Path:
        """
        Find existing thumbnail path by searching recursively through directory structure.
        This handles thumbnails that may have been stored at different directory depths.
        
        Args:
            data_dir: Base data directory
            hash_hex: SHA256 hash in hexadecimal
        
        Returns:
            Path object if found, None otherwise
        """
        thumbnails_base = data_dir / '.thumbnails'
        if not thumbnails_base.exists():
            return None
        
        # Try flat location first
        flat_path = thumbnails_base / f"{hash_hex}.jpg"
        if flat_path.exists():
            return flat_path
        
        # Recursively search subdirectories
        # Try all possible depth combinations (up to reasonable limit)
        hash_length = len(hash_hex)
        for depth in range(1, min(8, hash_length // 2) + 1):  # Try up to 8 levels deep
            path_parts = [thumbnails_base]
            for i in range(depth):
                if i * 2 + 2 > hash_length:
                    break
                next_chars = hash_hex[i * 2:(i * 2) + 2].upper()
                path_parts.append(next_chars)
            
            search_path = Path(*path_parts) / f"{hash_hex}.jpg"
            if search_path.exists():
                return search_path
        
        return None
    
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
    
    def get_video_hash_cache_path(self, data_dir: Path) -> Path:
        """Get the path to the video-to-hash cache file."""
        return data_dir / '.thumbnails' / 'video_hash_cache.json'
    
    def load_video_hash_cache(self, data_dir: Path) -> dict:
        """Load the video-to-hash cache from disk."""
        cache_path = self.get_video_hash_cache_path(data_dir)
        if cache_path.exists():
            try:
                with open(cache_path, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}
    
    def save_video_hash_cache(self, data_dir: Path, cache: dict):
        """Save the video-to-hash cache to disk."""
        cache_path = self.get_video_hash_cache_path(data_dir)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(cache_path, 'w') as f:
                json.dump(cache, f, indent=2)
        except Exception as e:
            print(f"[DEBUG] Warning: Could not save video hash cache: {e}")
    
    def get_cached_thumbnail_hash(self, video_path: Path, data_dir: Path) -> str:
        """
        Check if we have a cached hash for this video file.
        Returns the hash if found and thumbnail exists, None otherwise.
        """
        cache = self.load_video_hash_cache(data_dir)
        
        # Use absolute path as key
        video_key = str(video_path.resolve())
        
        if video_key in cache:
            hash_hex = cache[video_key].get('hash')
            # Verify the thumbnail still exists (use find to handle recursive structure)
            if hash_hex:
                thumbnail_path = self.find_thumbnail_path(data_dir, hash_hex)
                if thumbnail_path and thumbnail_path.exists():
                    # Check if video file hasn't been modified (optional optimization)
                    try:
                        video_mtime = video_path.stat().st_mtime
                        cached_mtime = cache[video_key].get('mtime', 0)
                        if video_mtime == cached_mtime:
                            return hash_hex
                    except:
                        # If we can't check mtime, still return hash if thumbnail exists
                        return hash_hex
        
        return None
    
    def cache_thumbnail_hash(self, video_path: Path, data_dir: Path, hash_hex: str):
        """Cache the thumbnail hash for a video file."""
        cache = self.load_video_hash_cache(data_dir)
        video_key = str(video_path.resolve())
        try:
            video_mtime = video_path.stat().st_mtime
            cache[video_key] = {'hash': hash_hex, 'mtime': video_mtime}
            self.save_video_hash_cache(data_dir, cache)
        except Exception as e:
            print(f"[DEBUG] Warning: Could not cache thumbnail hash: {e}")
    
    def generate_thumbnail_for_video(self, video_path: Path, data_dir: Path, timestamp: float = 1.0) -> str:
        """
        Generate thumbnail for a video file.
        Returns the hash of the thumbnail, or None if generation failed.
        
        First checks cache to avoid extracting frame if thumbnail already exists.
        """
        # Check cache first - this avoids extracting frame if we already have the thumbnail
        cached_hash = self.get_cached_thumbnail_hash(video_path, data_dir)
        if cached_hash:
            print(f"[DEBUG] Using cached thumbnail hash for {video_path.name}: {cached_hash[:8]}...")
            return cached_hash
        
        # Cache miss - need to extract frame and generate thumbnail
        import tempfile
        
        # Create temporary directory for extraction
        temp_dir = Path(tempfile.gettempdir())
        temp_thumbnail = temp_dir / f"temp_thumb_{video_path.stem}_{os.getpid()}.jpg"
        
        try:
            # Extract frame (we need this to compute the hash)
            if not self.extract_frame(video_path, temp_thumbnail, timestamp):
                return None
            
            # Hash the extracted frame
            hash_hex = self.hash_file(temp_thumbnail)
            
            # Check if thumbnail already exists (might have been created by another process)
            existing_path = self.find_thumbnail_path(data_dir, hash_hex)
            if existing_path and existing_path.exists():
                # Thumbnail already exists, delete temp and cache the hash
                print(f"[DEBUG] Thumbnail already exists for {video_path.name}, reusing: {hash_hex[:8]}...")
                temp_thumbnail.unlink()
                self.cache_thumbnail_hash(video_path, data_dir, hash_hex)
                return hash_hex
            
            # Get destination path for new thumbnail (uses recursive structure based on file count)
            thumbnail_path = self.get_thumbnail_path(data_dir, hash_hex)
            
            # Thumbnail doesn't exist, create directory and save it
            thumbnail_path.parent.mkdir(parents=True, exist_ok=True)
            temp_thumbnail.rename(thumbnail_path)
            print(f"[DEBUG] Generated new thumbnail for {video_path.name}: {hash_hex[:8]}...")
            
            # Cache the hash for future requests
            self.cache_thumbnail_hash(video_path, data_dir, hash_hex)
            
            return hash_hex
            
        except Exception as e:
            print(f"[DEBUG] Error generating thumbnail for {video_path.name}: {e}")
            # Clean up temp file on error
            if temp_thumbnail.exists():
                try:
                    temp_thumbnail.unlink()
                except:
                    pass
            return None
    
    def handle_serve_thumbnail(self, query_params):
        """Serve a thumbnail image by hash, generating it on-demand if needed."""
        try:
            data_path = query_params.get('path', [''])[0]
            hash_hex = query_params.get('hash', [''])[0]
            video_path_str = query_params.get('video', [''])[0]  # Optional: path to video for on-demand generation
            
            print(f"[DEBUG] Thumbnail request - path: {data_path}, hash: {hash_hex}, video: {video_path_str}")
            
            if not data_path:
                print("[DEBUG] Missing path parameter")
                self.send_error(400, "Missing path parameter")
                return
            
            base_path = self.resolve_data_path(data_path)
            print(f"[DEBUG] Resolved base_path: {base_path}")
            
            # If hash is provided, try to serve existing thumbnail
            if hash_hex:
                # Validate hash format
                if len(hash_hex) != 64 or not all(c in '0123456789abcdefABCDEF' for c in hash_hex):
                    self.send_error(400, "Invalid hash format")
                    return
                
                # Try to find existing thumbnail first (handles recursive structure)
                thumbnail_path = self.find_thumbnail_path(base_path, hash_hex)
                
                if thumbnail_path and thumbnail_path.exists() and thumbnail_path.is_file():
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
                print(f"[DEBUG] Parsed video path parts: {parts}")
                if len(parts) == 3:
                    day, event_type, filename = parts
                    video_path = base_path / day / event_type / filename
                    print(f"[DEBUG] Video path: {video_path}, exists: {video_path.exists()}")
                    
                    # Security check
                    data_dir = base_path.resolve()
                    video_resolved = video_path.resolve()
                    print(f"[DEBUG] Security check - data_dir: {data_dir}, video_resolved: {video_resolved}")
                    if str(video_resolved).startswith(str(data_dir)) and video_path.exists():
                        # Check if it's actually a video file (not an image)
                        video_extensions = {'.mp4', '.avi', '.mov', '.mkv', '.webm'}
                        if video_path.suffix.lower() in video_extensions:
                            print(f"[DEBUG] Generating thumbnail for video: {video_path}")
                            # Generate thumbnail
                            hash_hex = self.generate_thumbnail_for_video(video_path, base_path)
                            print(f"[DEBUG] Generated hash: {hash_hex}")
                            if hash_hex:
                                # Serve the newly generated thumbnail
                                thumbnail_path = self.get_thumbnail_path(base_path, hash_hex)
                                print(f"[DEBUG] Thumbnail path: {thumbnail_path}, exists: {thumbnail_path.exists()}")
                                if thumbnail_path.exists():
                                    print(f"[DEBUG] Serving generated thumbnail: {thumbnail_path}")
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
                                    print(f"[DEBUG] ERROR: Thumbnail was generated but file doesn't exist at {thumbnail_path}")
                            else:
                                print(f"[DEBUG] ERROR: Thumbnail generation returned None for {video_path}")
                        else:
                            # It's an image, serve it directly
                            print(f"[DEBUG] Serving image directly: {video_path}")
                            self.send_response(200)
                            content_type = 'image/jpeg' if video_path.suffix.lower() in {'.jpg', '.jpeg'} else 'image/png'
                            self.send_header('Content-Type', content_type)
                            file_size = video_path.stat().st_size
                            self.send_header('Content-Length', str(file_size))
                            self.end_headers()
                            with open(video_path, 'rb') as f:
                                self.wfile.write(f.read())
                            return
                    else:
                        print(f"[DEBUG] Security check failed or video doesn't exist")
                else:
                    print(f"[DEBUG] Invalid video path format, expected 3 parts, got {len(parts)}")
            
            # Thumbnail not found and couldn't generate
            print(f"[DEBUG] Returning 404 - thumbnail not found and couldn't generate")
            self.send_error(404, "Thumbnail not found")
                
        except Exception as e:
            print(f"[DEBUG] Exception in handle_serve_thumbnail: {e}")
            import traceback
            traceback.print_exc()
            self.send_error(500, f"Error serving thumbnail: {str(e)}")
    
    def send_json_response(self, data):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode('utf-8'))


def run_server(port=8080, data_path=None):
    # Set the default data path for the handler
    ExplorerHandler.default_data_path = data_path
    
    with socketserver.TCPServer(("", port), ExplorerHandler) as httpd:
        print(f"Explorer server running on http://localhost:{port}")
        if data_path:
            print(f"Default data path: {data_path}")
        print(f"Open http://localhost:{port}/index.html in your browser")
        httpd.serve_forever()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Video Explorer Server')
    parser.add_argument('--port', type=int, default=8080, help='Port to run the server on (default: 8080)')
    parser.add_argument('--data-path', type=str, default=None, 
                       help='Path to the data directory (relative to workspace root or absolute)')
    
    args = parser.parse_args()
    
    # Prompt for data path if not provided
    data_path = args.data_path
    if not data_path:
        data_path = input('Enter path to data directory: ').strip()
        if not data_path:
            print('Error: Data path is required')
            exit(1)
    
    os.chdir(Path(__file__).parent)
    run_server(port=args.port, data_path=data_path)
