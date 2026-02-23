#!/usr/bin/env python3
"""
Generate thumbnails for videos by extracting a frame at 1 second,
using filename-based hashing, and storing in a hash-based directory structure.
"""

import sys
import hashlib
import subprocess
from pathlib import Path
from typing import Optional


def extract_frame(video_path: Path, output_path: Path, timestamp: float = 1.0) -> bool:
    """
    Extract a frame from video at specified timestamp using ffmpeg.
    
    Args:
        video_path: Path to input video file
        output_path: Path where thumbnail should be saved
        timestamp: Time in seconds to extract frame (default: 1.0)
    
    Returns:
        True if successful, False otherwise
    """
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
        
        return result.returncode == 0 and output_path.exists()
    except subprocess.TimeoutExpired:
        print(f"Timeout extracting frame from {video_path}")
        return False
    except Exception as e:
        print(f"Error extracting frame from {video_path}: {e}")
        return False


def get_thumbnail_path(data_dir: Path, hash_hex: str, file_limit: int = 1000) -> Path:
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
    if len(hash_hex) < 2:
        raise ValueError("Hash must be at least 2 characters")
    
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

def generate_thumbnail_for_video(video_path: Path, data_dir: Path, timestamp: float = 1.0) -> Optional[str]:
    """
    Generate thumbnail for a video file.
    Returns the hash based on filename, or None if generation failed.
    
    Uses filename-based hashing for simpler lookup - filename is sufficient to identify the video.
    
    Args:
        video_path: Path to video file
        data_dir: Base data directory
        timestamp: Time in seconds to extract frame
    
    Returns:
        Hash of the filename if successful, None otherwise
    """
    # Create temporary output path for extracted frame
    temp_dir = Path('/tmp') if Path('/tmp').exists() else Path.cwd() / 'temp'
    temp_dir.mkdir(exist_ok=True)
    temp_thumbnail = temp_dir / f"temp_{video_path.stem}.jpg"
    
    try:
        # Hash the filename to create a unique identifier
        filename_hash = hashlib.sha256(video_path.name.encode('utf-8')).hexdigest()
        
        # Get destination path
        thumbnail_path = get_thumbnail_path(data_dir, filename_hash)
        
        # Check if thumbnail already exists
        if thumbnail_path.exists():
            # Thumbnail already exists, skip
            return filename_hash
        
        # Extract frame
        if not extract_frame(video_path, temp_thumbnail, timestamp):
            return None
        
        # Create directory if it doesn't exist
        thumbnail_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Move thumbnail to final location
        temp_thumbnail.rename(thumbnail_path)
        return filename_hash
        
    except Exception as e:
        print(f"Error processing {video_path}: {e}")
        if temp_thumbnail.exists():
            temp_thumbnail.unlink()
        return None

def scan_and_process(data_dir: Path, timestamp: float = 1.0, verbose: bool = False):
    """
    Scan data directory for videos and generate thumbnails.
    
    Args:
        data_dir: Base data directory
        timestamp: Time in seconds to extract frame
        verbose: Print progress information
    """
    video_extensions = {'.mp4', '.avi', '.mov', '.mkv', '.webm'}
    processed = 0
    skipped = 0
    errors = 0
    
    # Walk through day directories
    for day_dir in sorted(data_dir.iterdir()):
        if not day_dir.is_dir() or day_dir.name.startswith('.'):
            continue
        
        if verbose:
            print(f"Processing day: {day_dir.name}")
        
        # Walk through event type directories
        for event_dir in sorted(day_dir.iterdir()):
            if not event_dir.is_dir() or event_dir.name.startswith('.') or event_dir.name.lower() == 'test':
                continue
            
            # Process video files
            for video_file in sorted(event_dir.iterdir()):
                if not video_file.is_file():
                    continue
                
                if video_file.suffix.lower() not in video_extensions:
                    continue
                
                # Skip deadbeef files
                if 'deadbeef' in video_file.name.lower():
                    skipped += 1
                    continue
                
                # Check if file has content
                try:
                    if video_file.stat().st_size == 0:
                        skipped += 1
                        continue
                except OSError:
                    skipped += 1
                    continue
                
                hash_hex = generate_thumbnail_for_video(video_file, data_dir, timestamp)
                if hash_hex:
                    processed += 1
                    if verbose:
                        print(f"  ✓ {video_file.name} -> {hash_hex[:8]}...")
                else:
                    errors += 1
                    if verbose:
                        print(f"  ✗ {video_file.name} (error)")
    
    print(f"\nSummary:")
    print(f"  Processed: {processed}")
    print(f"  Skipped: {skipped}")
    print(f"  Errors: {errors}")

def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Generate thumbnails for videos in data directory'
    )
    parser.add_argument(
        'data_dir',
        type=str,
        help='Path to data directory'
    )
    parser.add_argument(
        '--timestamp',
        type=float,
        default=1.0,
        help='Time in seconds to extract frame (default: 1.0)'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Print verbose output'
    )
    
    args = parser.parse_args()
    
    data_dir = Path(args.data_dir).resolve()
    
    if not data_dir.exists():
        print(f"Error: Data directory does not exist: {data_dir}")
        sys.exit(1)
    
    if not data_dir.is_dir():
        print(f"Error: Path is not a directory: {data_dir}")
        sys.exit(1)
    
    # Check if ffmpeg is available
    try:
        subprocess.run(['ffmpeg', '-version'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Error: ffmpeg is not found")
        print("Please place ffmpeg binary in the explorer folder or install it system-wide")
        print("Download from: https://ffmpeg.org/download.html")
        sys.exit(1)
    
    print(f"Generating thumbnails for videos in: {data_dir}")
    print(f"Extracting frame at {args.timestamp} seconds")
    print(f"Thumbnails will be stored in: {data_dir / '.thumbnails'}\n")
    
    scan_and_process(data_dir, args.timestamp, args.verbose)

if __name__ == '__main__':
    main()
