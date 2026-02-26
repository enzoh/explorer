#!/usr/bin/env python3

import argparse
import datetime
import hashlib
import http.server
import json
import logging
import mimetypes
import pathlib
import re
import signal
import socketserver
import subprocess
import threading
import typing
import urllib.parse


def gen_thumbnail(input_file: str, output_file: str):

    logger = logging.getLogger('[gen_thumbnail]')

    # The FFmpeg command.
    command = [
        'ffmpeg', '-ss', '5.0', '-i', input_file, '-ss', '0', '-vframes', '1',
        '-q:v', '2', output_file
    ]

    # Run the FFmpeg command.
    try:
        result = subprocess.run(
            command,
            start_new_session=True,
            stderr=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            timeout=30,
        )

    # Catch timeout.
    except subprocess.TimeoutExpired:
        logger.error('FFmpeg subprocess timed out')
        return

    # Check if an error occured.
    if result.returncode != 0:
        if result.stderr:
            for line in result.stderr.splitlines():
                logger.debug(line.decode('utf-8', errors='replace'))
        logger.warning(
            'FFmpeg subprocess exited with non-zero exit code %d',
            result.returncode,
        )


def get_thumbnail_path(
    thumbnail_dir: pathlib.Path,
    digest: str,
    _offset: int = 0,
    _limit: int = 100,
) -> pathlib.Path:

    flat = thumbnail_dir / (digest + '.jpg')

    # If the thumbnail is already stored at this level, use it.
    if flat.exists():
        return flat

    # If a partition subdirectory for this prefix already exists, recurse into it.
    prefix = digest[_offset:_offset + 2]
    subdir = thumbnail_dir / prefix
    if subdir.is_dir():
        return get_thumbnail_path(subdir, digest, _offset + 2, _limit)

    # New thumbnail: store flat if the directory is under the limit, otherwise partition.
    count = sum(1 for _ in thumbnail_dir.iterdir()) if thumbnail_dir.exists() else 0
    if count < _limit:
        return flat

    subdir.mkdir(parents=True, exist_ok=True)
    return get_thumbnail_path(subdir, digest, _offset + 2, _limit)


class Handler(http.server.BaseHTTPRequestHandler):

    def __init__(self, *args, **kwargs):
        event_file_regex = r'^(\d+)-(\d+)-(\d+)-'
        event_type_regex = r'^(face|smart-motion|tampering)-detection$'
        self._event_file_pattern = re.compile(event_file_regex)
        self._event_type_pattern = re.compile(event_type_regex)
        self._logger = logging.getLogger('[Handler]')
        super().__init__(*args, **kwargs)

    def log_message(self, _format, *_args):
        pass

    def do_GET(self):

        # Log the request.
        path = urllib.parse.urlparse(self.path).path
        self._logger.info(
            'Receiving GET %s from %s:%s',
            path,
            *self.client_address,
        )

        # Match the requested resource.
        try:
            if path == '/':
                file = 'index.html'
                self.send_file(self.config_static_dir, file)
            elif path == '/api/data':
                self.send_data()
            elif path == '/api/list':
                self.send_list()
            elif path.startswith('/data/'):
                file = path.removeprefix('/data/')
                self.send_file(self.config_data_dir, file)
            elif path.startswith('/static/'):
                file = path.removeprefix('/static/')
                self.send_file(self.config_static_dir, file)
            elif path.startswith('/thumbnail/'):
                file = path.removeprefix('/thumbnail/')
                self.send_thumbnail(file)
            else:
                self.send_error(404, 'Not Found')

        # Catch client disconnect.
        except (BrokenPipeError, ConnectionResetError):
            pass

        # An unknown error occured.
        except Exception as err:
            self._logger.exception('Unhandled error while processing request')
            try:
                self.send_error(500, 'Internal Server Error')

            # Catch client disconnect.
            except (BrokenPipeError, ConnectionResetError):
                pass

    def send_data(self):

        # Parse the date.
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        date = params.get('date', [None])[0]
        if date is None:
            self.send_error(400, 'Bad Request')
            return
        try:
            datetime.datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            self.send_error(400, 'Bad Request')
            return

        # Resolve the date directory.
        date_dir = self.config_data_dir.resolve().joinpath(date)

        # Check if date directory exists.
        if not date_dir.exists() or not date_dir.is_dir():
            self.send_error(404, 'Not Found')
            return

        # List all events under the date directory.
        events = []
        for date_dir_item in date_dir.iterdir():
            event_type = date_dir_item.name
            if self._event_type_pattern.fullmatch(event_type):
                event_dir = date_dir.joinpath(event_type)
                for event_dir_item in event_dir.iterdir():
                    event_file = event_dir_item.name
                    if 'deadbeef' in event_file.lower():
                        continue
                    match = self._event_file_pattern.match(event_file)
                    if match:
                        hh, mm, ss = map(int, match.groups())
                        events.append({
                            'event_type': event_type,
                            'file': f'{date}/{event_type}/{event_file}',
                            'timestamp': f'{date}T{hh}:{mm}:{ss}Z'
                        })

        # Sort the events chronologically.
        events.sort(key=lambda event: event['timestamp'])

        # Encode the events in JSON format.
        body = json.dumps(events).encode('utf-8')

        # Set the response headers.
        self.send_response(200)
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Content-Type', 'application/json')
        self.end_headers()

        # Send the JSON-encoded events to the client.
        self.wfile.write(body)

    def send_file(self, prefix: pathlib.Path, suffix: str):

        # Resolve the file.
        path = prefix.resolve()
        file = path.joinpath(suffix)

        # Reject directory traversals.
        if not file.is_relative_to(path):
            self.send_error(403, 'Forbidden')
            return

        # Check if file exists.
        if not file.exists() or not file.is_file():
            self.send_error(404, 'Not Found')
            return

        # Guess the content type.
        content_type, _ = mimetypes.guess_type(file)
        content_type = content_type or 'application/octet-stream'

        # Set the response headers.
        self.send_response(200)
        self.send_header('Cache-Control', 'private, max-age=3600')
        self.send_header('Content-Length', str(file.stat().st_size))
        self.send_header('Content-Type', content_type)
        self.end_headers()

        # Send the file to the client.
        with open(file, 'rb') as handle:
            for chunk in iter(lambda: handle.read(8192), b''):
                self.wfile.write(chunk)

    def send_list(self):

        # List all valid dates in the data directory.
        dates = []
        for item in self.config_data_dir.iterdir():
            date = item.name
            try:
                datetime.datetime.strptime(date, '%Y-%m-%d')
                dates.append(date)
            except ValueError:
                continue

        # Sort the dates chronologically.
        dates.sort()

        # Encode the dates in JSON format.
        body = json.dumps(dates).encode('utf-8')

        # Set the response headers.
        self.send_response(200)
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Content-Type', 'application/json')
        self.end_headers()

        # Send the JSON-encoded dates to the client.
        self.wfile.write(body)

    def send_thumbnail(self, data_suffix: str):

        # Resolve the data directory.
        data_dir = self.config_data_dir.resolve()
        data_file = data_dir.joinpath(data_suffix)

        # Reject directory traversals.
        if not data_file.is_relative_to(data_dir):
            self.send_error(403, 'Forbidden')
            return

        # Check if the data file exists.
        if not data_file.exists() or not data_file.is_file():
            self.send_error(404, 'Not Found')
            return

        # For images, serve the source file directly.
        if data_suffix.endswith('.jpg') or data_suffix.endswith('.jpeg'):
            self.send_file(data_dir, data_suffix)
            return

        # For videos, generate and serve a thumbnail.
        if data_suffix.endswith('.mp4'):
            thumbnail_dir = self.config_thumbnail_dir.resolve()
            digest = hashlib.sha256(data_suffix.encode('utf-8')).hexdigest()
            thumbnail_file = get_thumbnail_path(thumbnail_dir, digest)

            if not thumbnail_file.exists() or not thumbnail_file.is_file():
                thumbnail_file.parent.mkdir(parents=True, exist_ok=True)
                gen_thumbnail(str(data_file), str(thumbnail_file))

            thumbnail_suffix = str(thumbnail_file.relative_to(thumbnail_dir))
            self.send_file(thumbnail_dir, thumbnail_suffix)
            return

        self.send_error(400, 'Bad Request')


# Entry point.
def main():

    # Parse command-line arguments.
    parser = argparse.ArgumentParser(description='Sovereign Data Explorer')
    parser.add_argument(
        '--data-dir',
        default='data',
        help='Data directory (default: data)',
        type=pathlib.Path,
    )
    parser.add_argument(
        '--listen-ip',
        default='0.0.0.0',
        help='IP address to listen on (default: 0.0.0.0)',
        type=str,
    )
    parser.add_argument(
        '--listen-port',
        default=8080,
        help='Port number to listen on (default: 8080)',
        type=int,
    )
    parser.add_argument(
        '--log-level',
        default='INFO',
        help='Granularity of log messages (default: INFO)',
        type=valid_log_level,
    )
    parser.add_argument(
        '--static-dir',
        default='static',
        help='Static assets directory (default: static)',
        type=pathlib.Path,
    )
    parser.add_argument(
        '--thumbnail-dir',
        default='thumbnail',
        help='Thumbnail directory (default: thumbnail)',
        type=pathlib.Path,
    )
    args = parser.parse_args()

    # Define the logger.
    logging.basicConfig(
        datefmt='%H:%M:%S',
        level=args.log_level,
        format='%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s %(message)s',
    )
    logger = logging.getLogger('[main]')

    # Define the web server and thread.
    server = None
    thread = None

    # Define the shutdown event.
    shutdown_event = threading.Event()

    # Define the shutdown handler.
    def shutdown_handler(sig, frame):

        # What kind of signal did we receive?
        name = signal.Signals(sig).name
        logger.info('Received signal %s', name)

        # Shutdown the web server.
        if server is not None:
            server.shutdown()
            server.server_close()

        # Graceful shutdown complete.
        logger.info('Graceful shutdown complete')
        shutdown_event.set()

    # Register the shutdown handler.
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # Configure the request handler.
    class HandlerWithConfig(Handler):
        config_data_dir = args.data_dir
        config_static_dir = args.static_dir
        config_thumbnail_dir = args.thumbnail_dir

    # Start the web server.
    socket_addr = (args.listen_ip, args.listen_port)
    server = http.server.ThreadingHTTPServer(socket_addr, HandlerWithConfig)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info('Listening on port %d', args.listen_port)

    # Wait for the shutdown event.
    shutdown_event.wait()
    thread.join()


# Check if the given string is a valid log level.
def valid_log_level(level: str) -> int:
    try:
        return logging._nameToLevel[level.upper()]
    except KeyError:
        raise argparse.ArgumentTypeError(f'Invalid log level: {level}')


# Start.
if __name__ == '__main__':
    main()