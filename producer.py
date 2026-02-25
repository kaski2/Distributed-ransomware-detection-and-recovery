from kafka import KafkaProducer
from pathlib import Path
import sys
import time
import os
import configparser
from datetime import datetime, timezone
import json
import binascii
from aws_client import start_snapshot_scheduler

topic = 'file-monitoring'

CONFIG_FILE = Path("settings.ini")

REQUIRED_SETTINGS = {
    "settings": ["MONITORED_DIR_PATH"]
}

def load_config():
    config = configparser.ConfigParser()
    try:
        if CONFIG_FILE.exists():
            config.read(CONFIG_FILE)
        else:
            print("Configuration file not found. Creating one with default values.")
            CONFIG_FILE.touch()
    except configparser.DuplicateSectionError as e:
        print(f"[Error] duplicate section in configuration file: {e}")
        sys.exit(1)
    except configparser.ParsingError as e:
        print(f"[Error] parsing error in settings.ini: {e}")
        sys.exit(1)
    except configparser.DuplicateOptionError as e:
        print(f"[Error] duplicate option in settings.ini: under section: {e.section} option: {e.option}")
        sys.exit(1)

    modified = False
    missing = []

    for section, options in REQUIRED_SETTINGS.items():
        if not config.has_section(section):
            config.add_section(section)
            modified = True

        for option in options:
            if not config.has_option(section, option) or not config[section][option].strip():
                config.set(section, option, "")
                modified = True
                missing.append(f"{section}.{option}")

    if modified:
        with open(CONFIG_FILE, "w") as configfile:
            config.write(configfile)

    if missing:
        print("ERROR MISSING MANDATORY SETTINGS IN settings.ini:")
        for item in missing:
            print(f" {item}")
        print("fill these missing values in settings.ini")
        sys.exit(1)
    return config


def send_event(event_producer, event_data):
    print(f"[DEBUG] Attempting to send to topic '{topic}': {event_data}")
    try:
        future = event_producer.send(topic, value=str(event_data).encode('utf-8'))
        record_metadata = future.get(timeout=10)
        print(f"Event sent: {event_data['event_type']} - {event_data['file_path']}")
        print(f"[DEBUG] Successfully sent to partition {record_metadata.partition}, offset {record_metadata.offset}")
    except Exception as e:
        print(f"[ERROR] Failed to send event: {e}")


def read_file_contents(file_path, max_size_bytes=1024*1024):
    """
    Safely read file contents. Returns either text content or base64-encoded binary.
    max_size_bytes: Maximum file size to read (default 1MB)
    """
    try:
        stat = os.stat(file_path)
        if stat.st_size > max_size_bytes:
            return f"<File too large: {stat.st_size} bytes, max: {max_size_bytes} bytes>"
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                return f.read()
        except Exception:
            with open(file_path, 'rb') as f:
                binary_data = f.read()
            return f"Binary file (hex): {binascii.hexlify(binary_data[:100]).decode('utf-8')}..."
    except Exception as e:
        return f"Error reading file: {e}"


def monitor_directory(event_producer, path, poll_interval=2):
    print(f"[DEBUG] Starting polling-based monitoring on path: {path} (interval: {poll_interval}s)")
    if not os.path.exists(path):
        print(f"[ERROR] Monitored path does not exist: {path}")
        sys.exit(1)

    # Track file state: path -> (mtime, size)
    file_state = {}

    # Initial scan to populate state without sending events
    for root, dirs, files in os.walk(path):
        for filename in files:
            file_path = os.path.join(root, filename)
            try:
                stat = os.stat(file_path)
                file_state[file_path] = (stat.st_mtime, stat.st_size)
            except OSError:
                pass

    print(f"[DEBUG] Initial scan complete. Tracking {len(file_state)} files.")

    while True:
        time.sleep(poll_interval)
        try:
            current_files = set()

            for root, dirs, files in os.walk(path):
                for filename in files:
                    file_path = os.path.join(root, filename)
                    current_files.add(file_path)
                    try:
                        stat = os.stat(file_path)
                        mtime, size = stat.st_mtime, stat.st_size
                    except OSError:
                        continue

                    if file_path not in file_state:
                        # New file created
                        file_state[file_path] = (mtime, size)
                        event_data = {
                            'timestamp': datetime.now(timezone.utc).isoformat(),
                            'event_type': 'created',
                            'file_path': file_path,
                            'is_directory': False,
                            'file_extension': Path(file_path).suffix,
                            'file_name': Path(file_path).name,
                            'file_contents': read_file_contents(file_path),
                        }
                        send_event(event_producer, event_data)

                    elif (mtime, size) != file_state[file_path]:
                        # File modified
                        file_state[file_path] = (mtime, size)
                        event_data = {
                            'timestamp': datetime.now(timezone.utc).isoformat(),
                            'event_type': 'modified',
                            'file_path': file_path,
                            'is_directory': False,
                            'file_extension': Path(file_path).suffix,
                            'file_name': Path(file_path).name,
                            'file_contents': read_file_contents(file_path),
                        }
                        send_event(event_producer, event_data)

            # Detect deleted files
            deleted = set(file_state.keys()) - current_files
            for file_path in deleted:
                del file_state[file_path]
                event_data = {
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'event_type': 'deleted',
                    'file_path': file_path,
                    'is_directory': False,
                    'file_extension': Path(file_path).suffix,
                    'file_name': Path(file_path).name,
                }
                send_event(event_producer, event_data)

        except Exception as e:
            print(f"[ERROR] Error during directory scan: {e}")


def main( path, poll_interval, kafka_servers):
    print(f"[DEBUG] Connecting to Kafka at: {kafka_servers}")
    print(f"[DEBUG] Path: {path}")
    try:
        event_producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            retries=5,
            retry_backoff_ms=1000,
        )
        print(f"[DEBUG] KafkaProducer created successfully")
    except Exception as e:
        print(f"[ERROR] Failed to create KafkaProducer: {e}")
        sys.exit(1)

    monitor_directory(event_producer, path, poll_interval)
