import json
import os
import threading
import hashlib
import base64
from datetime import datetime, timezone
from pathlib import Path
import configparser
import boto3
from kafka import KafkaConsumer
from watchdog.watchmedo import load_config



def _get_env_variables():
    
    aws_vars_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.aws', 'variables')
    config = configparser.ConfigParser()
    config.read_string('[DEFAULT]\n' + open(aws_vars_path).read())

    for key, value in config['DEFAULT'].items():
        os.environ[key.upper()] = value.strip('"').strip("'")

def _initialize_s3_client() -> boto3.client:
    _get_env_variables()
    s3 = boto3.client('s3',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY'],
        aws_secret_access_key=os.environ['AWS_SECRET_KEY'],
        region_name=os.environ['AWS_SERVER']
    )
    return s3


def get_file_state_from_kafka(kafka_servers: str, topic: str = 'file-monitoring', timeout_ms: int = 5000) -> dict:
    """
    Consumes messages from Kafka and builds the current file state.
    Returns a dict mapping file_path -> {size_bytes, last_modified, sha256, contents_b64, event_type}
    """
    file_state = {}
    
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_servers,
            consumer_timeout_ms=timeout_ms,
            group_id='snapshot-consumer',
            auto_offset_reset='earliest'
        )
        
        for message in consumer:
            try:
                event_data = json.loads(message.value.decode('utf-8'))
                file_path = event_data.get('file_path')
                event_type = event_data.get('event_type')
                
                if event_type == 'deleted':
                    # Remove from state if file was deleted
                    if file_path in file_state:
                        del file_state[file_path]
                
                elif event_type in ['created', 'modified']:
                    file_contents = event_data.get('file_contents', '')
                    
                    # Handle different content formats
                    if isinstance(file_contents, str):
                        if file_contents.startswith('<') or file_contents.startswith('Binary'):
                            # Skip unreadable/error messages
                            continue
                        # Encode the file contents to base64
                        contents_b64 = base64.b64encode(file_contents.encode('utf-8')).decode('utf-8')
                    else:
                        contents_b64 = base64.b64encode(str(file_contents).encode('utf-8')).decode('utf-8')
                    
                    # Calculate SHA256 of the contents
                    sha256_hash = hashlib.sha256(file_contents.encode('utf-8')).hexdigest()
                    
                    file_state[file_path] = {
                        'size_bytes': event_data.get('file_size', len(file_contents)),
                        'last_modified': event_data.get('timestamp', datetime.now(timezone.utc).isoformat()),
                        'sha256': sha256_hash,
                        'contents_b64': contents_b64,
                        'event_type': event_type
                    }
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                print(f"[Kafka] Error parsing message: {e}")
                continue
        
        consumer.close()
        print(f"[Kafka] Processed messages, current state has {len(file_state)} files")
        
    except Exception as e:
        print(f"[Kafka] Error consuming from topic '{topic}': {e}")
    
    return file_state


def take_directory_snapshot(directory: str, kafka_servers: str = None) -> dict:
    '''
    Takes a snapshot of file state from Kafka events.
    If kafka_servers is not provided, falls back to directory scanning.
    '''
    snapshot = {
        "directory": directory,
        "taken_at": datetime.now(timezone.utc).isoformat(),
        "files": {}
    }

    if kafka_servers:
        # Get file state from Kafka
        print(f"[Snapshot] Retrieving file state from Kafka ({kafka_servers})...")
        file_state = get_file_state_from_kafka(kafka_servers)
        
        # Convert to snapshot format
        for file_path, file_info in file_state.items():
            relative = file_path
            snapshot["files"][relative] = {
                "size_bytes": file_info['size_bytes'],
                "last_modified": file_info['last_modified'],
                "sha256": file_info['sha256'],
                "contents_b64": file_info['contents_b64'],
            }
    else:
        # Fallback: scan directory directly
        print(f"[Snapshot] Scanning directory: {directory}")
        base = Path(directory)
        for file_path in base.rglob("*"):
            if not file_path.is_file():
                continue
            try:
                relative = str(file_path.relative_to(base))
                stat = file_path.stat()

                sha256 = hashlib.sha256()
                file_contents = b""
                with open(file_path, "rb") as f:
                    for chunk in iter(lambda: f.read(8192), b""):
                        sha256.update(chunk)
                        file_contents += chunk

                encoded_contents = base64.b64encode(file_contents).decode('utf-8')

                snapshot["files"][relative] = {
                    "size_bytes": stat.st_size,
                    "last_modified": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                    "sha256": sha256.hexdigest(),
                    "contents_b64": encoded_contents,
                }
            except (PermissionError, OSError) as e:
                print(f"[Snapshot] Skipping {file_path}: {e}")

    print(f"[Snapshot] Captured {len(snapshot['files'])} files from {directory}")
    return snapshot


def upload_snapshot_to_s3(snapshot: dict) -> bool:
    '''
    Uploads the given snapshot to S3 as a JSON file.
    '''
    s3 = _initialize_s3_client()
    try:
        
        config = configparser.ConfigParser()
        config.read('/app/settings.ini')
        node_id = config.get('agent', 'node_id', fallback='unknown')
        
        bucket = os.environ['AWS_BUCKET_NAME']
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        key = f"snapshots/{node_id}_snapshot_{timestamp}.json"
        data = json.dumps(snapshot, indent=2)

        s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType="application/json")
        print(f"[AWS S3] Uploaded snapshot to s3://{bucket}/{key}")
        return True
    except Exception as e:
        print(f"[AWS S3] Failed to upload snapshot: {e}")
        return False


def get_snapshot_from_s3(key: str = None) -> dict | None:
    '''
    Retrieves a snapshot from S3.
    If `key` is None, retrieves the most recently uploaded snapshot.
    '''
    try:
        s3 = _initialize_s3_client()
        bucket = os.environ['AWS_BUCKET_NAME']

        if key is None:
            # List all snapshots and pick the most recent one
            response = s3.list_objects_v2(Bucket=bucket, Prefix="snapshots/")
            objects = response.get("Contents", [])
            if not objects:
                print("[AWS S3] No snapshots found in bucket.")
                return None
            latest = max(objects, key=lambda o: o["LastModified"])
            key = latest["Key"]
            print(f"[AWS S3] Latest snapshot found: {key} (modified {latest['LastModified']})")

        print(f"[AWS S3] Retrieving s3://{bucket}/{key}")
        response = s3.get_object(Bucket=bucket, Key=key)
        data = response['Body'].read().decode('utf-8')
        snapshot = json.loads(data)
        print(f"[AWS S3] Retrieved snapshot taken at {snapshot.get('taken_at')} with {len(snapshot.get('files', {}))} files")
        return snapshot

    except Exception as e:
        print(f"[AWS S3] Failed to retrieve snapshot: {e}")
        return None


def start_snapshot_scheduler(directory: str, interval_seconds: int = 300, kafka_servers: str = None) -> threading.Thread:
    '''
    Starts a background thread that periodically takes snapshots (from Kafka or directory) and uploads them to S3.
    kafka_servers: Optional Kafka bootstrap servers. If provided, snapshots are built from Kafka events.
    '''
    def _run():
        print(f"[Scheduler] Starting — snapshot every {interval_seconds}s for: {directory}")
        if kafka_servers:
            print(f"[Scheduler] Using Kafka ({kafka_servers}) as data source")
        while True:
            try:
                snapshot = take_directory_snapshot(directory, kafka_servers)
                upload_snapshot_to_s3(snapshot)
            except Exception as e:
                print(f"[Scheduler] Unexpected error: {e}")
            threading.Event().wait(interval_seconds)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return thread

def main(path, interval, kafka_servers):
    print("[testi5]")
    start_snapshot_scheduler(path, interval, kafka_servers)
    print("[testi6]")

