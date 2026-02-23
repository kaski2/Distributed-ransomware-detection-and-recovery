import json
import os
import threading
import hashlib
import base64
from datetime import datetime, timezone
from pathlib import Path
import configparser
import boto3
import time

def _get_env_variables():
    config = configparser.ConfigParser()
    config.read_string('[DEFAULT]\n' + open('.aws/variables').read())

    for key, value in config['DEFAULT'].items():
        os.environ[key.upper()] = value.strip('"').strip("'")

def _initialize_s3_client():
    _get_env_variables()
    s3 = boto3.client('s3',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY'],
        aws_secret_access_key=os.environ['AWS_SECRET_KEY'],
        region_name=os.environ['AWS_SERVER']
    )
    return s3


def take_directory_snapshot(directory: str) -> dict:
    snapshot = {
        "directory": directory,
        "taken_at": datetime.now(timezone.utc).isoformat(),
        "files": {}
    }

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
    s3 = _initialize_s3_client()
    try:
        bucket = os.environ['AWS_BUCKET_NAME']
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        key = f"snapshots/snapshot_{timestamp}.json"
        data = json.dumps(snapshot, indent=2)

        s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType="application/json")
        print(f"[AWS S3] Uploaded snapshot to s3://{bucket}/{key}")
        return True
    except Exception as e:
        print(f"[AWS S3] Failed to upload snapshot: {e}")
        return False


def get_snapshot_from_s3(key: str = None) -> dict | None:
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


def start_snapshot_scheduler(directory: str, interval_seconds: int = 300):
    def _run():
        print(f"[Scheduler] Starting — snapshot every {interval_seconds}s for: {directory}")
        while True:
            try:
                snapshot = take_directory_snapshot(directory)
                upload_snapshot_to_s3(snapshot)
            except Exception as e:
                print(f"[Scheduler] Unexpected error: {e}")
            threading.Event().wait(interval_seconds)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return thread


if __name__ == "__main__":
    MONITORED_DIR = "./node1_data"
    SNAPSHOT_INTERVAL = 30 
    start_snapshot_scheduler(MONITORED_DIR, SNAPSHOT_INTERVAL)
    
    print("[Main] Snapshot scheduler running.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[Main] Stopped.")
