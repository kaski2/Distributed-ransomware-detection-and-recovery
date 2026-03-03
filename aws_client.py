"""AWS S3 client utilities for directory snapshotting and scheduled uploads."""

import json
import os
import threading
import hashlib
import base64
from datetime import datetime, timezone
from pathlib import Path
import configparser
import boto3


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


def take_directory_snapshot(directory: str) -> dict:
    """Scan a local directory and return a snapshot dict with file metadata and contents."""
    snapshot = {
        "directory": directory,
        "taken_at": datetime.now(timezone.utc).isoformat(),
        "files": {}
    }

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
                "last_modified": datetime.fromtimestamp(
                    stat.st_mtime, tz=timezone.utc
                ).isoformat(),
                "sha256": sha256.hexdigest(),
                "contents_b64": encoded_contents,
            }
        except (PermissionError, OSError) as e:
            print(f"[Snapshot] Skipping {file_path}: {e}")

    print(f"[Snapshot] Captured {len(snapshot['files'])} files from {directory}")
    return snapshot


def upload_snapshot_to_s3(snapshot: dict) -> bool:
    """Upload a snapshot dict to S3 as JSON. Returns True on success."""
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
    except (boto3.exceptions.Boto3Error, KeyError, OSError) as e:
        print(f"[AWS S3] Failed to upload snapshot: {e}")
        return False


def get_snapshot_from_s3(key: str = None) -> dict | None:
    """Retrieve a snapshot from S3. If key is None, fetches the most recent one."""
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
        print(
            f"[AWS S3] Retrieved snapshot taken at {snapshot.get('taken_at')} "
            f"with {len(snapshot.get('files', {}))} files"
        )
        return snapshot

    except (boto3.exceptions.Boto3Error, KeyError, json.JSONDecodeError) as e:
        print(f"[AWS S3] Failed to retrieve snapshot: {e}")
        return None


def start_snapshot_scheduler(directory: str, interval_seconds: int = 300) -> threading.Thread:
    """Start a daemon thread that snapshots a directory and uploads to S3 on an interval."""
    def _run():
        print(f"[Scheduler] Starting — snapshot every {interval_seconds}s for: {directory}")
        while True:
            try:
                snapshot = take_directory_snapshot(directory)
                upload_snapshot_to_s3(snapshot)
            except (OSError, boto3.exceptions.Boto3Error) as e:
                print(f"[Scheduler] Unexpected error: {e}")
            threading.Event().wait(interval_seconds)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return thread


def main(path, interval):
    """Entry point to start the snapshot scheduler."""
    start_snapshot_scheduler(path, interval)

