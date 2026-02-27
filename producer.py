from kafka import KafkaProducer
from pathlib import Path
import sys
import time
import os
import json
import configparser
from datetime import datetime, timezone
import binascii

from alerting import AlertDetector, AlertManager, AlertSigner, GossipNode


topic = 'security-alerts'

def send_alert(event_producer, alert_data):
    print(f"[DEBUG] Attempting to send alert to topic '{topic}': {alert_data}")
    try:
        future = event_producer.send(topic, value=alert_data)
        record_metadata = future.get(timeout=10)
        print(f"Alert sent: {alert_data['alert_type']} - {alert_data['details'].get('file_path', '')}")
        print(f"[DEBUG] Successfully sent to partition {record_metadata.partition}, offset {record_metadata.offset}")
    except Exception as e:
        print(f"[ERROR] Failed to send alert: {e}")

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


def monitor_directory(path, on_event, poll_interval=2):
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
                        on_event(event_data)

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
                        on_event(event_data)

            # Detect deleted files
            deleted = set(file_state.keys()) - current_files
            for file_path in deleted:
                del file_state[file_path]

        except Exception as e:
            print(f"[ERROR] Error during directory scan: {e}")


def main( path, poll_interval, kafka_servers, config):
    print(f"[DEBUG] Connecting to Kafka at: {kafka_servers}")
    print(f"[DEBUG] Path: {path}")
    node_id = config['agent']["node_id"]
    keywords = config['detection']["ransom_note_keywords"].split(",")
    print(f"[DEBUG] Alert keywords: {keywords}")
    print(f"[DEBUG] Gossip peers: {config['gossip']['peers']}")

    try:
        event_producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            retries=5,
            retry_backoff_ms=1000,
            value_serializer=lambda value: json.dumps(value).encode('utf-8'),
        )
        print(f"[DEBUG] KafkaProducer created successfully")
    except Exception as e:
        print(f"[ERROR] Failed to create KafkaProducer: {e}")
        sys.exit(1)

    signer = AlertSigner(config["security"]["shared_secret"])

    def on_alert(alert, source):
        payload = dict(alert)
        payload["ingestion_source"] = source
        send_alert(event_producer, payload)

    gossip_node = GossipNode(
        node_id=node_id,
        listen_host=config["gossip"]["listen_host"],
        listen_port=config["gossip"]["listen_port"],
        peers=config["gossip"]["peers"],
        signer=signer,
        heartbeat_interval=config["gossip"]["heartbeat_interval"],
        leader_ttl=config["gossip"]["leader_ttl"],
        on_alert=on_alert,
        on_leader_change=lambda leader_id: print(f"[DEBUG] Leader changed to: {leader_id}"),
    )
    gossip_node.start()

    alert_manager = AlertManager(
        node_id=node_id,
        gossip_node=gossip_node,
        send_to_coordinator=config["gossip"]["send_to_coordinator"],
    )
    detector = AlertDetector(keywords)

    def on_event(event_data):
        for alert in detector.detect(event_data):
            alert_manager.send_alert(alert["alert_type"], alert["details"], alert["severity"])

    monitor_directory(path, on_event, poll_interval)
