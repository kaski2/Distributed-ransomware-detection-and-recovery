"""Kafka producer for file system monitoring and security alert dispatching."""

import binascii
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer

from alerting import AlertDetector, AlertManager, AlertSigner, GossipNode


TOPIC = 'security-alerts'


def parse_gossip_peers(peers_value):
    """Parse a comma-separated 'host:port' string or list into a list of (host, port) tuples."""
    if isinstance(peers_value, (list, tuple)):
        parsed_peers = []
        for peer in peers_value:
            if isinstance(peer, tuple) and len(peer) == 2:
                parsed_peers.append((peer[0], int(peer[1])))
        return parsed_peers

    parsed_peers = []
    for raw_peer in str(peers_value).split(","):
        peer = raw_peer.strip()
        if not peer:
            continue
        if ":" not in peer:
            raise ValueError(f"Invalid peer format: '{peer}'. Expected 'host:port'.")
        host, port_text = peer.rsplit(":", 1)
        parsed_peers.append((host.strip(), int(port_text.strip())))

    return parsed_peers


def parse_bool(value):
    """Return True if value is a truthy string (1, true, yes, on)."""
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def send_alert(event_producer, alert_data):
    """Send an alert dict to Kafka, logging success or failure."""
    try:
        future = event_producer.send(TOPIC, value=alert_data)
        future.get(timeout=10)
        print(f"Alert sent: {alert_data['alert_type']} - {alert_data['details'].get('file_path', '')}")
    except Exception as e:
        print(f"[ALERT] Failed to send alert: {e}")


def read_file_contents(file_path, max_size_bytes=1024*1024):
    """Read file as text or hex-encoded binary. Returns an error string on failure."""
    try:
        stat = os.stat(file_path)
        if stat.st_size > max_size_bytes:
            return f"File too large: {stat.st_size} bytes, max: {max_size_bytes} bytes>"

        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                return f.read()
        except OSError:
            with open(file_path, 'rb') as f:
                binary_data = f.read()
            return f"Binary file (hex): {binascii.hexlify(binary_data[:100]).decode('utf-8')}..."
    except OSError as e:
        return f"Error reading file: {e}"


def monitor_directory(path, on_event, poll_interval=1):
    """Poll a directory for file changes and call on_event for each create, modify, or delete."""
    if not os.path.exists(path):
        print(f"[DIR] Monitored path does not exist: {path}")
        sys.exit(1)

    # Track file state: path -> (mtime, size)
    file_state = {}

    # Initial scan to populate state without sending events
    for root, _, files in os.walk(path):
        for filename in files:
            file_path = os.path.join(root, filename)
            try:
                stat = os.stat(file_path)
                file_state[file_path] = (stat.st_mtime, stat.st_size)
            except OSError:
                pass

    print(f"Initial scan complete. Tracking {len(file_state)} files.")

    while True:
        time.sleep(poll_interval)
        try:
            current_files = set()

            for root, _, files in os.walk(path):
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

        except OSError as e:
            print(f"[DIR] Error during directory scan: {e}")


def _build_gossip_node(config, node_id, signer, on_alert):
    """Construct and return a GossipNode from config."""

    listen_host = config["gossip"]["listen_host"]
    listen_port = int(config["gossip"]["listen_port"])
    peers = parse_gossip_peers(config["gossip"]["peers"])
    send_to_coordinator = parse_bool(config["gossip"]["send_to_coordinator"])
    heartbeat_interval = float(config["gossip"]["heartbeat_interval"])
    leader_ttl = float(config["gossip"]["leader_ttl"])

    gossip_node = GossipNode(
        node_id=node_id,
        listen_host=listen_host,
        listen_port=listen_port,
        peers=peers,
        signer=signer,
        heartbeat_interval=heartbeat_interval,
        leader_ttl=leader_ttl,
        on_alert=on_alert,
        on_leader_change=lambda leader_id: print(f"Leader changed to: {leader_id}"),
    )
    return gossip_node, send_to_coordinator


def main(path, kafka_servers, config):
    """Start Kafka producer, gossip node, and directory monitor."""
    node_id = config['agent']["node_id"]
    keywords = config['detection']["ransom_note_keywords"].split(",")

    try:
        event_producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            retries=5,
            retry_backoff_ms=1000,
            value_serializer=lambda value: json.dumps(value).encode('utf-8'),
        )
        print("KafkaProducer created successfully")
    except Exception as e:
        print(f"[KAFKA] Failed to create KafkaProducer: {e}")
        sys.exit(1)

    signer = AlertSigner(config["security"]["shared_secret"])

    def on_alert(alert, source):
        payload = dict(alert)
        payload["ingestion_source"] = source
        send_alert(event_producer, payload)

    gossip_node, send_to_coordinator = _build_gossip_node(config, node_id, signer, on_alert)
    gossip_node.start()

    alert_manager = AlertManager(
        node_id=node_id,
        gossip_node=gossip_node,
        send_to_coordinator=send_to_coordinator,
    )
    detector = AlertDetector(keywords)

    def on_event(event_data):
        for alert in detector.detect(event_data):
            alert_manager.send_alert(alert["alert_type"], alert["details"], alert["severity"])

    monitor_directory(path, on_event)
