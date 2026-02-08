from kafka import KafkaProducer
from pathlib import Path
import sys
import time
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
import os
import configparser
from datetime import datetime, timedelta, timezone
import socket
import json

from alerting import AlertManager, AlertSigner, GossipNode


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


def get_bool_setting(config, section, option, fallback):
    value = config.get(section, option, fallback=str(fallback)).strip()
    return value.lower() in ("1", "true", "yes", "on")


def parse_peers(peers_value):
    peers = []
    if not peers_value:
        return peers
    for item in peers_value.split(","):
        item = item.strip()
        if not item:
            continue
        if ":" not in item:
            continue
        host, port_str = item.rsplit(":", 1)
        try:
            port = int(port_str)
        except ValueError:
            continue
        peers.append((host.strip(), port))
    return peers
   
 
class MyEventHandler(FileSystemEventHandler):
    def __init__(
        self,
        event_producer,
        alert_manager: AlertManager,
        rate_threshold: int,
        rate_window_seconds: int,
        cooldown_seconds: int,
        ransom_note_keywords,
        extension_change_alert: bool,
    ):
        self.event_producer = event_producer
        self.alert_manager = alert_manager
        self.rate_threshold = rate_threshold
        self.rate_window = timedelta(seconds=rate_window_seconds)
        self.cooldown_seconds = cooldown_seconds
        self.ransom_note_keywords = ransom_note_keywords
        self.extension_change_alert = extension_change_alert
        self.events = []
        self.last_rate_alert = None
        super().__init__()
    

    def send_event(self, event_data):
        future = self.event_producer.send(topic, value=str(event_data).encode('utf-8'))
        record_metadata = future.get(timeout=10)
            
        print(f"Event sent: {event_data['event_type']} - {event_data['file_path']}")
        print(f"  Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")

    def _check_rate_alert(self, now):
        cutoff = now - self.rate_window
        self.events = [t for t in self.events if t > cutoff]
        if len(self.events) < self.rate_threshold:
            return
        if self.last_rate_alert and (now - self.last_rate_alert).total_seconds() < self.cooldown_seconds:
            return

        self.last_rate_alert = now
        details = {
            "event_count": len(self.events),
            "window_seconds": int(self.rate_window.total_seconds()),
        }
        self.alert_manager.send_alert("rate_spike", details, "high")
    
    # Create standardized event data structure.
    def _create_event(self, event):
        return {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'event_type': event.event_type,
            'file_path': event.src_path,
            'is_directory': event.is_directory,
            'file_extension': Path(event.src_path).suffix if not event.is_directory else None,
            'file_name': Path(event.src_path).name
        }
    
    # Detects file modifications. 
    def on_modified(self, event):
        if not event.is_directory:
            event_data = self._create_event(event)
            self.send_event(event_data)
            now = datetime.now(timezone.utc)
            self.events.append(now)
            self._check_rate_alert(now)
    
    # Detects creation of files.
    def on_created(self, event):
        if not event.is_directory:
            event_data = self._create_event(event)
            self.send_event(event_data)
            filename = Path(event.src_path).name.lower()
            for keyword in self.ransom_note_keywords:
                if keyword in filename:
                    details = {"file_path": event.src_path, "keyword": keyword}
                    self.alert_manager.send_alert("ransom_note", details, "high")
                    break
    
    # Detects deletion of files.
    def on_deleted(self, event):
        if not event.is_directory:
            event_data = self._create_event(event)
            self.send_event(event_data)
    
    # Detects file move and rename events.
    def on_moved(self, event):
        if not event.is_directory:
            event_data = self._create_event(event)
            event_data['dest_path'] = event.dest_path
            self.send_event(event_data)
            if self.extension_change_alert:
                old_ext = Path(event.src_path).suffix
                new_ext = Path(event.dest_path).suffix
                if old_ext != new_ext:
                    details = {
                        "src_path": event.src_path,
                        "dest_path": event.dest_path,
                        "old_ext": old_ext,
                        "new_ext": new_ext,
                    }
                    self.alert_manager.send_alert("extension_change", details, "medium")

    
    
    
def monitor_directory(event_producer, path, alert_manager, config):
    rate_threshold = int(config.get("detection", "rate_threshold", fallback="10"))
    rate_window_seconds = int(config.get("detection", "rate_window_seconds", fallback="60"))
    cooldown_seconds = int(config.get("detection", "cooldown_seconds", fallback="30"))
    ransom_keywords = config.get(
        "detection",
        "ransom_note_keywords",
        fallback="readme,decrypt,recover,restore",
    )
    ransom_note_keywords = [k.strip().lower() for k in ransom_keywords.split(",") if k.strip()]
    extension_change_alert = get_bool_setting(config, "detection", "extension_change_alert", True)

    event_handler = MyEventHandler(
        event_producer,
        alert_manager,
        rate_threshold,
        rate_window_seconds,
        cooldown_seconds,
        ransom_note_keywords,
        extension_change_alert,
    )
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    finally:
        observer.stop()
        observer.join()
    


if __name__ == "__main__":
    config = load_config()
    path = config.get("settings", "MONITORED_DIR_PATH")
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    event_producer = KafkaProducer(bootstrap_servers=kafka_servers)
    alert_topic = os.getenv("ALERT_TOPIC", "security-alerts")
    node_id = config.get("agent", "node_id", fallback="").strip()
    if not node_id:
        node_id = socket.gethostname()

    shared_secret = config.get("security", "shared_secret", fallback="change-me")
    signer = AlertSigner(shared_secret)

    listen_host = config.get("gossip", "listen_host", fallback="0.0.0.0")
    listen_port = int(config.get("gossip", "listen_port", fallback="9101"))
    peers = parse_peers(config.get("gossip", "peers", fallback=""))
    send_to_coordinator = get_bool_setting(config, "gossip", "send_to_coordinator", True)
    heartbeat_interval = float(config.get("gossip", "heartbeat_interval", fallback="2.0"))
    leader_ttl = float(config.get("gossip", "leader_ttl", fallback="6.0"))

    def publish_alert(alert):
        payload = json.dumps(alert, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        event_producer.send(alert_topic, value=payload)

    def on_alert(alert, source):
        is_leader = gossip_node.is_leader() if gossip_node else False
        if is_leader:
            publish_alert(alert)
        print(f"ALERT via {source}: {alert['alert_type']} - {alert['details']}")

    def on_leader_change(leader_id):
        print(f"Coordinator updated: {leader_id}")

    gossip_node = GossipNode(
        node_id=node_id,
        listen_host=listen_host,
        listen_port=listen_port,
        peers=peers,
        signer=signer,
        heartbeat_interval=heartbeat_interval,
        leader_ttl=leader_ttl,
        on_alert=on_alert,
        on_leader_change=on_leader_change,
    )
    gossip_node.start()

    alert_manager = AlertManager(
        node_id=node_id,
        gossip_node=gossip_node,
        send_to_coordinator=send_to_coordinator,
    )

    monitor_directory(event_producer, path, alert_manager, config)
    
    
    
        