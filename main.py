import os

import aws_client
import producer
import threading
from pathlib import Path
import sys
import configparser


CONFIG_FILE = Path("settings.ini")

REQUIRED_SETTINGS = {
    "settings": ["MONITORED_DIR_PATH"],
    "agent": ["node_id"],
    "security": ["shared_secret"],
    "gossip": ["listen_host", "listen_port", "peers", "heartbeat_interval", "leader_ttl", "send_to_coordinator"],
    "detection": ["ransom_note_keywords", "rate_threshold", "rate_window_seconds", "extension_change_alert"]
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
        print(f"[CONFIG] Duplicate section in configuration file: {e}")
        sys.exit(1)
    except configparser.ParsingError as e:
        print(f"[CONFIG] Parsing error in settings.ini: {e}")
        sys.exit(1)
    except configparser.DuplicateOptionError as e:
        print(f"[CONFIG] Duplicate option in settings.ini: under section: {e.section} option: {e.option}")
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
        print("[CONFIG] ERROR MISSING MANDATORY SETTINGS IN settings.ini:")
        for item in missing:
            print(f" {item}")
        print("fill these missing values in settings.ini")
        sys.exit(1)
    return config

if __name__ == "__main__":
    config = load_config()
    path = config.get("settings", "MONITORED_DIR_PATH")
    poll_interval = config["settings"]["snapshot_interval_seconds"]
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    print(f"Connecting to Kafka at: {kafka_servers}")
    print(f"Path: {path}")
    t1 = threading.Thread(target=producer.main, args=(path, poll_interval, kafka_servers, config))
    t2 = threading.Thread(target=aws_client.main, args=(path, poll_interval))
    t1.start()
    t2.start()
    t1.join()
    t2.join()