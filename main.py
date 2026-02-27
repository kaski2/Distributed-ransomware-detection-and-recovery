import os

import aws_client
import producer
import threading
import sys
import configparser

REQUIRED_SETTINGS = {
    "settings": ["MONITORED_DIR_PATH"]
}

def load_config():
    config = configparser.ConfigParser()
    config_candidates = [
        "settings.ini",
        "/app/settings.ini",
        "settings-node1.ini",
        "settings-node2.ini",
        "settings-node3.ini",
    ]

    try:
        loaded_files = config.read(config_candidates)
        if not loaded_files:
            print("[Error] No settings file found. Expected one of: settings.ini, /app/settings.ini, settings-node1.ini, settings-node2.ini, settings-node3.ini")
            sys.exit(1)
    except configparser.DuplicateSectionError as e:
        print(f"[Error] duplicate section in configuration file: {e}")
        sys.exit(1)
    except configparser.ParsingError as e:
        print(f"[Error] parsing error in settings.ini: {e}")
        sys.exit(1)
    except configparser.DuplicateOptionError as e:
        print(f"[Error] duplicate option in settings.ini: under section: {e.section} option: {e.option}")
        sys.exit(1)

    missing = []

    for section, options in REQUIRED_SETTINGS.items():
        if not config.has_section(section):
            missing.append(f"{section}.*")
            continue

        for option in options:
            if not config.has_option(section, option) or not config[section][option].strip():
                missing.append(f"{section}.{option}")

    if missing:
        print("ERROR MISSING MANDATORY SETTINGS:")
        for item in missing:
            print(f" {item}")
        print("fill these missing values in your settings file")
        sys.exit(1)
    return config

if __name__ == "__main__":
    config = load_config()
    path = config.get("settings", "MONITORED_DIR_PATH")
    poll_interval = int(config.get("settings", "POLL_INTERVAL_SECONDS", fallback="2"))
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    print(f"[DEBUG] Connecting to Kafka at: {kafka_servers}")
    print(f"[DEBUG] Path: {path}")
    t1 = threading.Thread(target=producer.main, args=(path, poll_interval, kafka_servers))
    t2 = threading.Thread(target=aws_client.main, args=(path, poll_interval))
    t1.start()
    t2.start()
    t1.join()
    t2.join()