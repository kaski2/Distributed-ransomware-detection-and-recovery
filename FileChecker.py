from pathlib import Path
import sys
import time
import logging
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer
import os
from dotenv import load_dotenv
import configparser


# Config

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


load_dotenv()

path = os.getenv('MONITORED_DIR_PATH')

class MyEventHandler(FileSystemEventHandler):
    def on_any_event(self, event: FileSystemEvent) -> None:
        print(event)



if __name__ == "__main__":
    config = load_config()
    path = config.get("settings", "MONITORED_DIR_PATH")
    print(path)

    event_handler = MyEventHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    finally:
        observer.stop()
        observer.join()