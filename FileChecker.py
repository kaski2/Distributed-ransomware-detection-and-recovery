from pathlib import Path
import sys
import time
import logging
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer
import os
import configparser
from datetime import datetime, timedelta


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


class MyEventHandler(FileSystemEventHandler):
    def __init__(self):
        super().__init__()
        self.events = []  # Store (timestamp, filepath) tuples
        self.alert_threshold = 10  # Alert if 10+ files modified in 60 seconds
        self.time_window = 60  # seconds
    
    
    # Check if too many files are being modified rapidly. Ransomware often modifies many files in a short period.
    def _check_suspicious_activity(self):
        """Check if too many files are being modified rapidly"""
        now = datetime.now()
        cutoff = now - timedelta(seconds=self.time_window)
        
        # Remove old events
        self.events = [(t, p) for t, p in self.events if t > cutoff]
        
        # Alert if threshold exceeded
        if len(self.events) >= self.alert_threshold:
            print(f"\nSUSPICIOUS ACTIVITY DETECTED!")
            print(f"   {len(self.events)} files modified in {self.time_window} seconds")
            print(f"   This could be ransomware!\n")
    
    
    # Monitor file modifications. 
    def on_modified(self, event):
        if not event.is_directory:
            self.events.append((datetime.now(), event.src_path))
            self._check_suspicious_activity()
    
    
    # Monitor creation of ransom note files. Ransomware often creates files like "README.txt" or "DECRYPT_INSTRUCTIONS.txt" that contain ransom demands.
    def on_created(self, event):
        if not event.is_directory:
            filename = Path(event.src_path).name.lower()
            # Check for ransom notes
            if "readme" in filename or "decrypt" in filename or "recover" in filename:
                print(f"\nALERT: Possible ransom note created: {event.src_path}\n")
    
    
    # Monitor file extension changes. Ransomware often changes file extensions when it encrypts files.
    def on_moved(self, event):
        if not event.is_directory:
            old_ext = Path(event.src_path).suffix
            new_ext = Path(event.dest_path).suffix
            # Alert if file extension changed
            if old_ext != new_ext:
                print(f"File extension changed: {event.src_path} -> {event.dest_path}")



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