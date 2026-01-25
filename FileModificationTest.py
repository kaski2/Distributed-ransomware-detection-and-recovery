import os
from pathlib import Path
import configparser

CONFIG_FILE = Path("settings.ini")

REQUIRED_SETTINGS = {
    "settings": ["MONITORED_DIR_PATH"]
    }

def get_path():
    config = configparser.ConfigParser()
    if not CONFIG_FILE.exists():
        print("Configuration file not found. Creating one with default values.")
        config['settings'] = {'MONITORED_DIR_PATH': 'C:/default/path'}  # Set a default path
        with open(CONFIG_FILE, 'w') as configfile:
            config.write(configfile)
        return config['settings']['MONITORED_DIR_PATH']  # Return the default path
    else:
        config.read(CONFIG_FILE)
        return config.get("settings", "MONITORED_DIR_PATH")
        


def write_test_files(path):
    for i in range(1, 11):
        filename = f"test{i}.txt"
        filepath = os.path.join(path, filename)

        with open(filepath, "a") as f:
            f.write("hello world!\n")

if __name__ == "__main__":
    path = get_path()
    write_test_files(path)