from kafka import KafkaProducer
from pathlib import Path
import sys
import time
import logging
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer
import os
import configparser
from datetime import datetime, timedelta


topic = 'file-monitoring'
   
 
class MyEventHandler(FileSystemEventHandler):
    
    def __init__(self, event_producer):
        self.event_producer = event_producer
        super().__init__()
    

    def send_event(self, event_data):
        future = self.event_producer.send(topic, value=str(event_data).encode('utf-8'))
        record_metadata = future.get(timeout=10)
            
        print(f"Event sent: {event_data['event_type']} - {event_data['file_path']}")
        print(f"  Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
    
    # Create standardized event data structure.
    def _create_event(self, event):
        return {
            'timestamp': datetime.utcnow().isoformat(),
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
            self.event_producer.send_event(event_data)
    
    # Detects creation of files.
    def on_created(self, event):
        if not event.is_directory:
            event_data = self._create_event(event)
            self.event_producer.send_event(event_data)
    
    # Detects deletion of files.
    def on_deleted(self, event):
        if not event.is_directory:
            event_data = self._create_event(event)
            self.event_producer.send_event(event_data)
    
    # Detects file move and rename events.
    def on_moved(self, event):
        if not event.is_directory:
            event_data = self._create_event(event)
            event_data['dest_path'] = event.dest_path
            self.event_producer.send_event(event_data)

    
    
    
def monitor_directory(path, kafka_servers='localhost:9092', topic='file-events'):    
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
    


if __name__ == "__main__":
    event_producer = KafkaProducer(bootstrap_servers='localhost:9092')
    
    
    
        