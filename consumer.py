from kafka import KafkaConsumer

topic = "file-monitoring"
group_id = "file-monitoring-leader"

consumer = KafkaConsumer(
    topic,
    bootstrap_servers="localhost:9092",
    group_id=group_id,
    auto_offset_reset="latest",
    enable_auto_commit=True,
)

for message in consumer:
    print(f"Received: {message.value.decode('utf-8')}")
