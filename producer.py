from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

producer.send('testTopic', b'kafka test')
producer.flush()
print("Message sent to 'testTopic'")