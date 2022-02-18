from kafka import KafkaProducer

# Create producer
producer = KafkaProducer(bootstrap_servers='kafka:9092')
for _ in range(100):
    producer.send('foobar', b'some_message_bytes')