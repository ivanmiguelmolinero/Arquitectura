from kafka import KafkaConsumer

# Create consumer.
consumer = KafkaConsumer('prueba_topic')
for msg in consumer:
    print(msg.value)