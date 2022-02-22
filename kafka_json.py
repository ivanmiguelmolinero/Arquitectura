from concurrent.futures import thread
from ensurepip import bootstrap
from enum import auto
import imp
import threading
import logging
import time
import json

from kafka import KafkaConsumer, KafkaProducer

# Create Producer Class
class Producer(threading.Thread):
    daemon = True
    
    def run(self):
        producer = KafkaProducer(bootstrap_servers='kafka:9092',
                                value_serializer=lambda v:
                                json.dumps(v).encode('utf-8'))

        while True:
            producer.send('topic_prueba', {"Nombre": "Iván"})
            producer.send('topic_prueba', {"Nombre": "Pedro"})
            time.sleep(1)

#Create consumer class
class Consumer(threading.Thread):
    daemon = True

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='kafka:9092',
                                auto_offset_reset='earliest',
                                value_serializer=lambda m:
                                json.loads(m.encode('utf-8')))
        consumer.subscribe(['topic_prueba'])

        for message in consumer:
            print(message)

# Main thread
def main():
    threads = [
        Producer(),
        Consumer()
    ]

    for t in threads:
        t.start()
    
    time.sleep(10)


# Start
if __name__ = '__main__':
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:' +
                '%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    main()