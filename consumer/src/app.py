from flask import Flask
from confluent_kafka import Consumer
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER")

if __name__ == '__main__':

    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'mygroup',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe(['mytopic'])

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        print('Received message: {}'.format(msg.value().decode('utf-8')))

        consumer.close()

    app.run(port=5000)
