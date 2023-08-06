from flask import Flask
from confluent_kafka import Consumer
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER")

consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'mygroup',
        'auto.offset.reset': 'earliest'
    })

def consume_message(topic):
    '''subscribes to a topic and consume messages'''
    
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            print('Received message: {}'.format(msg.value().decode('utf-8')))

    except Exception as error:
        print(error)
    finally:
        consumer.close()

if __name__ == '__main__':
    consume_message('tutorial-topic')

    app.run(port=5000)
