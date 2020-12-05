# publisher.py
import time

import pika
from datetime import datetime


class Publisher:
    def __init__(self, config):
        self.config = config

    def publish(self, routing_key, message):
        connection = self.create_connection()
        # Create a new channel with the next available channel number or pass in a channel number to use
        channel = connection.channel()

        # Creates an exchange if it does not already exist, and if the exchange exists,
        # verifies that it is of the correct and expected class.
        channel.exchange_declare(exchange=self.config['exchange'], exchange_type='topic')

        # Publishes message to the exchange with the given routing key
        channel.basic_publish(exchange=self.config['exchange'], routing_key=routing_key, body=message)
        print(" [x] Sent message %r for %r" % (message, routing_key))

    # Create new connection
    def create_connection(self):
        param = pika.ConnectionParameters(self.config['host'], self.config['port'],
                                        '/', pika.PlainCredentials('user', 'password'))
        return pika.BlockingConnection(param)


config = {'host': 'localhost', 'port': 5672, 'exchange': 'topic_exchange'}
publisher = Publisher(config)
publisher.publish('timestamp', datetime.now().strftime("%Y-%d-%m %H:%M:%S"))
publisher.publish('message', "This is a test message")
time.sleep(3)
publisher.publish('timestamp.test', datetime.now().strftime("%Y-%d-%m %H:%M:%S"))
