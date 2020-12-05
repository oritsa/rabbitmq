# console_subscriber.py
import pika
import sys


class Subscriber:
    def __init__(self, queueName, bindingKey, config):
        self.queueName = queueName
        self.bindingKey = bindingKey
        self.config = config
        self.connection = self._create_connection()

    def __del__(self):
        self.connection.close()

    def _create_connection(self):
        parameters = pika.ConnectionParameters(self.config['host'], self.config['port'],
                                               '/', pika.PlainCredentials('user', 'password'))
        return pika.BlockingConnection(parameters)

    def on_message_callback(self, channel, method, properties, body):
        print(f"Received new message: {body.decode('UTF-8')}")
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def setup(self):
        channel = self.connection.channel()
        channel.exchange_declare(exchange=self.config['exchange'], exchange_type='topic')

        # This method creates or checks a queue
        channel.queue_declare(queue=self.queueName)

        # Binds the queue to the specified exchang
        channel.queue_bind(queue=self.queueName, exchange=self.config['exchange'], routing_key=self.bindingKey)
        channel.basic_consume(queue=self.queueName, on_message_callback=self.on_message_callback)
        print(f" [*] Waiting for data for {self.queueName}. To exit press CTRL+C")
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()


config = {'host': 'localhost', 'port': 5672, 'exchange': 'topic_exchange'}
subscriber = Subscriber("console_queue", "#", config)
subscriber.setup()
