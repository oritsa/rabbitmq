# email_producer.py

import pika

queue_name = 'emails_queue'
exchange_name = 'email_exchange'
routing_key = 'send_mail'

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672, '/', pika.PlainCredentials('user', 'password')))

# Create a new channel with the next available channel number or pass in a channel number to use
channel = connection.channel()

# Creates an exchange if it does not already exist, and if the exchange exists,
# verifies that it is of the correct and expected class.
channel.exchange_declare(exchange=exchange_name, exchange_type="direct")

# Creates a queue with the given name if not already exists.
# If the queue name is empty, the broker will create a unique queue name.
channel.queue_declare(queue=queue_name, durable=True)

# binding exchange to send messages to the declared queue
channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=routing_key)

# Publishes message to the exchange with the given routing key
channel.basic_publish(exchange=exchange_name, routing_key=routing_key, body="orit.sanandaji@modelity.com")

#channel.basic_publish(exchange=exchange_name, routing_key=routing_key, body="Hagai.Hen@modelity.com")

connection.close()