# email_consumer.py

import pika
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

queue_name = 'emails_queue'
exchange_name = 'email_exchange'
routing_key = 'send_mail'

smtp_server = "smtp.gmail.com"
port = 587  # For starttls
from_account = "jojotherabbit2020@gmail.com" # new gmail account only for this purpose
from_user = "jojotherabbit2020"
from_password = "JojoRabbit2020"

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost', 5672, '/', pika.PlainCredentials("user", "password")))

# Create a new channel with the next available channel number or pass in a channel number to use
channel = connection.channel()

# Creates an exchange if it does not already exist, and if the exchange exists,
# verifies that it is of the correct and expected class.
channel.exchange_declare(exchange=exchange_name, exchange_type="direct")

# Creates a queue with the given name if not already exists.
# If the queue name is empty, the broker will create a unique queue name.
channel.queue_declare(queue=queue_name, durable=True)

# Binding exchange to send messages to the declared queue
channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=routing_key)

print('[*] Waiting for messages. To exit press CTRL+C')


def on_message_callback(ch, method, properties, body):
    print(f"{body.decode('UTF-8')} is received")

    try:
        mail = MIMEMultipart('alternative')
        mail["Subject"] = "Using RabbitMQ"
        mail["From"] = from_account
        mail["To"] = body.decode("UTF-8")
        content = MIMEText("Hi! I am Using RabbitMQ!")
        mail.attach(content)

        server = smtplib.SMTP(smtp_server, port)
        server.starttls()
        server.login(from_account, from_password)
        server.sendmail(from_account, body.decode("UTF-8"), mail.as_string())
        server.quit()
        print("[x] Email was sent successfully")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except ConnectionRefusedError:
        print('Failed to connect to the server. Bad connection settings?')
    except smtplib.SMTPServerDisconnected:
        print('Failed to connect to the server. Wrong user/password?')
    except smtplib.SMTPException as e:
        print('SMTP error occurred: ' + str(e))


channel.basic_consume(queue=queue_name, on_message_callback=on_message_callback)
print(f"[*] Waiting for data from {queue_name} To exit press CTRL+C")

try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()