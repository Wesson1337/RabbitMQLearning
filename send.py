import pika
import sys

exchange_name = "logs"

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange=exchange_name, exchange_type='fanout')

message = ' '.join(sys.argv[1:]) or "info: Hello World!"
channel.basic_publish(exchange=exchange_name, routing_key='', body=message)
print(" [x] Sent %r" % message)

connection.close()
