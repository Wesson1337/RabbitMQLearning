import pika
import sys

exchange_name = "direct_logs"

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)
channel = connection.channel()

channel.exchange_declare(exchange=exchange_name, exchange_type="direct")

result = channel.queue_declare(queue="", exclusive=True)
queue_name = result.method.queue

severities = sys.argv[1:]
if not severities:
    sys.stderr.write(f"Usage: {sys.argv[0]} [info] [warning] [error]")
    sys.exit(1)

for severity in severities:
    channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=severity)

print(" [*] Waiting for logs. To exit press CTRL+C")


def callback(ch, method, properties, body):
    print(f" [x] {method.routing_key}:{body}")


channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()
