import time

import pika
from pika.adapters.blocking_connection import BlockingChannel


def callback(ch: BlockingChannel, method, properties, body: str):
    print(f"[x] Received {body}")
    time.sleep(str(body).count("."))
    print("[x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)


connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()

channel.queue_declare(queue="task_queue", durable=True)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(
    queue="hello",
    on_message_callback=callback
)

channel.start_consuming()
