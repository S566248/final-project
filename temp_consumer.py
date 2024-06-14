import pika
import json
import time
from collections import deque

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='temperature_queue', durable=True)

def callback(ch, method, properties, body):
    message = json.loads(body)
    print(f"Received: {message}")

    # Example processing logic
    timestamp = message['timestamp']
    sensor_id = message['sensor_id']
    temperature = message['temperature']

    # Simulated analysis and alerting logic here...

    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue='temperature_queue', on_message_callback=callback)

print('Waiting for messages. To exit press CTRL+C')
try:
    channel.start_consuming()
except KeyboardInterrupt:
    print("Consumer stopped.")
finally:
    connection.close()