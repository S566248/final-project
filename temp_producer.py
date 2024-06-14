import pika
import json
from faker import Faker
import time

fake = Faker()

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='temperature_exchange', exchange_type='direct')

def publish_temperature():
    while True:
        data = {
            'timestamp': fake.date_time().isoformat(),
            'sensor_id': fake.uuid4(),
            'temperature': fake.random_int(min=-10, max=110)
        }
        message = json.dumps(data)
        channel.basic_publish(exchange='temperature_exchange',
                              routing_key='temperature',
                              body=message)
        print(f"Sent: {message}")
        time.sleep(30)

if __name__ == "__main__":
    try:
        publish_temperature()
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        connection.close()