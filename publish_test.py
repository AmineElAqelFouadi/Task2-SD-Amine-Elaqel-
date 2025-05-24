import pika
import json
import time

RABBITMQ_HOST = '44.202.82.165'
RABBITMQ_USER = 'user'
RABBITMQ_PASSWORD = 'password123'
QUEUE_NAME = 'insult_filter_queue'

INSULTS = [
    "Eres un tonto", "Eres un bobo", "Tonto el que lo lea"
]

def send_insult_messages():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    count = 0
    # Repetir la lista hasta enviar al menos 20 mensajes
    while count < 3000:
        for insult in INSULTS:
            if count >= 3000:
                break
            message = json.dumps({'text': insult})
            channel.basic_publish(
                exchange='',
                routing_key=QUEUE_NAME,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2
                )
            )
            print(f"Sent: {insult}")
            time.sleep(0)
            count += 1

    connection.close()

if __name__ == "__main__":
    send_insult_messages()