import pika
import json
import boto3
import time
import math

RABBITMQ_HOST = '44.202.82.165'
RABBITMQ_USER = 'user'
RABBITMQ_PASSWORD = 'password123'
QUEUE_NAME = 'insult_filter_queue'
LAMBDA_FUNCTION_NAME = 'InsultFilterLambda'

MAX_WORKERS = 10
MESSAGES_PER_WORKER = 100

lambda_client = boto3.client('lambda', region_name='us-east-1')

def get_queue_message_count(channel):
    try:
        queue = channel.queue_declare(queue=QUEUE_NAME, durable=True, passive=True)
        return queue.method.message_count
    except Exception as e:
        print(f"Error obteniendo conteo con pika: {e}")
        return 0

def invoke_lambda_batch(messages):
    records = [{"body": json.dumps({"text": msg})} for msg in messages]
    payload = json.dumps({"Records": records})
    lambda_client.invoke(
        FunctionName=LAMBDA_FUNCTION_NAME,
        InvocationType='Event',
        Payload=payload
    )
    print(f"[Lambda] Invocada con {len(messages)} mensajes.")

def get_messages(channel, n):
    collected = []
    delivery_tags = []
    for _ in range(n):
        method_frame, header_frame, body = channel.basic_get(queue=QUEUE_NAME, auto_ack=False)
        if not method_frame:
            break
        msg = body.decode() if isinstance(body, bytes) else body
        collected.append(msg)
        delivery_tags.append(method_frame.delivery_tag)
    return collected, delivery_tags
def stream_operator():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    print("[StreamOperator] Iniciando operador escalable...")

    while True:
        total_messages = get_queue_message_count(channel)
        if total_messages == 0:
            print("[Info] Cola vacía. Esperando...")
            time.sleep(5)
            continue

        num_workers = min(MAX_WORKERS, math.ceil(total_messages / MESSAGES_PER_WORKER))
        print(f"[Info] Mensajes en cola: {total_messages}")
        print(f"[Info] Se lanzará {num_workers} Lambdas")

        for _ in range(num_workers):
            batch_size = min(MESSAGES_PER_WORKER, total_messages)
            if batch_size == 0:
                break

            messages, tags = get_messages(channel, batch_size)
            if not messages:
                break

            invoke_lambda_batch(messages)

            for tag in tags:
                channel.basic_ack(delivery_tag=tag)

            total_messages -= batch_size

        time.sleep(5)

if __name__ == '__main__':
    stream_operator()