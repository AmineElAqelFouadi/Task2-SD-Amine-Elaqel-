import pika
import json
import boto3
import time  # para delay

RABBITMQ_HOST = '54.164.154.111'
RABBITMQ_USER = 'user'
RABBITMQ_PASSWORD = 'password123'
QUEUE_NAME = 'insult_filter_queue'
LAMBDA_FUNCTION_NAME = 'InsultFilterLambda'  # Cambia por el nombre real de tu Lambda

lambda_client = boto3.client('lambda', region_name='us-east-1')

def invoke_lambda(message):
    payload = json.dumps({
        "Records": [
            {"body": json.dumps({"text": message})}
        ]
    })
    lambda_client.invoke(
        FunctionName=LAMBDA_FUNCTION_NAME,
        InvocationType='Event',  # Invocación asíncrona para no esperar respuesta
        Payload=payload
    )
    print(f"Lambda invoked with message: {message}")

def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        text = data.get('text', '').strip()
        if not text:
            print("Mensaje vacío recibido, descartando.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        print(f"Received message: {text}")
        invoke_lambda(text)
        ch.basic_ack(delivery_tag=method.delivery_tag)  # Confirmar que el mensaje fue procesado
        time.sleep(0)  # Pausa de 2 segundos entre mensajes
    except Exception as e:
        print(f"Error procesando mensaje: {e}")
        # Para evitar bloqueo, reconoce el mensaje y descártalo o reintenta con requeue=True
        ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
    print("Waiting for messages...")
    channel.start_consuming()

if __name__ == '__main__':
    main()