import boto3
import json
import time

QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/256418587663/InsultFilterQueue'  # Cambia por tu URL real

INSULTS = [
    "Eres un tonto", "Eres un bobo", "Tonto el que lo lea"
]

sqs = boto3.client('sqs', region_name='us-east-1')

def send_messages():
    count = 0
    while count < 2000:
        for insult in INSULTS:
            if count >= 2000:
                break
            mensaje = json.dumps({'text': insult})
            response = sqs.send_message(
                QueueUrl=QUEUE_URL,
                MessageBody=mensaje
            )
            print(f"Mensaje enviado: {insult} | MessageId: {response['MessageId']}")
            count += 1
            time.sleep(0)

if __name__ == "__main__":
    send_messages()