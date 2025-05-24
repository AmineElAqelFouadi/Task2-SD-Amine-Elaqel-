import lithops
import boto3
from botocore.exceptions import ClientError

INSULTS = ["tonto", "bobo"]
BUCKET = 'insultos'

def mapper(file_key):
    s3 = boto3.client('s3')

    # Descargar y procesar archivo
    obj = s3.get_object(Bucket=BUCKET, Key=file_key)
    text = obj['Body'].read().decode('utf-8')

    # Censurar y contar
    censored_count = 0
    words = []
    for word in text.split():
        if word.lower() in INSULTS:
            words.append("CENSURADO")
            censored_count += 1
        else:
            words.append(word)

    # Subir resultado censurado
    censored_text = ' '.join(words)
    s3.put_object(
        Bucket=BUCKET,
        Key='insultos_censurados.txt',
        Body=censored_text.encode('utf-8')
    )

    return censored_count

def reducer(results):
    s3 = boto3.client('s3')
    total = sum(results)

    # Obtener total anterior
    try:
        response = s3.get_object(Bucket=BUCKET, Key='total_censurados.txt')
        previous_total = int(response['Body'].read().decode('utf-8'))
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            previous_total = 0
        else:
                        raise

    # Calcular y guardar nuevo total
    new_total = previous_total + total
    s3.put_object(
        Bucket=BUCKET,
        Key='total_censurados.txt',
        Body=str(new_total).encode('utf-8')
    )

    return new_total

def main():
    # Verificar que existe el archivo fuente
    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket=BUCKET, Key='insultos.txt')
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            raise Exception("Archivo insultos.txt no encontrado en el bucket")

    # Configurar Lithops
    fexec = lithops.FunctionExecutor()

    # Ejecutar Map y Reduce por separado
    map_futures = fexec.map(mapper, ['insultos.txt'])
    results = fexec.get_result(map_futures)
    total = reducer(results)

    print(f"Censura completada. Total acumulado: {total}")

if __name__ == "__main__":
    main()