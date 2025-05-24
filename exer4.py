import lithops
import boto3
from botocore.exceptions import ClientError

INSULTS = ["tonto", "bobo"]
BUCKET = 'insultos'

def batch_process(file_batch):
    s3 = boto3.client('s3')
    total_censored = 0

    for file_key in file_batch:
        try:
            # Descargar y procesar archivo
            obj = s3.get_object(Bucket=BUCKET, Key=file_key)
            text = obj['Body'].read().decode('utf-8')

            # Censurar y contar
            censored_words = []
            censored_count = 0
            for word in text.split():
                if word.lower() in INSULTS:
                    censored_words.append("CENSURADO")
                    censored_count += 1
                else:
                    censored_words.append(word)

            total_censored += censored_count

            # Subir archivo censurado (CORRECCIÓN AQUÍ)
            censored_text = ' '.join(censored_words)
            s3.put_object(
                Bucket=BUCKET,
                Key=f"censored_{file_key}",
                Body=censored_text.encode('utf-8')
            )  # <--- Paréntesis que faltaba

        except Exception as e:
            print(f"Error procesando {file_key}: {str(e)}")

    return total_censored
def batch_operation(function, maxfunc, bucket):
    s3 = boto3.client('s3')

    # Obtener lista de archivos
    response = s3.list_objects_v2(Bucket=bucket)
    all_files = [
        item['Key']
        for item in response.get('Contents', [])
        if item['Key'].endswith('.txt')
        and not item['Key'].startswith('censored_')
        and item['Key'] != 'total_censurados.txt'
    ]

    # Agrupar todos los archivos en un solo lote
    batches = [all_files]  # Un solo lote con todos los archivos

    # Configurar Lithops para 1 worker
    config = {
    'lithops': {
        'backend': 'aws_lambda',
        'storage': 'aws_s3'
    },
    'aws_lambda': {
        'workers': maxfunc,
        'execution_role': 'arn:aws:iam::256418587663:role/LabRole',
        'region': 'us-east-1'  # Añade la región aquí
    }
}
    fexec = lithops.FunctionExecutor(config=config)
    futures = fexec.map(function, batches)
    return sum(fexec.get_result(futures))

def main():
    MAX_CONCURRENT = 1  # Solo 1 ejecución Lambda

    # Ejecutar proceso
    total_censored = batch_operation(batch_process, MAX_CONCURRENT, BUCKET)

    # Actualizar total acumulado
    s3 = boto3.client('s3')
    try:
        total_obj = s3.get_object(Bucket=BUCKET, Key='total_censurados.txt')
        previous_total = int(total_obj['Body'].read().decode('utf-8'))
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            previous_total = 0

    new_total = previous_total + total_censored
    s3.put_object(
        Bucket=BUCKET,
        Key='total_censurados.txt',
        Body=str(new_total).encode('utf-8')
    )

    print(f"Archivos procesados: {len(all_files)}")
    print(f"Total censurado en esta ejecución: {total_censored}")
    print(f"Total acumulado: {new_total}")

if __name__ == "__main__":
    main()