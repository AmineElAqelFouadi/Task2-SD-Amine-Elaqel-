## Arxius `.py` utilitzats per exercici

### Exercise 1 – InsultFilter amb Lambda i RabbitMQ
- `InsultFilterLambda` (definit com a funció Lambda a AWS)
- `client_send_texts.py`: envia textos a la cua RabbitMQ.


### Exercise 2 – Escalat dinàmic amb control de càrrega
- `stream_operator.py`: calcula i llança el nombre de Lambdas segons la càrrega.
- `client_send_texts.py`: per generar càrrega de textos.
- `send_sqs.py`: utilitzat puntualment per provar la cua SQS i limitar manualment les Lambdas des del panell AWS (Simultaneidad).

### Exercise 3 – Lithops MapReduce sobre S3
- `lither.py`: aplica map per censurar insults i reduce per actualitzar el total acumulat.

### Exercise 4 – Operació batch amb concurrència limitada
- `exer4`: Processar diversos fitxers limitant el nombre de funcions amb `maxfunc`.
