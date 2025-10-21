# Este código se ejecuta después de la aplicación spark_streaming_consumer2.py
Comando para ejecutar en Putty: python3 kafka_producer1.py
import json
import time
import random
from kafka import KafkaProducer
import pandas as pd

# dataset de fuente original
df = pd.read_excel("Online Retail.xlsx")

# Pasar columnas de fecha a texto
for col in df.columns:
    if 'date' in col.lower():
        df[col] = df[col].astype(str)

# Archivo CSV
df.to_csv("onlineretail.csv", index=False)

# Definir producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Envío de datos
for i in range(10):
    record = df.sample(1).to_dict(orient="records")[0]
    producer.send("online_retail_data", record)
    print(f" Enviado: {record}")
    time.sleep(1)

producer.flush()
producer.close()
print(" Ejecución completada.")
