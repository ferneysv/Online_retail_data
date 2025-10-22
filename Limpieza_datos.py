# En este código se refleja el proceso de limpieza, análisis y transformación de datos
# Comando para crear el topic (online_retail_data) 
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic online_retail_data
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["SPARK_HOME"] = "/opt/spark"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, avg, sum as _sum, desc

# Nueva sesión Spark
spark = SparkSession.builder.appName("LimpiezaYEDA_Retail").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#  Cargar o leer datos
df = spark.read.csv("onlineretail.csv", header=True, inferSchema=True)

print(" Datos Reales: ")
df.show(5)

print("Filas y columnas: ")
print((df.count(), len(df.columns)))

#  Limpiar Datos
print(" Contar los valores nulos: ")
null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
null_counts.show()

# Eliminar valores duplicados
df = df.dropna().dropDuplicates()

# Filtro de los precios positivos y cantidades válidas
df = df.filter((df["UnitPrice"] > 0) & (df["Quantity"] > 0))

print(" Datos limpios: ")
df.show(5)

# Transformacion de valor total
df = df.withColumn("TotalValue", col("Quantity") * col("UnitPrice"))

# (EDA)

print(" Ventas por país: ")
ventas_pais = df.groupBy("Country").agg(
    count("InvoiceNo").alias("TotalTransacciones"),
    _sum("TotalValue").alias("ValorTotalVentas"),
    avg("UnitPrice").alias("PrecioPromedio")
).orderBy(desc("ValorTotalVentas"))

ventas_pais.show(10)

print(" Listado de productos más vendidos: ")
productos_top = df.groupBy("Description").agg(
 _sum("Quantity").alias("TotalCantidadVendida")
).orderBy(desc("TotalCantidadVendida"))

productos_top.show(10, truncate=False)


#  RDD en operaciones bajas
rdd = df.rdd

#  cantidad total y promedio de precios con el comando RDD
total_registros = rdd.count()
precio_promedio_rdd = rdd.map(lambda x: x.UnitPrice).mean()

print(f" RDD para Análisis exploratorio de datos")
print(f"Total de registros: {total_registros}")
print(f"Precio promedio: {precio_promedio_rdd:.2f}")

#  Guardar desarrollo de datos limpios y transformados
df.write.csv("onlineretail_clean_eda.csv", header=True, mode="overwrite")

print(" Limpieza, y análisis exploratorio de datos")

spark.stop()


