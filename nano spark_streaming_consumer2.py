# Este código se ejecuta antes del nano kafka_producer1.py
# comando para ejecutar en putty: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 spark_streaming_consumer2.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, avg
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# Nueva sesión de Spark y datos de Kafka
spark = SparkSession.builder \
    .appName("OnlineRetailStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "online_retail_data") \
    .load()

# Esquema para el CSV
schema = StructType() \
    .add("InvoiceNo", StringType()) \
    .add("StockCode", StringType()) \
    .add("Description", StringType()) \
    .add("Quantity", IntegerType()) \
    .add("InvoiceDate", StringType()) \
    .add("UnitPrice", DoubleType()) \
    .add("CustomerID", StringType()) \
    .add("Country", StringType())

# Datos JSON
df = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Número de ventas y precio promedio por país
agg_df = df.groupBy("Country").agg(
    count("InvoiceNo").alias("TotalVentas"),
    avg("UnitPrice").alias("PrecioPromedio")
)

# Resultados
query = agg_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
