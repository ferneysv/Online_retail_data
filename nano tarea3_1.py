# para acceder al nano: nano tarea3_1.py
# se ejecuta primero y luego se ejecuta en el navegador para visualizar los datos en spark
# comando para ejecutar: python3 tarea3_.py
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count

# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('Tarea3').getOrCreate()

# Define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea3/online_retail.csv'

# Lee el archivo .csv
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path)

# Imprime el esquema
df.printSchema()

# Muestra las primeras filas del DataFrame
df.show()

# Estadísticas básicas
df.summary().show()

# Contar los valores nulos
null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
null_counts.show()
