from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, count, round

# 1. Inicializar SparkSession
spark = SparkSession.builder \
    .appName("Tarea3_Batch_JesusAcevedo") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Cargar Dataset desde HDFS
file_path = "hdfs://localhost:9000/Tarea3/datos_sensores_historico.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

print("\n=== 1. ESQUEMA DEL DATASET ===")
df.printSchema()

print("\n=== 2. MUESTRA DE DATOS HISTÓRICOS (PRIMEROS 10) ===")
df.show(10)

print("\n=== 3. ANÁLISIS EXPLORATORIO (EDA) - ESTADÍSTICAS POR UBICACIÓN ===")
eda_df = df.groupBy("ubicacion").agg(
    count("sensor_id").alias("total_lecturas"),
    round(avg("temperatura"), 2).alias("temp_promedio"),
    round(avg("pm25"), 2).alias("pm25_promedio"),
    max("pm25").alias("pm25_maximo")
).orderBy(col("pm25_promedio").desc())

eda_df.show()

print("\n=== 4. FILTRADO DE ALERTAS AMBIENTALES (PM2.5 > 50.0) ===")
alertas_df = df.filter(col("pm25") > 50.0)
print(f"Total de alertas críticas registradas: {alertas_df.count()}")
alertas_df.show(10)

spark.stop()
