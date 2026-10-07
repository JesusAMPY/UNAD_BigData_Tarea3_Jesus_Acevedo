# Tarea 3: Procesamiento de Datos con Apache Spark y Kafka
**Estudiante:** Jesus Antonio Acevedo Montoya  
**Curso:** Big Data - UNAD  
**Grupo:** 202016911_27  

## Descripción de la Solución
Este repositorio contiene la solución práctica para la Tarea 3, abordando dos modalidades de procesamiento de datos sobre un problema de monitoreo de calidad del aire:

1. **Procesamiento Batch (`batch_analysis.py`):** Carga de datos históricos desde HDFS (`datos_sensores_historico.csv`), análisis exploratorio (EDA) y filtrado de alertas ambientales en PySpark.
2. **Procesamiento en Tiempo Real:** 
   - `kafka_producer.py`: Genera telemetría simulada de sensores y la transmite hacia un topic de Apache Kafka.
   - `spark_streaming_consumer.py`: Consume el flujo desde Kafka mediante Spark Structured Streaming y calcula promedios continuos en ventanas de 1 minuto.

## Instrucciones de Ejecución
1. **Batch:** `python3 batch_analysis.py`
2. **Streaming:** 
   - Terminal 1: `python3 kafka_producer.py`
   - Terminal 2: `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py`
