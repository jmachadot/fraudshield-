import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, sum, avg, stddev, expr, to_date
from pyspark.sql.avro.functions import from_avro
 
# ==========================================
# Configuración de Rutas y Conexiones
# ==========================================
KAFKA_BROKER = "localhost:9092"
TOPIC_CRUDAS = "transacciones.crudas"
 
# Rutas locales (simulando el almacenamiento de objetos)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUTA_ESQUEMA = os.path.join(BASE_DIR, "ingestion", "esquema_transaccion.avsc")
RUTA_BRONZE = os.path.join(BASE_DIR, "lakehouse", "bronze", "transacciones")
CHECKPOINT_BRONZE = os.path.join(BASE_DIR, "lakehouse", "_checkpoints", "bronze")
 
def iniciar_procesamiento():
    # 1. Leer el esquema Avro previamente definido
    with open(RUTA_ESQUEMA, 'r') as f:
        esquema_transaccion_avro = f.read()
 
    # 2. Inicializar Spark Session con soporte para Delta Lake y Kafka
    print("Inicializando Spark Session...")
    spark = (SparkSession.builder
             .appName("FraudShield-Streaming")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.driver.host", "127.0.0.1")
             .config("spark.driver.bindAddress", "127.0.0.1")
             .getOrCreate())
 
    spark.sparkContext.setLogLevel("WARN")
 
    # ==========================================
    # Fase A: Lectura del flujo y aplicación del esquema
    # ==========================================
    print(f"Conectando al topic '{TOPIC_CRUDAS}' en Kafka...")
    flujo_crudo = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC_CRUDAS)
        .option("startingOffsets", "earliest") # Usamos earliest para procesar lo generado en la simulación
        .option("maxOffsetsPerTrigger", 2000) # <--- LA CLAVE PARA NO SATURAR LA RAM
        .load()
    )
 
    # Deserialización Avro
    transacciones = (
        flujo_crudo
        # Recortar los 5 bytes de cabecera que inyecta Confluent Kafka
        .withColumn("avro_puro", expr("substring(value, 6, length(value))"))
        .select(from_avro(col("avro_puro"), esquema_transaccion_avro).alias("t"))
        .select("t.*")
        # Castear el monto al tipo decimal especificado y crear columna de partición
        .withColumn("monto", col("monto").cast("decimal(12,2)"))
        .withColumn("fecha_ingesta", to_date(col("timestamp_evento")))
    )
 
    # ==========================================
    # Fase B: Ingeniería de características en ventana
    # ==========================================
    # Convertimos timestamp_evento (milisegundos) a tipo Timestamp de Spark para las ventanas
    transacciones_con_tiempo = transacciones.withColumn(
        "tiempo_evento", col("timestamp_evento")
    )
 
    print("Configurando cálculo de características en ventanas de tiempo...")
    caracteristicas_cliente = (
        transacciones_con_tiempo
        # Tolerancia a datos tardíos (Watermark de 10 minutos)
        .withWatermark("tiempo_evento", "10 minutes")
        .groupBy(
            col("id_cliente"),
            window(col("tiempo_evento"), "5 minutes", "1 minute")
        )
        .agg(
            count("*").alias("vel_cliente_5m"),
            sum("monto").alias("suma_cliente_5m"),
            avg("monto").alias("monto_medio_5m"),
            stddev("monto").alias("monto_desv_5m")
        )
    )
 
    # Nota: En una ejecución productiva real, este dataframe de 'caracteristicas_cliente'
    # se cruza (join) con el flujo principal para enviarlo al motor de puntuación.
 
    # ==========================================
    # Fase C: Almacenamiento Lakehouse (Capa Bronze)
    # ==========================================
    print("Iniciando escritura en Delta Lake (Capa Bronze)...")
    consulta_bronze = (
        transacciones.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_BRONZE)
        .partitionBy("fecha_ingesta")
        .start(RUTA_BRONZE)
    )
 
    # Para monitoreo por consola en entorno de desarrollo
    consulta_consola = (
        caracteristicas_cliente.writeStream
        .format("console")
        .outputMode("update")
        .option("truncate", False)
        .start()
    )
 
    # Mantener el proceso en ejecución
    spark.streams.awaitAnyTermination()
 
if __name__ == "__main__":
    iniciar_procesamiento()