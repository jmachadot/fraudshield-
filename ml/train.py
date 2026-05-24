import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, log, when, lit, rand
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
 
# ==========================================
# Configuración de Rutas
# ==========================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Para esta simulación local, leemos el JSON generado. En prod, leeríamos de lakehouse/gold/
RUTA_DATOS = os.path.join(BASE_DIR, "data", "transacciones_sinteticas.json")
RUTA_MODELO = os.path.join(BASE_DIR, "lakehouse", "modelos", "gbt_fraude")
 
def entrenar_modelo():
    print("Inicializando Spark Session para ML...")
    spark = SparkSession.builder \
        .appName("FraudShield-ML-Training") \
        .getOrCreate()
 
    spark.sparkContext.setLogLevel("WARN")
 
    # 1. Cargar datos históricos
    print("Cargando conjunto de datos históricos...")
    df = spark.read.json(RUTA_DATOS)
 
    # Castear la etiqueta a numérico (requerido por MLlib)
    df = df.withColumn("es_fraude", col("es_fraude").cast("integer"))
 
    # ==========================================
    # Simulación de la Capa Gold (Ingeniería de Características)
    # ==========================================
    print("Aplicando ingeniería de características...")
    # Estabilizar la varianza del monto con logaritmo
    df = df.withColumn("log_monto", log(col("monto") + lit(1.0)))
 
    # Dado que estamos leyendo el JSON crudo para la prueba, simularemos algunas
    # características de ventana que provendrían de la tabla Gold real .
    df = df.withColumn("vel_cliente_5m", (rand() * 5).cast("integer"))
    df = df.withColumn("suma_cliente_60m", col("monto") * (rand() * 3 + 1))
    df = df.withColumn("z_monto_cliente", rand() * 3)
    df = df.withColumn("distancia_geo", rand() * 100)
    df = df.withColumn("velocidad_geo", rand() * 50)
    df = df.withColumn("tasa_fraude_comercio", rand() * 0.05)
    df = df.withColumn("dispositivo_nuevo", when(rand() > 0.9, 1.0).otherwise(0.0))
    df = df.withColumn("horario_atipico", when(rand() > 0.8, 1.0).otherwise(0.0))
 
    # ==========================================
    # Tratamiento de Desbalanceo de Clases
    # ==========================================
    print("Aplicando estrategia de submuestreo y ponderación...")
 
    # Separar clases
    df_fraude = df.filter(col("es_fraude") == 1)
    df_legitimo = df.filter(col("es_fraude") == 0)
 
    total_fraudes = df_fraude.count()
    total_legitimos = df_legitimo.count()
 
    # Submuestreo de la clase mayoritaria (apuntando a ~10% de prevalencia)
    fraccion_submuestreo = (total_fraudes * 9) / total_legitimos
    df_legitimo_muestra = df_legitimo.sample(withReplacement=False, fraction=fraccion_submuestreo, seed=42)
 
    df_balanceado = df_fraude.unionByName(df_legitimo_muestra)
 
    # Calcular pesos para penalizar más los errores en la clase minoritaria
    total_balanceado = df_balanceado.count()
    fraudes_balanceado = df_fraude.count()
    legitimos_balanceado = df_legitimo_muestra.count()
 
    peso_fraude = total_balanceado / (2.0 * fraudes_balanceado)
    peso_legitimo = total_balanceado / (2.0 * legitimos_balanceado)
 
    df_entrenamiento = df_balanceado.withColumn(
        "peso_instancia",
        when(col("es_fraude") == 1, lit(peso_fraude)).otherwise(lit(peso_legitimo))
    )
 
    # El conjunto de prueba debe conservar la prevalencia real (sin submuestreo)
    df_prueba = df.sampleBy("es_fraude", fractions={0: 0.2, 1: 0.2}, seed=42)
 
    # ==========================================
    # Definición y Entrenamiento del Pipeline
    # ==========================================
    print("Ensamblando características y entrenando Gradient Boosted Trees...")
 
    caracteristicas = [
        "log_monto", "vel_cliente_5m", "suma_cliente_60m",
        "z_monto_cliente", "distancia_geo", "velocidad_geo",
        "tasa_fraude_comercio", "dispositivo_nuevo", "horario_atipico"
    ]
 
    ensamblador = VectorAssembler(
        inputCols=caracteristicas,
        outputCol="features",
        handleInvalid="keep"
    )
 
    # GBTClassifier configurado según la investigación
    clasificador = GBTClassifier(
        labelCol="es_fraude",
        featuresCol="features",
        maxDepth=6,
        maxIter=120,
        stepSize=0.1,
        weightCol="peso_instancia",
        seed=42
    )
 
    pipeline = Pipeline(stages=[ensamblador, clasificador])
 
    # Ajustar el modelo
    modelo = pipeline.fit(df_entrenamiento)
 
    # ==========================================
    # Evaluación del Modelo
    # ==========================================
    print("Evaluando el modelo sobre el conjunto de prueba (prevalencia real)...")
    predicciones = modelo.transform(df_prueba)
 
    # Métrica principal: Área bajo la curva de Precisión-Exhaustividad (AUC-PR)
    evaluador_pr = BinaryClassificationEvaluator(
        labelCol="es_fraude",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderPR"
    )
    auc_pr = evaluador_pr.evaluate(predicciones)
 
    # Métrica secundaria: Área bajo la curva ROC (AUC-ROC)
    evaluador_roc = BinaryClassificationEvaluator(
        labelCol="es_fraude",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )
    auc_roc = evaluador_roc.evaluate(predicciones)
 
    print("-" * 40)
    print(f"Resultados de la Evaluación:")
    print(f"AUC-PR:  {auc_pr:.4f} (Métrica principal para datos desbalanceados)")
    print(f"AUC-ROC: {auc_roc:.4f}")
    print("-" * 40)
 
    # ==========================================
    # Guardar el Modelo
    # ==========================================
    print(f"Guardando el modelo serializado en: {RUTA_MODELO}")
    modelo.write().overwrite().save(RUTA_MODELO)
    print("Entrenamiento finalizado exitosamente.")
 
if __name__ == "__main__":
    entrenar_modelo()