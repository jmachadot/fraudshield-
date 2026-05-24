"""
FraudShield - Generador de tablas Gold de ejemplo para la demo local.
=====================================================================
Ubicacion: colocar este archivo en la RAIZ del proyecto  ->  fraudshield/generate_gold.py

Deriva tres tablas analiticas a partir de data/transacciones_sinteticas.json y
las guarda como Parquet en lakehouse/gold/, de modo que las consultas 04, 05 y
06 devuelvan resultados en el entorno local:

  - caracteristicas      -> consulta 06_viaje_imposible.sql
  - alertas              -> consulta 04_clientes_alertas.sql
  - indicadores_modelo   -> consulta 05_evolucion_diaria.sql

En un Lakehouse real estas tablas serian Delta (ver setup_medallion.sql); aqui
se guardan como Parquet para mantener la demo local sin dependencias extra.

Ejecutar una sola vez, despues de generator.py y con el entorno virtual activo:
    python generate_gold.py
"""
import os
import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RUTA_DATOS = os.path.join(BASE_DIR, "data", "transacciones_sinteticas.json")
RUTA_GOLD = os.path.join(BASE_DIR, "lakehouse", "gold")
RADIO_TIERRA_KM = 6371.0


def main():
    if not os.path.exists(RUTA_DATOS):
        print(f"ERROR: no se encontro {RUTA_DATOS}")
        print("Ejecuta primero:  python data/generator.py")
        sys.exit(1)

    spark = SparkSession.builder.appName("FraudShield-Gold").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("Cargando transacciones sinteticas...")
    df = (spark.read.json(RUTA_DATOS)
          .withColumn("es_fraude", F.col("es_fraude").cast("int"))
          .withColumn("ts", F.timestamp_seconds(F.col("timestamp_evento") / 1000)))

    # =====================================================================
    # 1) gold.caracteristicas  ->  consulta 06 (viaje imposible)
    #    Calcula la velocidad geografica real entre transacciones consecutivas
    #    del mismo cliente (distancia haversine / tiempo transcurrido).
    # =====================================================================
    print("Construyendo gold.caracteristicas ...")
    w_cli  = Window.partitionBy("id_cliente").orderBy("timestamp_evento")
    w_5m   = Window.partitionBy("id_cliente").orderBy("timestamp_evento").rangeBetween(-300000, 0)
    w_60m  = Window.partitionBy("id_cliente").orderBy("timestamp_evento").rangeBetween(-3600000, 0)
    w_cliL = Window.partitionBy("id_cliente")
    w_com  = Window.partitionBy("id_comercio")

    lat1 = F.radians(F.lag("latitud").over(w_cli))
    lon1 = F.radians(F.lag("longitud").over(w_cli))
    lat2 = F.radians(F.col("latitud"))
    lon2 = F.radians(F.col("longitud"))
    h = (F.pow(F.sin((lat2 - lat1) / 2), F.lit(2))
         + F.cos(lat1) * F.cos(lat2) * F.pow(F.sin((lon2 - lon1) / 2), F.lit(2)))
    distancia = F.lit(2 * RADIO_TIERRA_KM) * F.asin(F.sqrt(h))
    horas = (F.col("timestamp_evento") - F.lag("timestamp_evento").over(w_cli)) / F.lit(3600000.0)

    caracteristicas = (df
        .withColumn("distancia_geo", F.coalesce(distancia, F.lit(0.0)))
        .withColumn("delta_horas", horas)
        .withColumn("velocidad_geo", F.when(F.col("delta_horas") > 0,
                    F.col("distancia_geo") / F.col("delta_horas")).otherwise(F.lit(0.0)))
        .withColumn("log_monto", F.log(F.col("monto") + F.lit(1.0)))
        .withColumn("vel_cliente_5m", F.count(F.lit(1)).over(w_5m))
        .withColumn("suma_cliente_60m", F.sum("monto").over(w_60m).cast("decimal(12,2)"))
        .withColumn("z_monto_cliente", F.coalesce(
            (F.col("monto") - F.avg("monto").over(w_cliL)) /
            (F.stddev("monto").over(w_cliL) + F.lit(0.001)), F.lit(0.0)))
        .withColumn("tasa_fraude_comercio", F.avg("es_fraude").over(w_com))
        .withColumn("dispositivo_nuevo", F.when(F.col("id_dispositivo").contains("NUEVO"), 1).otherwise(0))
        .withColumn("horario_atipico", F.when(F.hour("ts").between(0, 5), 1).otherwise(0))
        .select(
            "id_transaccion", "id_cliente",
            F.col("ts").alias("timestamp_evento"),
            F.round("log_monto", 4).alias("log_monto"),
            "vel_cliente_5m", "suma_cliente_60m",
            F.round("z_monto_cliente", 4).alias("z_monto_cliente"),
            F.round("distancia_geo", 2).alias("distancia_geo"),
            F.round("velocidad_geo", 2).alias("velocidad_geo"),
            F.round("tasa_fraude_comercio", 4).alias("tasa_fraude_comercio"),
            "dispositivo_nuevo", "horario_atipico"))

    # =====================================================================
    # 2) gold.alertas  ->  consulta 04 (clientes con mas alertas)
    #    El fraude se concentra: ~13% de los clientes son "reincidentes" y
    #    generan la mayor parte de las alertas. La fecha cae en los ultimos
    #    7 dias para que el filtro de la consulta 04 las incluya.
    # =====================================================================
    print("Construyendo gold.alertas ...")
    clientes = (df.select("id_cliente").distinct()
                .withColumn("reincidente", F.rand(7) < F.lit(0.13)))

    trans_marcadas = df.join(clientes, "id_cliente").withColumn(
        "p_alerta",
        F.when(F.col("es_fraude") == 1, F.lit(0.90))
         .when(F.col("reincidente"), F.lit(0.16))
         .otherwise(F.lit(0.012)))

    prob = (F.when(F.col("es_fraude") == 1, F.rand(21) * 0.24 + F.lit(0.75))
             .otherwise(F.rand(22) * 0.34 + F.lit(0.55)))

    alertas = (trans_marcadas
        .filter(F.rand(13) < F.col("p_alerta"))
        .withColumn("id_alerta", F.expr("uuid()"))
        .withColumn("fecha_alerta", F.expr("current_timestamp() - make_dt_interval(rand() * 7)"))
        .withColumn("probabilidad", F.round(prob, 4))
        .withColumn("nivel_riesgo",
            F.when(F.col("probabilidad") >= 0.85, F.lit("ALTO"))
             .when(F.col("probabilidad") >= 0.70, F.lit("MEDIO"))
             .otherwise(F.lit("BAJO")))
        .select("id_alerta", "id_transaccion", "id_cliente",
                "fecha_alerta", "probabilidad", "nivel_riesgo"))

    # =====================================================================
    # 3) gold.indicadores_modelo  ->  consulta 05 (evolucion diaria)
    #    Una fila por dia de los ultimos 30 dias, con cifras coherentes con
    #    el desempeno reportado (recall ~0,81 y precision ~0,87).
    # =====================================================================
    print("Construyendo gold.indicadores_modelo ...")
    indicadores = (spark.range(30)
        .withColumn("fecha", F.expr("date_sub(current_date(), cast(id as int))"))
        .withColumn("verdaderos_positivos", (F.lit(74) + F.floor(F.rand(31) * 19)).cast("int"))
        .withColumn("falsos_positivos",     (F.lit(8)  + F.floor(F.rand(32) * 11)).cast("int"))
        .withColumn("falsos_negativos",     (F.lit(13) + F.floor(F.rand(33) * 12)).cast("int"))
        .select("fecha", "verdaderos_positivos", "falsos_positivos", "falsos_negativos"))

    # =====================================================================
    # Guardar como Parquet en lakehouse/gold/
    # =====================================================================
    caracteristicas = caracteristicas.cache()
    alertas = alertas.cache()
    indicadores = indicadores.cache()

    print("-" * 60)
    for nombre, tdf in [("caracteristicas", caracteristicas),
                        ("alertas", alertas),
                        ("indicadores_modelo", indicadores)]:
        ruta = os.path.join(RUTA_GOLD, nombre)
        n = tdf.count()
        tdf.write.mode("overwrite").parquet(ruta)
        print(f"  gold.{nombre:<20} {n:>7} filas  ->  lakehouse/gold/{nombre}")

    # Resumen orientativo para la demo
    n_viajes = caracteristicas.filter(F.col("velocidad_geo") > 900).count()
    n_rein = alertas.groupBy("id_cliente").count().filter(F.col("count") >= 3).count()
    print("-" * 60)
    print(f"Consulta 06: {n_viajes} transacciones con 'viaje imposible' (velocidad_geo > 900).")
    print(f"Consulta 04: {n_rein} clientes con 3 o mas alertas en los ultimos 7 dias.")
    print("Tablas Gold generadas correctamente en lakehouse/gold/")
    print("Siguiente paso:  python queries/run_analytics.py")

    spark.stop()


if __name__ == "__main__":
    main()