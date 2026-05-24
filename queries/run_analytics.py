import os
from pyspark.sql import SparkSession
 
# Rutas
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
QUERIES_DIR = os.path.join(BASE_DIR, "queries")
 
def ejecutar_consultas():
    print("Inicializando motor analítico de Spark SQL...")
    spark = SparkSession.builder \
        .appName("FraudShield-Analytics") \
        .getOrCreate()
 
    spark.sparkContext.setLogLevel("WARN")
 
    # =================================================================
    # Simulación de Vistas Silver y Gold para entorno de desarrollo local
    # En producción, estas tablas ya existirían en el catálogo Delta Lake
    # =================================================================
    ruta_datos = os.path.join(BASE_DIR, "data", "transacciones_sinteticas.json")
 
    try:
        # Cargamos los datos sintéticos y los registramos como tablas temporales
        # para simular las bases de datos "silver" y "gold"
        df_transacciones = spark.read.json(ruta_datos)
 
        # Simular base de datos Silver
        spark.sql("CREATE DATABASE IF NOT EXISTS silver")
        df_transacciones.createOrReplaceTempView("vista_transacciones")
        spark.sql("CREATE OR REPLACE TEMP VIEW transacciones AS SELECT * FROM vista_transacciones")
 
        # En un flujo real, los comercios vienen de una tabla maestra separada
        # Aquí la abstraemos temporalmente para que las consultas JOIN funcionen
        df_comercios = df_transacciones.select("id_comercio", "mcc", "pais").distinct()
        df_comercios = df_comercios.withColumnRenamed("mcc", "categoria_mcc") \
                                   .withColumn("nombre_comercio", df_transacciones["id_comercio"])
        df_comercios.createOrReplaceTempView("comercios")

        # --- Tablas Gold de ejemplo (generadas por generate_gold.py) ---
        ruta_gold = os.path.join(BASE_DIR, "lakehouse", "gold")
        for tabla in ["caracteristicas", "alertas", "indicadores_modelo"]:
            ruta_tabla = os.path.join(ruta_gold, tabla)
            if os.path.isdir(ruta_tabla):
                spark.read.parquet(ruta_tabla).createOrReplaceTempView(tabla)
                print(f"Vista Gold registrada: {tabla}")
            else:
                print(f"AVISO: falta la tabla '{tabla}'. Ejecuta antes generate_gold.py")
 
    except Exception as e:
        print(f"Error cargando los datos base. Asegúrate de haber ejecutado generator.py primero. Detalles: {e}")
        return
 
    # =================================================================
    # Ejecución de Archivos SQL
    # =================================================================
    archivos_sql = sorted([f for f in os.listdir(QUERIES_DIR) if f.endswith('.sql')])
 
    if not archivos_sql:
        print("No se encontraron archivos .sql en la carpeta queries/")
        return
 
    print("\n" + "="*50)
    print("EJECUTANDO BATERÍA DE CONSULTAS ANALÍTICAS")
    print("="*50)
 
    for archivo in archivos_sql:
        ruta_archivo = os.path.join(QUERIES_DIR, archivo)
        print(f"\n--- Ejecutando: {archivo} ---")
 
        with open(ruta_archivo, 'r') as f:
            query = f.read()
 
            # Limpieza básica para el parser de Spark SQL en modo Temp View
            # Quitamos los prefijos de base de datos "silver." y "gold." para
            # compatibilidad con las vistas temporales locales creadas arriba.
            query_limpia = query.replace("silver.", "").replace("gold.", "")
 
            try:
                # Ejecutar y mostrar los primeros 20 resultados
                resultado = spark.sql(query_limpia)
                resultado.show(truncate=False)
            except Exception as e:
                # Algunas consultas fallarán si apuntan a tablas Gold complejas
                # (como gold.alertas) que no simulamos completamente en este script básico.
                print(f"No se pudo evaluar la consulta localmente (requiere tablas completas del Lakehouse).\n{str(e)[:100]}...\n")
 
if __name__ == "__main__":
    ejecutar_consultas()