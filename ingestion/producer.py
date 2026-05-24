import json
import os
import time
from decimal import Decimal
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
 
# Configuración de URLs locales (ajustar si usas Docker o la nube)
KAFKA_BROKER = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC_CRUDAS = "transacciones.crudas"
 
# Rutas de archivos
RUTA_ESQUEMA = os.path.join(os.path.dirname(__file__), "esquema_transaccion.avsc")
RUTA_DATOS = os.path.join(os.path.dirname(__file__), "../data/transacciones_sinteticas.json")
 
def preparar_transaccion(obj, ctx):
    """
    Convierte el float del generador en un Decimal de Python
    para cumplir con el logicalType de Avro.
    """
    obj['monto'] = Decimal(str(obj['monto']))
    return obj
 
def configurar_productor():
    """Configura el productor con idempotencia y validación de esquema."""
    # 1. Cargar el esquema Avro
    with open(RUTA_ESQUEMA, 'r') as f:
        esquema_str = f.read()
 
    # 2. Configurar cliente del Schema Registry
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
 
    # 3. Configurar el serializador Avro
    avro_serializer = AvroSerializer(
        schema_registry_client=sr_client,
        schema_str=esquema_str,
        to_dict=preparar_transaccion
    )
 
    # 4. Configurar el Productor Kafka con garantías exactly-once
    productor = SerializingProducer({
        "bootstrap.servers": KAFKA_BROKER,
        "key.serializer": StringSerializer('utf_8'),
        "value.serializer": avro_serializer,
        "acks": "all",                 # Confirmación de todas las réplicas
        "enable.idempotence": True,    # Publicación idempotente: exactamente una vez
        "linger.ms": 5                 # Pequeño retraso para agrupar mensajes (batching)
    })
 
    return productor
 
def reporte_entrega(err, msg):
    """Callback que se ejecuta cuando Kafka confirma la recepción del evento."""
    if err is not None:
        print(f"Error al entregar mensaje: {err}")
 
def ejecutar_ingesta():
    productor = configurar_productor()
    contador = 0
 
    print(f"Iniciando ingesta de datos hacia el topic '{TOPIC_CRUDAS}'...")
 
    # Leer el archivo línea por línea para simular el flujo en tiempo real
    with open(RUTA_DATOS, 'r', encoding='utf-8') as f:
        for linea in f:
            evento = json.loads(linea)
 
            try:
                # Publicar en Kafka usando id_comercio como clave de partición
                productor.produce(
                    topic=TOPIC_CRUDAS,
                    key=evento["id_comercio"],
                    value=evento,
                    on_delivery=reporte_entrega
                )
 
                # Forzar el envío de los mensajes encolados periódicamente
                productor.poll(0)
                contador += 1
 
                if contador % 5000 == 0:
                    print(f"[{contador}] eventos enviados a Kafka...")
 
            except Exception as e:
                print(f"Excepción publicando el evento {evento['id_transaccion']}: {e}")
 
            # Descomentar la siguiente línea para simular la latencia real de ingreso
            # time.sleep(0.01)
 
    # Asegurarse de que todos los mensajes en la cola se entreguen antes de salir
    print("Vaciando cola de mensajes (flush)...")
    productor.flush()
    print(f"Ingesta finalizada. Total de eventos procesados: {contador}")
 
if __name__ == "__main__":
    ejecutar_ingesta()