import random
import uuid
import json
import os
from datetime import datetime, timedelta
 
# Configuración de simulación basada en los parámetros del documento
NUM_CLIENTES = 2000
NUM_COMERCIOS = 300
TOTAL_TRANSACCIONES = 50000
PREVALENCIA_FRAUDE = 0.004  # 0.4% de fraude
RUTA_SALIDA = "transacciones_sinteticas.json"
 
def generar_comercios(n):
    """Genera un catálogo maestro de comercios ficticios."""
    comercios = []
    for _ in range(n):
        comercios.append({
            "id_comercio": f"COM-{uuid.uuid4().hex[:8].upper()}",
            "mcc": random.choice([5411, 5812, 5912, 5541, 5311]), # Códigos de categoría retail
            "pais": "PE" # Perú
        })
    return comercios
 
def generar_clientes(n):
    """Genera una población de clientes con perfiles de comportamiento base."""
    clientes = []
    for _ in range(n):
        clientes.append({
            "id": f"CLI-{uuid.uuid4().hex[:8].upper()}",
            "monto_medio": random.uniform(20.0, 400.0),
            "lat": random.uniform(-12.5, -11.5),  # Coordenadas geográficas base
            "lon": random.uniform(-77.5, -76.5),
            "reloj": datetime(2026, 1, 1, random.randint(0, 23), random.randint(0, 59)),
            "dispositivo_habitual": f"DEV-{uuid.uuid4().hex[:8].upper()}",
            "hash_tarjeta": f"HASH-{uuid.uuid4().hex[:16].upper()}"
        })
    return clientes
 
def generar_transaccion(cliente, comercios, es_fraude=False):
    """
    Genera un evento transaccional respetando el esquema Avro de FraudShield.
    Inyecta patrones de fraude si la bandera es_fraude es True.
    """
    comercio = random.choice(comercios)
 
    # Simular el avance del tiempo entre transacciones del mismo cliente
    cliente["reloj"] += timedelta(minutes=random.randint(5, 2880)) # Entre 5 mins y 2 días
 
    # Estructura base alineada al esquema Avro
    evento = {
        "id_transaccion": str(uuid.uuid4()),
        "timestamp_evento": int(cliente["reloj"].timestamp() * 1000), # timestamp-millis
        "id_cliente": cliente["id"],
        "id_comercio": comercio["id_comercio"],
        "id_terminal": f"TERM-{random.randint(100, 999)}" if random.random() > 0.3 else None,
        "canal": random.choice(["POS", "ECOMMERCE", "APP_MOVIL"]),
        "hash_tarjeta": cliente["hash_tarjeta"],
        "monto": round(abs(random.gauss(cliente["monto_medio"], 25.0)), 2), # Evitar montos negativos
        "moneda": "PEN",
        "mcc": comercio["mcc"],
        "tarjeta_presente": True,
        "pais": comercio["pais"],
        "latitud": cliente["lat"] + random.uniform(-0.05, 0.05), # Ligera variación de movimiento
        "longitud": cliente["lon"] + random.uniform(-0.05, 0.05),
        "id_dispositivo": cliente["dispositivo_habitual"],
        "ip_origen": f"190.23.{random.randint(0,255)}.{random.randint(0,255)}",
        "es_fraude": False # Etiqueta real para entrenamiento
    }
 
    # Ajustar lógica de negocio de canales
    if evento["canal"] in ["ECOMMERCE", "APP_MOVIL"]:
        evento["tarjeta_presente"] = False
        evento["id_terminal"] = None
 
    # Lógica de inyección de fraude
    if es_fraude:
        patron = random.choice(["rafaga", "monto_alto", "viaje_imposible"])
 
        if patron == "rafaga":
            evento["monto"] = round(random.uniform(1.0, 8.0), 2)
            # Para simular ráfaga en la realidad, el productor debería enviar varias seguidas
 
        elif patron == "monto_alto":
            evento["monto"] = round(cliente["monto_medio"] * random.uniform(8, 20), 2)
 
        elif patron == "viaje_imposible":
            evento["latitud"] = cliente["lat"] + random.uniform(15, 40)
            evento["longitud"] = cliente["lon"] + random.uniform(15, 40)
            evento["tarjeta_presente"] = False
            evento["canal"] = "ECOMMERCE"
            evento["id_dispositivo"] = f"DEV-NUEVO-{uuid.uuid4().hex[:4].upper()}"
            evento["ip_origen"] = f"45.10.{random.randint(0,255)}.{random.randint(0,255)}"
 
        evento["es_fraude"] = True
 
    return evento
 
def ejecutar_simulacion():
    print("Inicializando simulador de FraudShield...")
    comercios = generar_comercios(NUM_COMERCIOS)
    clientes = generar_clientes(NUM_CLIENTES)
 
    transacciones = []
    fraudes_inyectados = 0
 
    print(f"Generando {TOTAL_TRANSACCIONES} transacciones...")
    for i in range(TOTAL_TRANSACCIONES):
        cliente = random.choice(clientes)
        es_fraude = random.random() < PREVALENCIA_FRAUDE
 
        if es_fraude:
            fraudes_inyectados += 1
 
        evento = generar_transaccion(cliente, comercios, es_fraude)
        transacciones.append(evento)
 
        if (i + 1) % 10000 == 0:
            print(f"Progreso: {i + 1} / {TOTAL_TRANSACCIONES} completadas.")
 
    # Ordenar cronológicamente para simular un flujo real en Kafka
    transacciones.sort(key=lambda x: x["timestamp_evento"])
 
    # Guardar a disco
    ruta_completa = os.path.join(os.path.dirname(__file__), RUTA_SALIDA)
    with open(ruta_completa, 'w', encoding='utf-8') as f:
        for t in transacciones:
            f.write(json.dumps(t) + "\n")
 
    print("-" * 40)
    print("Simulación finalizada exitosamente.")
    print(f"Total transacciones: {len(transacciones)}")
    print(f"Total fraudes: {fraudes_inyectados} ({(fraudes_inyectados/TOTAL_TRANSACCIONES)*100:.2f}%)")
    print(f"Archivo exportado: {ruta_completa}")
 
if __name__ == "__main__":
    ejecutar_simulacion()