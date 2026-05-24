-- =================================================================
-- FraudShield - Definición de Tablas Delta Lake (Arquitectura Medallón)
-- =================================================================
 
-- CAPA BRONZE: Datos crudos
CREATE DATABASE IF NOT EXISTS bronze;
 
CREATE TABLE IF NOT EXISTS bronze.transacciones (
    id_transaccion STRING,
    timestamp_evento TIMESTAMP,
    id_cliente STRING,
    id_comercio STRING,
    id_terminal STRING,
    canal STRING,
    hash_tarjeta STRING,
    monto DECIMAL(12,2),
    moneda STRING,
    mcc INT,
    tarjeta_presente BOOLEAN,
    pais STRING,
    latitud DOUBLE,
    longitud DOUBLE,
    id_dispositivo STRING,
    ip_origen STRING,
    es_fraude BOOLEAN,
    fecha_ingesta DATE
)
USING DELTA
PARTITIONED BY (fecha_ingesta)
LOCATION 's3://fraudshield-data-lake-2026/bronze/transacciones';
 
-- CAPA SILVER: Datos depurados y conformados
CREATE DATABASE IF NOT EXISTS silver;
 
CREATE TABLE IF NOT EXISTS silver.transacciones (
    id_transaccion STRING,
    timestamp_evento TIMESTAMP,
    id_cliente STRING,
    id_comercio STRING,
    canal STRING,
    monto DECIMAL(12,2),
    es_fraude BOOLEAN
)
USING DELTA
PARTITIONED BY (CAST(timestamp_evento AS DATE))
LOCATION 's3://fraudshield-data-lake-2026/silver/transacciones';
 
CREATE TABLE IF NOT EXISTS silver.comercios (
    id_comercio STRING,
    nombre_comercio STRING,
    categoria_mcc INT,
    pais STRING
)
USING DELTA
LOCATION 's3://fraudshield-data-lake-2026/silver/comercios';
 
-- CAPA GOLD: Agregados, Características y Alertas
CREATE DATABASE IF NOT EXISTS gold;
 
CREATE TABLE IF NOT EXISTS gold.caracteristicas (
    id_transaccion STRING,
    id_cliente STRING,
    timestamp_evento TIMESTAMP,
    log_monto DOUBLE,
    vel_cliente_5m INT,
    suma_cliente_60m DECIMAL(12,2),
    z_monto_cliente DOUBLE,
    distancia_geo DOUBLE,
    velocidad_geo DOUBLE,
    tasa_fraude_comercio DOUBLE,
    dispositivo_nuevo INT,
    horario_atipico INT
)
USING DELTA
LOCATION 's3://fraudshield-data-lake-2026/gold/caracteristicas';
 
CREATE TABLE IF NOT EXISTS gold.alertas (
    id_alerta STRING,
    id_transaccion STRING,
    id_cliente STRING,
    fecha_alerta TIMESTAMP,
    probabilidad DOUBLE,
    nivel_riesgo STRING
)
USING DELTA
LOCATION 's3://fraudshield-data-lake-2026/gold/alertas';