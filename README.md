<div align="center">

# 🛡️ FraudShield

**Plataforma de detección de fraude en tiempo real para el sector retail**

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.5-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.1-00ADD4?logo=databricks&logoColor=white)](https://delta.io/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Terraform](https://img.shields.io/badge/Terraform-AWS-7B42BC?logo=terraform&logoColor=white)](https://www.terraform.io/)
[![License](https://img.shields.io/badge/License-Acad%C3%A9mico-lightgrey)](#licencia)

</div>

Arquitectura de Big Data que evalúa transacciones de comercio minorista en tiempo
casi real y estima la probabilidad de que cada operación sea fraudulenta. El
proyecto adopta un paradigma *streaming-first* (arquitectura Kappa) e integra
ingesta de eventos, procesamiento de flujos, un lago de datos transaccional y un
modelo de aprendizaje automático escalable.

> Trabajo de investigación — Maestría en Inteligencia Artificial · Curso de Big Data.

---

## Tabla de contenido

- [Descripción](#descripción)
- [Arquitectura](#arquitectura)
- [Stack tecnológico](#stack-tecnológico)
- [Estructura del repositorio](#estructura-del-repositorio)
- [Requisitos previos](#requisitos-previos)
- [Instalación](#instalación)
- [Ejecución del pipeline](#ejecución-del-pipeline)
- [Consultas analíticas](#consultas-analíticas)
- [Solución de problemas](#solución-de-problemas)
- [Notas y limitaciones](#notas-y-limitaciones)

---

## Descripción

El fraude transaccional es una de las principales fuentes de pérdida del comercio
minorista, agravada por la operación omnicanal. Detectarlo es un problema de
**Big Data**: exige procesar un gran volumen de transacciones, decidir en
milisegundos y trabajar con una clase fuertemente desbalanceada (el fraude
representa menos del 1 % de las operaciones).

**FraudShield** aborda el problema con una arquitectura **Kappa**: todos los datos
se tratan como un flujo continuo de eventos, gobernado por una única base de
código tanto para el procesamiento en tiempo real como para el reproceso
histórico. El sistema cubre el recorrido completo del dato:

1. **Ingesta** de los eventos transaccionales de todos los canales de venta.
2. **Procesamiento en flujo** con cálculo de características sobre ventanas de tiempo.
3. **Almacenamiento** en un lago de datos transaccional organizado por capas.
4. **Detección** del fraude mediante un modelo de clasificación supervisado.
5. **Explotación analítica** del histórico mediante consultas SQL.

---

## Arquitectura

```
   FUENTES            INGESTA            PROCESAMIENTO         ALMACENAMIENTO         SERVICIO
 ┌──────────┐     ┌─────────────┐     ┌─────────────────┐    ┌────────────────┐    ┌────────────┐
 │ POS      │     │             │     │ Spark Structured│    │  Delta Lake    │    │ Decisión   │
 │ E-commerce├───►│ Apache Kafka├────►│ Streaming       ├───►│  Bronze        ├───►│ Alertas    │
 │ App móvil│     │ (topic +    │     │ (ventanas +     │    │  Silver        │    │ Tablero    │
 │ Pasarela │     │  Schema Reg)│     │  estado)        │    │  Gold          │    │ Reentren.  │
 └──────────┘     └─────────────┘     └────────┬────────┘    └────────────────┘    └────────────┘
                                               │                                         ▲
                                               └────────► Modelo de detección (MLlib) ───┘
```

La plataforma adopta el patrón **Kappa**: un único recorrido de procesamiento de
flujos atiende tanto la puntuación en tiempo real como la construcción del
histórico analítico. El lago de datos se organiza según la **arquitectura
medallón** en tres capas de refinamiento creciente:

| Capa   | Contenido                                                        |
|--------|------------------------------------------------------------------|
| Bronze | Eventos crudos, sin transformación. Fuente única de verdad.      |
| Silver | Datos depurados, validados y sin duplicados.                     |
| Gold   | Tablas de características e indicadores listas para el consumo.  |

---

## Stack tecnológico

| Componente            | Tecnología                        | Función                                              |
|-----------------------|-----------------------------------|------------------------------------------------------|
| Ingesta               | Apache Kafka + Schema Registry    | Registro de eventos distribuido y duradero.          |
| Procesamiento         | Apache Spark Structured Streaming | Cálculo de características en ventana, con estado.   |
| Almacenamiento        | Delta Lake (Lakehouse)            | Lago de datos transaccional con garantías ACID.      |
| Modelo de detección   | Apache Spark MLlib                | Entrenamiento y puntuación (Gradient Boosted Trees). |
| Serialización         | Apache Avro                       | Contrato de datos y evolución del esquema.           |
| Infraestructura       | Docker / Terraform                | Entorno local / aprovisionamiento en la nube.        |

---

## Estructura del repositorio

```
fraudshield/
├── docker-compose.yml          Infraestructura local (Kafka + Schema Registry)
├── requirements.txt            Dependencias de Python
├── generate_gold.py            Genera las tablas Gold de ejemplo para la demo
├── README.md
│
├── infra/
│   └── main.tf                 Infraestructura como código (AWS) — referencia
│
├── ingestion/
│   ├── esquema_transaccion.avsc   Esquema Avro del evento transaccional
│   └── producer.py                Productor de eventos hacia Kafka
│
├── streaming/
│   └── app.py                  Trabajo de Spark Structured Streaming
│
├── lakehouse/
│   └── setup_medallion.sql     Definición de las tablas Delta Lake
│
├── ml/
│   └── train.py                Entrenamiento del modelo de detección
│
├── queries/
│   ├── run_analytics.py        Orquestador de las consultas analíticas
│   ├── 01_tasa_fraude_categoria.sql
│   ├── 02_comercios_mayor_fraude.sql
│   ├── 03_distribucion_horaria.sql
│   ├── 04_clientes_alertas.sql
│   ├── 05_evolucion_diaria.sql
│   └── 06_viaje_imposible.sql
│
└── data/
    └── generator.py            Generador de transacciones sintéticas
```

> Las carpetas de datos, modelos y el entorno virtual se generan al ejecutar el
> pipeline y están excluidas del repositorio mediante `.gitignore`.

---

## Requisitos previos

| Herramienta     | Versión       | Notas                                                    |
|-----------------|---------------|----------------------------------------------------------|
| Docker Desktop  | Reciente      | Incluye `docker compose`. Asignar al menos 4 GB RAM.     |
| Python          | **3.11**      | No usar 3.12 (PySpark 3.5 no lo soporta de forma fiable).|
| JDK             | **17**        | Requerido por PySpark. Configurar `JAVA_HOME`.           |
| Git             | Reciente      | Para clonar el repositorio.                              |

Verificación rápida:

```bash
docker --version
python --version
java -version
```

---

## Instalación

### 1. Clonar el repositorio

```bash
git clone https://github.com/jmachadot/fraudshield-.git
cd fraudshield-
```

### 2. Levantar la infraestructura (Kafka + Schema Registry)

```bash
docker compose up -d
docker compose ps          # los servicios deben aparecer "running" / "healthy"
```

Crear el topic principal de transacciones:

```bash
docker exec fraudshield-kafka kafka-topics --create \
  --topic transacciones.crudas --bootstrap-server kafka:29092 \
  --partitions 6 --replication-factor 1
```

### 3. Crear el entorno virtual de Python

```bash
python -m venv venv
```

Activarlo:

```powershell
# Windows (PowerShell)
venv\Scripts\activate
```
```bash
# macOS / Linux / WSL
source venv/bin/activate
```

Instalar las dependencias (la descarga de PySpark es grande, ~300 MB):

```bash
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

---

## Ejecución del pipeline

Con el entorno virtual activo y la infraestructura levantada, ejecutar los
scripts **en este orden** desde la raíz del proyecto:

| Paso | Comando                          | Descripción                                              |
|------|----------------------------------|----------------------------------------------------------|
| 1    | `python data/generator.py`       | Genera el conjunto de transacciones sintéticas.          |
| 2    | `python ingestion/producer.py`   | Publica los eventos en Kafka.                            |
| 3    | `python streaming/app.py`        | Procesa el flujo y materializa la capa Bronze.           |
| 4    | `python ml/train.py`             | Entrena el modelo de detección (Gradient Boosted Trees). |
| 5    | `python generate_gold.py`        | Genera las tablas Gold de ejemplo.                       |
| 6    | `python queries/run_analytics.py`| Ejecuta la batería de consultas analíticas.              |

> **Nota sobre el paso 3:** `app.py` es un proceso de *streaming* continuo. Cuando
> deje de mostrar lotes con datos nuevos (un `Batch` vacío), detenerlo con
> `Ctrl + C`. El progreso queda guardado en el *checkpoint*.

> **Nota sobre el paso 5:** `generate_gold.py` solo se ejecuta una vez (o cuando se
> regeneren los datos). Para que `run_analytics.py` lea las tablas Gold, este debe
> registrarlas como vistas temporales (ver el script).

---

## Consultas analíticas

El proyecto incluye seis consultas analíticas en `queries/`:

| Consulta                        | Pregunta de negocio                                              |
|---------------------------------|------------------------------------------------------------------|
| `01_tasa_fraude_categoria.sql`  | Tasa de fraude por categoría de comercio.                        |
| `02_comercios_mayor_fraude.sql` | Comercios con mayor monto de fraude.                             |
| `03_distribucion_horaria.sql`   | Distribución horaria de transacciones legítimas y fraudulentas.  |
| `04_clientes_alertas.sql`       | Clientes con mayor número de alertas.                            |
| `05_evolucion_diaria.sql`       | Evolución diaria de la detección (recall y precisión).           |
| `06_viaje_imposible.sql`        | Detección del patrón de "viaje imposible" (clonación de tarjeta).|

---

## Solución de problemas

| Síntoma                                              | Solución                                                        |
|------------------------------------------------------|-----------------------------------------------------------------|
| `dependency kafka failed to start`                   | El `CLUSTER_ID` debe ser un UUID válido. Reiniciar con `docker compose down -v`. |
| `ModuleNotFoundError: No module named 'numpy'`       | `python -m pip install numpy`                                   |
| `app.py` o `train.py` fallan al escribir archivos en Windows | Ejecutar los scripts dentro de **WSL2**; dejar Kafka en Docker. |
| El productor no conecta con Kafka                    | Verificar que el contenedor figura como `healthy`.              |
| Las consultas Gold no devuelven datos                | Ejecutar antes `generate_gold.py`.                              |
| `git push` → `Repository not found`                  | Corregir la URL del remote con `git remote set-url origin <URL>`. |

---

## Notas y limitaciones

- El archivo `infra/main.tf` (Terraform) describe el aprovisionamiento en la nube
  (AWS) y **no se utiliza en la ejecución local**; se incluye como referencia de
  la arquitectura productiva.
- Los datos son **sintéticos**: el generador inyecta patrones de fraude conocidos
  de forma controlada, por lo que las métricas constituyen una cota optimista del
  desempeño esperable en producción.
- En el entorno local, las tablas Gold se materializan como **Parquet**; en un
  Lakehouse real serían tablas **Delta** (ver `setup_medallion.sql`).
- En Windows nativo, Spark requiere `winutils.exe` y `HADOOP_HOME` para escribir
  archivos; la alternativa recomendada es trabajar dentro de **WSL2**.

---

## Licencia

Proyecto académico desarrollado con fines educativos en el marco de la Maestría
en Inteligencia Artificial.
