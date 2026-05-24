# FraudShield — Detección de fraude en tiempo real para retail

Arquitectura de Big Data (Kafka + Spark + Delta Lake) para la detección de
fraude en el sector retail. Trabajo de investigación — Maestría en IA.

## Requisitos
- Docker Desktop, Python 3.11, JDK 17

## Puesta en marcha
1. `docker compose up -d`
2. `python -m venv venv` y activar el entorno
3. `pip install -r requirements.txt`
4. Ejecutar el pipeline:
   - `python data/generator.py`
   - `python ingestion/producer.py`
   - `python streaming/app.py`
   - `python ml/train.py`
   - `python generate_gold.py`
   - `python queries/run_analytics.py`