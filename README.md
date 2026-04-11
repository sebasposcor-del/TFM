# Sistema de Predicción Energética y Alerta Temprana en Barcelona
TFM — Máster en Data Science, La Salle — Universitat Ramon Llull

## Quickstart
1. Clona el repo
2. Copia `.env.example` como `.env` y rellena los valores
3. `docker-compose up`
4. `python -m ingesters.electricity_od_bcn`

## Descripción
Sistema de alerta temprana para anticipar picos de demanda eléctrica 
a nivel de barrio en Barcelona con 24–72h de antelación. 
Orientado a operadores municipales para la toma de decisiones preventivas.

## Stack
- **Python 3.11** + Polars + PyMongo
- **MongoDB** — almacenamiento de datos históricos y predicciones
- **Docker** + Docker Compose — entorno reproducible
- **GitHub Actions** — CI automático en cada push

## Fuentes de datos
| Fuente | Contenido |
|---|---|
| Open Data BCN | Consumo eléctrico por código postal (2019–2025) |
| Meteocat XEMA | Meteorología horaria — 4 estaciones Barcelona |
| MODIS LST | Temperatura superficial diaria por código postal |
| REE e·sios | Demanda y precios del sistema eléctrico nacional |

## Estructura
src/
├── base/          # Clase abstracta BaseETL
├── ingesters/     # Pipelines ETL por fuente
├── utils/         # Config, logger
└── tests/         # Tests unitarios

## CI
Cada push ejecuta automáticamente: `black` · `isort` · `pylint` · `mypy` · `pytest`