# Sistema de Predicción Energética y Alerta Temprana en Barcelona
TFM — Máster en Data Science, La Salle — Universitat Ramon Llull

## Quickstart
1. Clona el repo
2. Copia `.env.example` como `.env` y rellena los valores
3. `docker-compose up`
4. Ejecuta los ingestores en orden:
```bash
   python -m ingesters.electricity_od_bcn
   python -m ingesters.Meteocat_xema
   python -m ingesters.MODIS_LST_BCN
   python -m ingesters.festivos
```
5. Construye el dataset EDA:
```bash
   python -m features.builder_electric_bcn
```

## Descripción
Sistema de alerta temprana para anticipar picos de demanda eléctrica
a nivel de barrio en Barcelona con 24–72h de antelación.
Orientado a operadores municipales para la toma de decisiones preventivas.

## Stack
- **Python 3.11** + Polars + PyMongo
- **MongoDB 7.0** — almacenamiento de datos históricos y predicciones
- **Docker** + Docker Compose — entorno reproducible
- **GitHub Actions** — CI automático en cada push (black · isort · pylint · mypy · pytest)

## Fuentes de datos
| Fuente | Contenido | Estado |
|---|---|---|
| Open Data BCN | Consumo eléctrico por código postal (2019–2025) | ✅ |
| Meteocat XEMA | Meteorología horaria — 4 estaciones Barcelona | ✅ |
| MODIS LST | Temperatura superficial diaria por código postal | ✅ |
| REE e·sios | Demanda y precios del sistema eléctrico nacional | 🔄 Pendiente |
| Endolla Barcelona | Puntos de recarga VE por zona | 🔄 Pendiente |

## Dataset EDA
424,148 registros · 23 columnas · granularidad 6h por código postal (2019–2025)

## Estructura
src/
├── base/          # Clase abstracta BaseETL
├── ingesters/     # Pipelines ETL por fuente de datos
├── features/      # Construcción del dataset final
├── utils/         # Config centralizada y logger
└── tests/         # Tests unitarios

## CI
Cada push a `main` o `develop` ejecuta automáticamente:
`black` · `isort` · `pylint` · `mypy` · `pytest`

## Notas técnicas
- mypy: se suprime `no-any-return` en `_build_mapeo_estaciones` por limitación
  de stubs de Polars con `.unique()` — el tipo es correcto en tiempo de ejecución
- REE e·sios y Endolla BCN pendientes de token y datos respectivamente