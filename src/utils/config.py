"""Configuración centralizada del proyecto TFM — Energy Prediction BCN."""

import os
# ─────────────────────────────────────────
# MongoDB
# ─────────────────────────────────────────
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "tfm_energy")

# ─────────────────────────────────────────
# Open Data BCN — Consumo eléctrico
# ─────────────────────────────────────────
ODBC_CATALOG_URL = (
    "https://opendata-ajuntament.barcelona.cat"
    "/data/api/3/action/package_show"
    "?id=consum-electricitat-bcn"
)

# ─────────────────────────────────────────
# Meteocat XEMA (via Transparència Catalunya)
# ─────────────────────────────────────────
METEOCAT_BASE_URL = "https://analisi.transparenciacatalunya.cat/resource/nzvn-apee.json"

METEOCAT_STATIONS = ["D5", "X2", "X4", "X8", "AN"]

METEOCAT_VARIABLES = ["30", "32", "33", "34", "35", "36"]

METEOCAT_YEARS = list(range(2019, 2026))

# ─────────────────────────────────────────
# FESTIVOS
# ─────────────────────────────────────────
NAGER_BASE_URL  = "https://date.nager.at/api/v3/PublicHolidays/{año}/ES"

# ─────────────────────────────────────────
# Pipeline — parámetros generales
# ─────────────────────────────────────────
PAGE_SIZE = 50_000   # Registros por página en peticiones paginadas
BATCH_SIZE = 10_000  # Registros por batch en upserts a MongoDB