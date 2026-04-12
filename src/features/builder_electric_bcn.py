from pymongo import MongoClient
import polars as pl
import polars.selectors as cs
import json

# ── Conexión ──────────────────────────────────────────────────────────────────
MONGO_URI = "mongodb://mongo:27017/"
MONGO_DB_NAME = "tfm_energy"

client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]

# ── Extracción desde MongoDB ───────────────────────────────────────────────────
datos_modis_lst  = pl.DataFrame(list(db["clean_modis_lst"].find({}, {"_id": 0})))
datos_electricity = pl.DataFrame(list(db["clean_electricity"].find({}, {"_id": 0})))
datos_meteocat   = pl.DataFrame(list(db["clean_meteocat"].find({}, {"_id": 0})))
datos_festivos   = pl.DataFrame(list(db["clean_festivos"].find({}, {"_id": 0})))

# ── Transformación: MODIS (diario → 4 bloques de 6h) ──────────────────────────
horas = pl.DataFrame({"offset_horas": [0, 6, 12, 18]})

datos_modis_lst = (
    datos_modis_lst
    .join(horas, how="cross")
    .with_columns(
        (pl.col("fecha") + pl.duration(hours=pl.col("offset_horas"))).alias("datetime")
    )
    .drop(["fecha", "offset_horas"])
    .select(["cod_postal", "datetime", "lst_celsius"])
)

# ── Transformación: Electricity (pivot sectores) ───────────────────────────────
datos_electricity = (
    datos_electricity
    .pivot(
        values="mwh",
        index=["cod_postal", "datetime"],
        on="sector_economic",
        aggregate_function="sum"
    )
    .rename({
        "Indústria":      "mwh_industria",
        "Residencial":    "mwh_residencial",
        "Serveis":        "mwh_servicios",
        "No especificat": "mwh_no_especificado",
    })
    .with_columns(
        pl.sum_horizontal(cs.starts_with("mwh_")).alias("mwh_total")
    )
)

# ── Transformación: Festivos (string → datetime) ───────────────────────────────
datos_festivos = (
    datos_festivos
    .with_columns(
        pl.col("fecha").str.strptime(pl.Datetime, "%Y-%m-%d").alias("fecha")
    )
    .select(["fecha", "nombre_local"])
)

# ── Transformación: Meteocat (30min → 6h, una fila por estación y bloque) ─────
datos_meteocat = (
    datos_meteocat
    .with_columns(
        pl.col("data_lectura").dt.truncate("6h").alias("datetime")
    )
    .group_by(["codi_estacio", "datetime"])
    .agg([
        pl.col("temp").mean().alias("temp_mean"),
        pl.col("temp").max().alias("temp_max"),
        pl.col("temp").min().alias("temp_min"),
        pl.col("humedad").mean().alias("humedad_mean"),
        pl.col("viento").mean().alias("viento_mean"),
        pl.col("precipitacion").sum().alias("precipitacion_sum"),
        pl.col("irradiancia").mean().alias("irradiancia_mean"),
    ])
    .sort(["codi_estacio", "datetime"])
)

# ── Mapeo código postal → estación meteorológica más cercana ──────────────────
ESTACIONES = {
    "D5": (2.1228, 41.3833),
    "X2": (2.1686, 41.3874),
    "X4": (2.1528, 41.4035),
    "X8": (2.1971, 41.4034),
}

def centroide(coordenadas):
    coords = coordenadas[0]
    if isinstance(coords[0][0], list):
        coords = coords[0]
    lon = sum(c[0] for c in coords) / len(coords)
    lat = sum(c[1] for c in coords) / len(coords)
    return lon, lat

def estacion_mas_cercana(lon, lat):
    return min(ESTACIONES, key=lambda e:
        (ESTACIONES[e][0] - lon) ** 2 + (ESTACIONES[e][1] - lat) ** 2
    )

with open("/home/app/src/data/BARCELONA.geojson") as f:
    geojson = json.load(f)

df_mapeo = pl.DataFrame([
    {"cod_postal": f["properties"]["COD_POSTAL"],
     "codi_estacio": estacion_mas_cercana(*centroide(f["geometry"]["coordinates"]))}
    for f in geojson["features"]
]).unique("cod_postal")

print(f"Datetimes únicos en electricity: {datos_electricity['datetime'].n_unique():,}")
print(f"Datetimes únicos en meteocat agregado: {datos_meteocat['datetime'].n_unique():,}")

# ── Construcción del dataset ───────────────────────────────────────────────────
dataset = (
    datos_electricity
    .join(datos_modis_lst, how="left", on=["cod_postal", "datetime"])
    .with_columns(pl.col("datetime").dt.truncate("1d").alias("fecha_join"))
    .join(datos_festivos, how="left", left_on="fecha_join", right_on="fecha")
    .drop("fecha_join")
    .join(df_mapeo, how="left", on="cod_postal")
    .join(datos_meteocat, how="left", on=["codi_estacio", "datetime"])
    .drop("codi_estacio")
    .with_columns([
        pl.col("nombre_local").is_not_null().cast(pl.Int8).alias("es_festivo"),
        pl.col("datetime").dt.hour().alias("hora"),
        pl.col("datetime").dt.weekday().alias("dia_semana"),
        pl.col("datetime").dt.month().alias("mes"),
        pl.col("datetime").dt.year().alias("anio"),
        pl.col("datetime").dt.week().alias("semana_anio"),
        (pl.col("datetime").dt.weekday() >= 5).cast(pl.Int8).alias("es_finde"),
    ])
)
# ¿En qué años se concentran los nulls de temp_mean?
print(
    dataset
    .filter(pl.col("temp_mean").is_null())
    .group_by("anio")
    .agg(pl.len().alias("nulls"))
    .sort("anio")
)

# ── Validación ────────────────────────────────────────────────────────────────
print(f"Shape:             {dataset.shape}")
print(f"Filas duplicadas:  {dataset.is_duplicated().sum():,}")
print(f"\nNulls por columna:")
print(dataset.null_count())

# ── Carga en MongoDB ──────────────────────────────────────────────────────────
coll = db["dataset_eda"]
coll.drop()
coll.insert_many(dataset.to_dicts())
print(f"\nDataset guardado: {coll.count_documents({}):,} registros")

client.close()