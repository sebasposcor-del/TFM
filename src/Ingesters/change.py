from pymongo import MongoClient
import polars as pl
from datetime import datetime

client = MongoClient("mongodb://mongo:27017/")
db = client["tfm_energy"]

dataset = pl.DataFrame(
    list(db["dataset_eda"].find({}, {"_id": 0})),
    infer_schema_length=None  # revisa todo el dataset
)
print(f"Shape: {dataset.shape}")
print(f"Filas duplicadas: {dataset.is_duplicated().sum():,}")

print(f"\nNulls por columna:")
print(dataset.null_count())

print(f"\nNulls temp_mean por año:")
print(
    dataset
    .filter(pl.col("temp_mean").is_null())
    .group_by("anio")
    .agg(pl.len().alias("nulls"))
    .sort("anio")
)

print(f"\nRango de fechas:")
print(f"Min: {dataset['datetime'].min()}")
print(f"Max: {dataset['datetime'].max()}")

client.close()