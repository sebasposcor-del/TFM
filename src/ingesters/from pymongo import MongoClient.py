from pymongo import MongoClient
import polars as pl

# Conexión
client = MongoClient("mongodb://mongo:27017/")
db = client["tfm"]

# Leer colección limpia (sin el _id de Mongo)
docs = list(db["dataset_eda"].find({}, {"_id": 0}))
df = pl.DataFrame(docs)

# Exportar
df.write_parquet("/home/app/data/dataset_eda.parquet")

print(f"✅ Exportado: {df.shape[0]:,} filas x {df.shape[1]} columnas")