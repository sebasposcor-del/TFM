from pymongo import MongoClient
import polars as pl

client = MongoClient('mongodb://mongo:27017/')
records = list(client['tfm_energy']['raw_meteocat'].find({}, {"_id": 0}))
dfpl = pl.DataFrame(records)

print(dfpl.filter(
    (pl.col("codi_estacio") == "D5") &
    (pl.col("codi_variable") == "32")  # ojo, aquí es string porque no has casteado
).shape)