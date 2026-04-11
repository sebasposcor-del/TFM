from pymongo import MongoClient
import polars as pl

MONGO_URI= "mongodb://mongo:27017/"
MONGO_DB_NAME= "tfm_energy"

client = MongoClient(MONGO_URI) #objeto cliente con conexion a mongo
db = client[MONGO_DB_NAME] #conexión con la base de datos tfm energy


coll_modis = db["clean_modis_lst"] #conexión con la colección clean_modis_lst
datos_modis_lst = pl.DataFrame(list(coll_modis.find({}, { "_id":0})))
print(datos_modis_lst.head())


coll_electricity = db["clean_electricity"] #conexión con la colección clean_electricity_data
datos_electricity = pl.DataFrame(list(coll_electricity.find({}, {"_id":0})))
print(datos_electricity.head())

coll_meteocat = db["clean_meteocat"] #conexión con la colección clean_meteocat
datos_meteocat = pl.DataFrame(list(coll_meteocat.find({}, {"_id":0})))
print(datos_meteocat.head())

coll_festivos = db["clean_festivos"] #conexión con la colección clean_festivos
datos_festivos = pl.DataFrame(list(coll_festivos.find({}, {"_id":0})))
print(datos_festivos.head())


horas = pl.DataFrame({"offset_horas": [0, 6, 12, 18]}).lazy()

datos_modis_lst = (
    datos_modis_lst.lazy()
    .join(horas, how="cross")
    .with_columns(
        (pl.col("fecha") + pl.duration(hours=pl.col("offset_horas"))).alias("datetime")
    )
    .drop(["fecha", "offset_horas"])
    .select(["cod_postal", "datetime", "lst_celsius"])
    .collect()
)

print(datos_modis_lst.head(8))

datos_festivos = (
    datos_festivos.lazy()
    .with_columns(
        pl.col("fecha").str.strptime(pl.Datetime, "%Y-%m-%d").alias("fecha")
    )
    .select(["fecha", "nombre_local"])
    .collect()
)

print(datos_festivos.head())