from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lpad
import json
import requests
import time
import pandas as pd
from functools import reduce
from pyspark.sql import DataFrame
import io
from pymongo import MongoClient



############ PASO 1 SESION SPARK #################################
# SparkSession es el punto de entrada a PySpark. Es como conectarte a una base de datos antes de hacer queries.
spark = SparkSession.builder.appName("OpenDataBCNMWh")\
                            .getOrCreate()
print(f"Spark Version: {spark.version}")
print("Spark session created")

################################# PASO 2 OBTENER CATALOGO #################################

urlCatalogo = (
    "https://opendata-ajuntament.barcelona.cat"
    "/data/api/3/action/package_show"
    "?id=consum-electricitat-bcn"
)

response = requests.get(urlCatalogo)
print (f"status: {response.status_code}")

data = response.json()
resources = data["result"]["resources"]

print ("\n Available resources")
for res in resources:
    name  = res["name"]
    id = res["id"]
    active = res["datastore_active"]
    url = res.get("url", "sin url")
    print(f"{name} | id: {id} | API Availability {active} | url: {url}")


################################# PASO 3 MUESTRA DE DATOS #################################

resources2024 = None
for resource in resources:
    if "2024" in resource["name"]:
        resources2024 = resource["id"]

print(f"\nResource 2024: {resources2024}")

time.sleep(2)

urlDatos = (
    "https://opendata-ajuntament.barcelona.cat"
    "/data/api/action/datastore_search"
    f"?resource_id={resources2024}"
    "&limit=5"
)

response = requests.get(urlDatos)
print (f"status: {response.status_code}")

resultado = requests.get(urlDatos).json()
registros = resultado["result"]["records"]
total = resultado["result"]["total"]

print(f"Total registros en 2024: {total:,}")
print(f"\nPrimeras 5 filas:")
for reg in registros:
    print(f"  {reg}")

############## PASO 3.5 DESCARGAR Y LEER CSVs CON SPARK #################################

open_data_csvs = []

for res in resources:
    if res.get("url") and "2026" not in res["name"]:
        print(f"Descargando {res['name']}...")

        for attempt in range(3):
            try:
                response = requests.get(res["url"], timeout=120)
                response.raise_for_status()
                dfPandas = pd.read_csv(io.BytesIO(response.content), encoding="utf-8")
                dfSpark_year = spark.createDataFrame(dfPandas)
                open_data_csvs.append(dfSpark_year)
                print(f"  OK - {len(dfPandas):,} registros")
                break
            except Exception as e:
                print(f"Intento {attempt + 1} falló: {e}")
                if attempt < 2:
                    time.sleep(5)
                else:
                    print(f"  SKIP: {res['name']}")

        time.sleep(1)

# Unir todos los años en un solo DataFrame


df_spark = reduce(DataFrame.unionByName, open_data_csvs) #reduce aplica función unionByBame
print(f"Total registros todos los años: {df_spark.count():,}")

################################# PASO 4 DATOS A DF SPARK #################################

#dfPandas = pd.DataFrame(registros)
#dfPandas = dfPandas.drop(columns=["_id"])
#
#dfSpark = spark.createDataFrame(dfPandas)
#dfSpark.show(truncate = False)

print("Schema:")
df_spark.printSchema()

print(f"Filas: {df_spark.count()}")


################## PASO 5 TRANSFORMACIONES #################################

df_spark = (
    df_spark
    .withColumn("MWh", col("Valor").cast("integer"))
    .withColumn("Fecha", to_date(col("Data")))
    .withColumn("Codigo_Postal", lpad(col("Codi_Postal"), 5, "0"))
    .drop("Any")
)

df_spark.show(truncate=False)
df_spark.printSchema()
df_spark.count()

################################# PASO 6 Escribir Mongos #################################

#client = MongoClient("mongodb://localhost:27017/") #Servidor local
#db = client["TFM"] #DataBase
#collection = db["Consumo_MWh_BCN"] #Tabla

spark.stop()
