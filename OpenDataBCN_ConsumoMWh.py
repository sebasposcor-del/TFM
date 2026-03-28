from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lpad
import json
import requests
import time
import pandas as pd
from functools import reduce
from pyspark.sql import DataFrame


############ PASO 1 SESION SPARK #################################
# SparkSession es el punto de entrada a PySpark. Es como conectarte a una base de datos antes de hacer queries.
spark = SparkSession.builder.appName("OpenDataBCNMWh")\
                            .getOrCreate()
print(f"Spark Version: {spark.version}")
print("Spark session has been created")

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

import io

dfs = []

for res in resources:
    if res.get("url") and "2025" not in res["name"]:  # 2025 no tiene API disponible aún
        print(f"Descargando {res['name']}...")
        response = requests.get(res["url"])
        
        #  CSV con pandas desde memoria a Spark
        dfPandas = pd.read_csv(io.StringIO(response.text))
        dfSpark_year = spark.createDataFrame(dfPandas)
        dfs.append(dfSpark_year)
        time.sleep(1)  # Respetar el servidor

# Unir todos los años en un solo DataFrame


dfSpark = reduce(DataFrame.unionByName, dfs)
print(f"Total registros todos los años: {dfSpark.count():,}")

################################# PASO 4 DATOS A DF SPARK #################################

#dfPandas = pd.DataFrame(registros)
#dfPandas = dfPandas.drop(columns=["_id"])
#
#dfSpark = spark.createDataFrame(dfPandas)
#dfSpark.show(truncate = False)

print("Schema:")
dfSpark.printSchema()

print(f"Filas: {dfSpark.count()}")


################## PASO 5 TRANSFORMACIONES #################################

dfSpark = (
    dfSpark
    .withColumn("Valor", col("Valor").cast("integer"))
    .withColumn("Data", to_date(col("Data")))
    .withColumn("Codi_Postal", lpad(col("Codi_Postal"), 5, "0"))
    .drop("Any")
)

dfSpark.show(truncate=False)
dfSpark.printSchema()
dfSpark.count()

################################# PASO 6 Escribir Mongos #################################
spark.stop()
