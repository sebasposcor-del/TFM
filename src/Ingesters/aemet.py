"""Pipeline ETL AEMET de Barcelona."""
import io
import time

import polars as pl
from pymongo import UpdateOne
import requests

#from base.base_etl import BaseETL


#Por cada estación (4):
#    Por cada bloque de 10 días (219 bloques):
#        Request 1 → pedir URL temporal
#        Request 2 → descargar datos reales
#        Guardar en lista
#        sleep(2) para respetar rate limit




#import requests
import json
#
API_KEY = ("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzZWJhc3Bvc2NvckBnbWF"
"pbC5jb20iLCJqdGkiOiI1ZDFhMzEzMi03OGI5LTRkMGMtOGQxMS0xYThiMmMxYzF"
"mZTQiLCJpc3MiOiJBRU1FVCIsImlhdCI6MTc3NTA1OTU3MCwidXNlcklkIjoiNWQx"
"YTMxMzItNzhiOS00ZDBjLThkMTEtMWE4YjJjMWMxZmU0Iiwicm9sZSI6IiJ9.BefIQg"
"hh5Wu7-Y1I2_EMs5po4flPczs92uBBhGIlmkk"
)
#
#url = "https://opendata.aemet.es/opendata/api/valores/climatologicos/inventarioestaciones/todasestaciones"
#
#headers = {"cache-control": "no-cache"}
#response = requests.get(url, params={"api_key": API_KEY}, headers=headers)
#response_json = response.json()
#
#url2 = response_json["datos"]
#
#estaciones = requests.get(url2).json()
#
#i = 0
#for est in estaciones:
#    if "BARCELONA" in est["provincia"]:
#        print(est)
#        i=i+1
#print(f"Total estaciones en Barcelona: {i}")

url = (
    "https://opendata.aemet.es/opendata"
    "/api/valores/climatologicos/diarios"
    "/datos/fechaini/2024-01-01T00:00:00UTC"
    "/fechafin/2024-01-10T23:59:59UTC"
    "/estacion/0201X"
)
headers = {"cache-control": "no-cache"}

response = requests.get(url, params={"api_key": API_KEY}, timeout=30, headers=headers)
drassanes = response.json()

print(response.status_code)
print(drassanes)

url_temp = drassanes




#import requests
# 
#key = 'XXXXX';
#url = 'https://api.meteo.cat/referencia/v1/municipis'
# 
#response = requests.get(url, headers={"Content-Type": "application/json", "X-Api-Key": key})
# 
#print(response.status_code)  #statusCode
#print(response.text) #valors de la resposta
#// TODO fer el tractament que es vulgui amb les dades de resposta