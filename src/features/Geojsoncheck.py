# Ejecuta esto para ver qué hay disponible
import json

with open("/home/app/src/data/BARCELONA.geojson") as f:
    geojson = json.load(f)

# Ver propiedades del primer feature
print(geojson["features"][0]["properties"])