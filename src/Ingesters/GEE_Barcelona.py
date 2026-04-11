import json
import os
import zipfile

import geopandas as gpd

with open("/home/app/src/data/BARCELONA.geojson", "r", encoding="utf-8") as f:
    data = json.load(f)

print(f"N° de polígonos totales: {len(data['features'])}")

bcn_ciudad = []
for feature in data["features"]:
    postal_code = feature["properties"]["COD_POSTAL"]
    if 8001 <= int(postal_code) <= 8042:
        bcn_ciudad.append(feature)

print(f"N° de polígonos BCN ciudad: {len(bcn_ciudad)}")

data["features"] = bcn_ciudad

with open("/home/app/src/data/BARCELONA_CIUDAD.geojson", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=4)

# Leer GeoJSON con geopandas
gdf = gpd.read_file("/home/app/src/data/BARCELONA_CIUDAD.geojson")

# Crear carpeta para shapefile
os.makedirs("/home/app/src/data/BCN_ciudad", exist_ok=True)

# Guardar como Shapefile
gdf.to_file("/home/app/src/data/BCN_ciudad/BCN_ciudad.shp", driver="ESRI Shapefile")

# Comprimir en zip
with zipfile.ZipFile("/home/app/src/data/BCN_ciudad.zip", "w") as zf:
    for file in os.listdir("/home/app/src/data/BCN_ciudad/"):
        zf.write(f"/home/app/src/data/BCN_ciudad/{file}", file)

print("BCN_ciudad.zip listo")
