"""Pipeline ETL para consumo eléctrico de Barcelona."""

import io
import time

import polars as pl
import requests
from pymongo import UpdateOne

from base.base_etl import BaseETL


class OpenDataBcnIngester(BaseETL):
    """Ingesta de consumo elétrcio MwH de Barcelona desde Open Data BC"""

    def __init__(self):
        super().__init__()  # Llama a constructir de clase base para conectar a mongos y traer loggers
        self.catalog_url = (
            "https://opendata-ajuntament.barcelona.cat"
            "/data/api/3/action/package_show"
            "?id=consum-electricitat-bcn"
        )

        self.raw_collection = self.db["raw_electricity"]
        self.clean_collection = self.db["clean_electricity"]
        # Index para uniqueness
        self.clean_collection.create_index(
            [("datetime", 1), ("cod_postal", 1), ("sector_economic", 1)], unique=True
            )

    def extract(self) -> pl.DataFrame:
        """Descarga los csvs de Open Data BCN, los une y devuelve un DataFrame raw"""
        # Qué recibo, que hago?, que devuelvo?
        # 1. Llamo api
        response = requests.get(self.catalog_url, timeout=120)
        response.raise_for_status()
        energy_data = response.json()

        # 2. poor cada recurso, descargo csv
        resources = energy_data["result"]["resources"]
        for res in resources:
            self.logger.info(
                f" Catalogo disponible: {res['name']} | id: {res['id']} | url: {res.get('url', 'sin url')}"
            )

        od_bcn_csvs = []
        for res in resources:
            if not res.get("url"):
                self.logger.warning(f"Recurso {res['name']} no tiene url, no se descargará")
                continue

            for attempt in range(3):
                try:
                    response = requests.get(res["url"], timeout=120)
                    response.raise_for_status()
                    # content son los bytes crudos del csv
                    # BytesIO envuelve  esos bytes en un objeto similar a un archivo
                    #  que polars puede leer directamente sin necesidad de escribirlo en disco
                    od_bcn_year = pl.read_csv(io.BytesIO(response.content))
                    od_bcn_csvs.append(od_bcn_year)
                    self.logger.info(f"Descargado recurso {res['name']} con éxito")
                    break
                except requests.RequestException as e:
                    self.logger.error(
                        f"Error al descargar recurso {res['name']} en intento {attempt + 1}: {e}"
                    )
                    time.sleep(5)  # Espera antes de reintentar
                    continue
            else:
                self.logger.error(
                    f"No se pudo descargar el recurso {res['name']} después de 3 intentos"
                )

        # 3. uno todos los data frames
        raw_od_bcn = pl.concat(od_bcn_csvs, how="vertical")
        # 4. devuelvo data Frame a raw
        return raw_od_bcn

    def load_raw(self, df: pl.DataFrame) -> None:
        self.logger.info("Gurdando datos raw en MongoDB...")
        self.raw_collection.drop()  # Elimina la colección antes de insertar nuevos datos para evitar duplicados
        self.raw_collection.insert_many(df.to_dicts())  # Convierte el DataFrame a una
        self.logger.info("Datos raw guardados con éxito en MongoDB")

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        self.logger.info("Leyendo datos raw de MongosDB...")
        raw_od_bcn_lst = list(
            self.raw_collection.find({}, {"_id": 0})
        )  # Lee los datos raw de MongoDB, excluyendo el campo _id
        self.logger.info(f"Registros leídos de raw: {len(raw_od_bcn_lst)}")
        raw_od_bcn = pl.DataFrame(raw_od_bcn_lst)  # Convierte los
        self.logger.info("Transformando datos raw...")

        no_consta = raw_od_bcn.filter(pl.col("Tram_Horari") == "No consta").shape[0]
        raw_od_bcn = raw_od_bcn.filter(
            pl.col("Tram_Horari") != "No consta"
        )  # Elimina registros con "No consta" en Tram_Horari
        self.logger.info(f"Registros 'No consta' descartados: {no_consta:,}")
        # Aquí iría la lógica de transformación
        raw_od_bcn = (
            (
                raw_od_bcn.with_columns(
                    pl.col("Valor").cast(pl.Int64).alias("MWh"),
                    pl.col("Codi_Postal").cast(pl.String).str.zfill(5),
                    (
                        pl.col("Data")
                        + pl.lit(" ")
                        + pl.col("Tram_Horari")
                        .cast(pl.String)
                        .str.extract(r"De (\d{2}:\d{2}:\d{2})")
                    )
                    .str.to_datetime("%Y-%m-%d %H:%M:%S")
                    .alias("Datetime"),
                )
            )
            .drop("Any", "Data", "Tram_Horari", "Valor")
            .select("Datetime", "Codi_Postal", "Sector_Economic", "MWh")
            .rename({
                "Datetime": "datetime",
                "Codi_Postal": "cod_postal",
                "Sector_Economic": "sector_economic",
                "MWh": "mwh"
            })
        )

        print(raw_od_bcn.head())
        self.logger.info(f"Transformación completada: {len(raw_od_bcn):,} registros")
        return raw_od_bcn

    def load_clean(self, df: pl.DataFrame) -> None:

        self.logger.info("Guardando datos limpios en MongoDB (upsert)...")

        # Convierte el DataFrame de Polars a una lista de diccionarios
        # Cada dict es un registro: {"Datetime": ..., "Codi_Postal": ..., "Sector_Economic": ..., "MWh": ...}
        records = df.to_dicts()

        # Tamaño de cada lote: procesamos 10,000 registros a la vez
        batch_size = 10000

        # Contadores globales para el log final
        total_inserted = 0
        total_modified = 0

        # Recorre los registros de 10,000 en 10,000
        # Con 1.17M registros, 117 batches
        # i toma valores: 0, 10000, 20000, 30000...
        for i in range(0, len(records), batch_size):

            # Corta la lista desde i hasta i+10000
            # Ejemplo: batch 1 = records[0:10000], batch 2 = records[10000:20000], etc.
            batch = records[i : i + batch_size]

            # Para cada registro del batch, crea una operación UpdateOne
            operations = []
            for rec in batch:
                operations.append(
                    UpdateOne(  # Busca UN documento que coincida con  filtro. Si  encuentras, actualízalo. Si no, créalo.
                        # 1er argumento: FILTRO — "busca un doc con esta clave compuesta"
                        {
                            "datetime": rec["datetime"],
                            "cod_postal": rec["cod_postal"],
                            "sector_economic": rec["sector_economic"],
                        },
                        # 2do argumento: ACCIÓN — "reemplaza estos campos con los valores nuevos"
                        {"$set": rec},
                        # 3er argumento: si no lo encuentra, créalo
                        upsert=True,
                    )
                )

            # 0,000 operaciones a MongoDB de una
            result = self.clean_collection.bulk_write(operations)

            # Acumula estadísticas
            total_inserted += result.upserted_count
            total_modified += result.modified_count

        # Resumen
        self.logger.info(
            f"Upsert completado — "
            f"total insertados: {total_inserted}, "
            f"total actualizados: {total_modified}"
        )


if __name__ == "__main__":
    ingester = OpenDataBcnIngester()
    ingester.run()
