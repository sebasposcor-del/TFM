"""Pipeline ETL para consumo eléctrico de Barcelona."""

import io
import time

import polars as pl
import requests

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
                    # BytesIO envuelve  esos bytes en un objeto similar a un archivo que polars puede leer directamente sin necesidad de escribirlo en disco
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
        )

        print(raw_od_bcn.head())
        self.logger.info(f"Transformación completada: {len(raw_od_bcn):,} registros")
        return raw_od_bcn

    def load_clean(self, df: pl.DataFrame) -> None:
        self.logger.info("Guardando datos limpios en MongoDB...")
        self.clean_collection.drop()  # Elimina la colección antes de insertar nuevos datos para evitar duplicados
        self.clean_collection.insert_many(
            df.to_dicts()
        )  # Convierte el DataFrame a una lista de diccionarios y los inserta en MongoDB
        self.logger.info(f"Clean completado: {len(df):,} registros")


if __name__ == "__main__":
    ingester = OpenDataBcnIngester()
    ingester.run()
