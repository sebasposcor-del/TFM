import json

import polars as pl
import polars.selectors as cs
from pymongo import MongoClient

from base.base_etl import BaseETL


class DatasetBuilder(BaseETL):

    ESTACIONES = {
        "D5": (2.1228, 41.3833),
        "X2": (2.1686, 41.3874),
        "X4": (2.1528, 41.4035),
        "X8": (2.1971, 41.4034),
    }

    def __init__(self):
        super().__init__()

    def extract(self):
        self.datos_modis_lst = pl.DataFrame(list(self.db["clean_modis_lst"].find({}, {"_id": 0})))
        self.datos_electricity = pl.DataFrame(
            list(self.db["clean_electricity"].find({}, {"_id": 0}))
        )
        self.datos_meteocat = pl.DataFrame(list(self.db["clean_meteocat"].find({}, {"_id": 0})))
        self.datos_festivos = pl.DataFrame(list(self.db["clean_festivos"].find({}, {"_id": 0})))

    def transform(self) -> pl.DataFrame:
        modis = self._transform_modis_lst(self.datos_modis_lst)
        electricity = self._transform_electricity(self.datos_electricity)
        festivos = self._transform_festivos(self.datos_festivos)
        meteocat = self._transform_meteocat(self.datos_meteocat)

        return self._build_dataset(electricity, modis, festivos, meteocat)

    def _transform_electricity(self, df: pl.DataFrame) -> pl.DataFrame:
        return (
            df.pivot(
                values="mwh",
                index=["cod_postal", "datetime"],
                on="sector_economic",
                aggregate_function="sum",
            )
            .rename(
                {
                    "Indústria": "mwh_industria",
                    "Residencial": "mwh_residencial",
                    "Serveis": "mwh_servicios",
                    "No especificat": "mwh_no_especificado",
                }
            )
            .with_columns(pl.sum_horizontal(cs.starts_with("mwh_")).alias("mwh_total"))
        )

    def _transform_modis_lst(self, df: pl.DataFrame) -> pl.DataFrame:
        horas = pl.DataFrame({"offset_horas": [0, 6, 12, 18]})

        return (
            df.join(horas, how="cross")
            .with_columns(
                (pl.col("fecha") + pl.duration(hours=pl.col("offset_horas"))).alias("datetime")
            )
            .drop(["fecha", "offset_horas"])
            .select(["cod_postal", "datetime", "lst_celsius"])
        )

    def _transform_festivos(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.col("fecha").str.strptime(pl.Datetime, "%Y-%m-%d").alias("fecha")
        ).select(["fecha", "nombre_local"])

    def _transform_meteocat(self, df: pl.DataFrame) -> pl.DataFrame:
        return (
            df.with_columns(pl.col("data_lectura").dt.truncate("6h").alias("datetime"))
            .group_by(["codi_estacio", "datetime"])
            .agg(
                [
                    pl.col("temp").mean().alias("temp_mean"),
                    pl.col("temp").max().alias("temp_max"),
                    pl.col("temp").min().alias("temp_min"),
                    pl.col("humedad").mean().alias("humedad_mean"),
                    pl.col("viento").mean().alias("viento_mean"),
                    pl.col("precipitacion").sum().alias("precipitacion_sum"),
                    pl.col("irradiancia").mean().alias("irradiancia_mean"),
                ]
            )
            .sort(["codi_estacio", "datetime"])
        )

    def _build_mapeo_estaciones(self) -> pl.DataFrame:
        with open("/home/app/src/data/BARCELONA.geojson") as f:
            geojson = json.load(f)

        def centroide(coordenadas):
            coords = coordenadas[0]
            if isinstance(coords[0][0], list):
                coords = coords[0]
            lon = sum(c[0] for c in coords) / len(coords)
            lat = sum(c[1] for c in coords) / len(coords)
            return lon, lat

        def estacion_mas_cercana(lon, lat):
            return min(
                self.ESTACIONES,
                key=lambda e: (self.ESTACIONES[e][0] - lon) ** 2
                + (self.ESTACIONES[e][1] - lat) ** 2,
            )

        result: pl.DataFrame = pl.DataFrame(
            [
                {
                    "cod_postal": f["properties"]["COD_POSTAL"],
                    "codi_estacio": estacion_mas_cercana(*centroide(f["geometry"]["coordinates"])),
                }
                for f in geojson["features"]
            ]
        ).unique("cod_postal")

        return result

    def _build_dataset(self, electricity, modis, festivos, meteocat) -> pl.DataFrame:
        df_mapeo = self._build_mapeo_estaciones()

        dataset = (
            electricity.join(modis, how="left", on=["cod_postal", "datetime"])
            .with_columns(pl.col("datetime").dt.truncate("1d").alias("fecha_join"))
            .join(festivos, how="left", left_on="fecha_join", right_on="fecha")
            .drop("fecha_join")
            .join(df_mapeo, how="left", on="cod_postal")
            .join(meteocat, how="left", on=["codi_estacio", "datetime"])
            .drop("codi_estacio")
            .with_columns(
                [
                    pl.col("nombre_local").is_not_null().cast(pl.Int8).alias("es_festivo"),
                    pl.col("datetime").dt.hour().alias("hora"),
                    pl.col("datetime").dt.weekday().alias("dia_semana"),
                    pl.col("datetime").dt.month().alias("mes"),
                    pl.col("datetime").dt.year().alias("anio"),
                    pl.col("datetime").dt.week().alias("semana_anio"),
                    (pl.col("datetime").dt.weekday() >= 5).cast(pl.Int8).alias("es_finde"),
                ]
            )
        )

        return dataset

    def load_raw(self, df) -> None:
        pass

    def load_clean(self, df):
        coll = self.db["dataset_eda"]
        coll.drop()
        coll.insert_many(df.to_dicts())
        self.logger.info(f"Dataset guardado: {coll.count_documents({}):,} rows")

    def _validate(self, df: pl.DataFrame) -> None:
        if df.is_empty():
            raise ValueError("El dataset está vacío")

        cols_esperadas = ["datetime", "cod_postal", "mwh_total"]
        for col in cols_esperadas:
            if col not in df.columns:
                raise ValueError(f"Columna crítica ausente: {col}")

        self.logger.info(f"Shape: {df.shape}")
        self.logger.info(f"Filas duplicadas: {df.is_duplicated().sum():,}")
        self.logger.info(f"Nulls:\n{df.null_count()}")

    def run(self):
        self.logger.info("Iniciando proceso de construcción del dataset...")
        self.extract()
        self.logger.info("Extracción completada.")
        df = self.transform()
        self.logger.info("Transformación completada.")
        self._validate(df)
        self.load_clean(df)
        self.logger.info("Proceso de construcción del dataset finalizado.")


if __name__ == "__main__":
    builder = DatasetBuilder()
    builder.run()
