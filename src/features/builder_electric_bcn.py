import json

import polars as pl
import polars.selectors as cs
from pymongo import MongoClient

from base.base_etl import BaseETL


class DatasetBuilder(BaseETL):

    ESTACIONES = {
        "D5": {"coords": (2.1228, 41.4177), "nombre": "Barcelona - Observatori Fabra"},
        "X2": {"coords": (2.1898, 41.3842), "nombre": "Barcelona - Zoo"},
        "X4": {"coords": (2.1656, 41.3793), "nombre": "Barcelona - el Raval"},
        "X8": {"coords": (2.1133, 41.3876), "nombre": "Barcelona - Zona Universitària"},
        "AN": {"coords": (2.1859, 41.3907), "nombre": "Barcelona - Parc de la Ciutadella"},
    }

    NOMBRES_POSTALES = {
        "08001": "Las Ramblas / El Raval",
        "08002": "Barri Gòtic",
        "08003": "La Barceloneta",
        "08004": "Montjuïc / Poble Sec",
        "08005": "El Poblenou",
        "08006": "Sarrià - Sant Gervasi",
        "08007": "Dreta de l'Eixample / Pg. de Gràcia",
        "08008": "Dreta de l'Eixample",
        "08009": "Dreta de l'Eixample",
        "08010": "Pl. Catalunya / Gran Via",
        "08011": "Sant Antoni",
        "08012": "Vila de Gràcia",
        "08013": "La Sagrada Família",
        "08014": "Sants - Montjuïc",
        "08015": "Esquerra de l'Eixample",
        "08016": "Nou Barris",
        "08017": "Sarrià - Sant Gervasi",
        "08018": "Fort Pienc",
        "08019": "El Besòs i el Maresme",
        "08020": "La Verneda",
        "08021": "Sant Gervasi - Galvany",
        "08022": "Les Tres Torres / Bonanova",
        "08023": "Vallcarca i els Penitents",
        "08024": "Gràcia Nova",
        "08025": "El Guinardó",
        "08026": "El Clot / Camp de l'Arpa",
        "08027": "La Sagrera",
        "08028": "Zona Universitària / Les Corts",
        "08029": "Nova Esquerra de l'Eixample",
        "08030": "El Bon Pastor / Sant Andreu",
        "08031": "Vilapicina / Torre Llobeta",
        "08032": "El Carmel / El Guinardó",
        "08033": "Vallbona / Ciutat Meridiana",
        "08034": "Pedralbes / Sarrià",
        "08035": "Sant Genís dels Agudells / Vall d'Hebron",
        "08036": "L'Antiga Esquerra de l'Eixample",
        "08037": "Vila de Gràcia",
        "08038": "Montjuïc / Zona Franca",
        "08039": "El Port / La Barceloneta",
        "08040": "La Zona Franca",
        "08041": "Sant Andreu",
        "08042": "Torre Baró / Nou Barris",
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
        modis       = self._transform_modis_lst(self.datos_modis_lst)
        electricity = self._transform_electricity(self.datos_electricity)
        festivos    = self._transform_festivos(self.datos_festivos)
        meteocat    = self._transform_meteocat(self.datos_meteocat)

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
                    "Indústria":      "mwh_industria",
                    "Residencial":    "mwh_residencial",
                    "Serveis":        "mwh_servicios",
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
                key=lambda e: (self.ESTACIONES[e]["coords"][0] - lon) ** 2
                            + (self.ESTACIONES[e]["coords"][1] - lat) ** 2,
            )

        result = pl.DataFrame([
            {
                "cod_postal":      f["properties"]["COD_POSTAL"],
                "nombre_postal":   self.NOMBRES_POSTALES.get(
                                       f["properties"]["COD_POSTAL"], "Desconocido"
                                   ),
                "centroide_lon":   (c := centroide(f["geometry"]["coordinates"]))[0],
                "centroide_lat":   c[1],
                "codi_estacio":    (est := estacion_mas_cercana(*c)),
                "nombre_estacio":  self.ESTACIONES[est]["nombre"],
                "estacio_lon":     self.ESTACIONES[est]["coords"][0],
                "estacio_lat":     self.ESTACIONES[est]["coords"][1],
            }
            for f in geojson["features"]
        ])

        return result.unique("cod_postal")

    def _build_dataset(self, electricity, modis, festivos, meteocat) -> pl.DataFrame:
        df_mapeo = self._build_mapeo_estaciones()

        dataset = (
            electricity.join(modis, how="left", on=["cod_postal", "datetime"])
            .with_columns(pl.col("datetime").dt.truncate("1d").alias("fecha_join"))
            .join(festivos, how="left", left_on="fecha_join", right_on="fecha")
            .drop("fecha_join")
            .join(df_mapeo, how="left", on="cod_postal")
            .join(meteocat, how="left", on=["codi_estacio", "datetime"])
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
            .select([
                "cod_postal",
                "nombre_postal",
                "centroide_lon",
                "centroide_lat",
                "codi_estacio",
                "nombre_estacio",
                "estacio_lon",
                "estacio_lat",
                "datetime",
                "mwh_total",
                "mwh_industria",
                "mwh_residencial",
                "mwh_servicios",
                "mwh_no_especificado",
                "lst_celsius",
                "temp_mean",
                "temp_max",
                "temp_min",
                "humedad_mean",
                "viento_mean",
                "precipitacion_sum",
                "irradiancia_mean",
                "es_festivo",
                "nombre_local",
                "hora",
                "dia_semana",
                "mes",
                "anio",
                "semana_anio",
                "es_finde",
            ])
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