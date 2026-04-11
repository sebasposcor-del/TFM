"""Pipeline ETL para datos de temperatura superficial (LST) de MODIS via Google Drive."""

import io

import gdown
import polars as pl
from pymongo import UpdateOne

from base.base_etl import BaseETL
from utils.config import BATCH_SIZE, MODIS_FILE_ID

MODIS_SCALE_FACTOR = 0.02
KELVIN_OFFSET = 273.15


class MODISIngester(BaseETL):
    """Ingesta de datos LST (Land Surface Temperature) de MODIS exportados desde GEE."""

    def __init__(self):
        super().__init__()
        self.raw_col = self.db["raw_modis_lst"]
        self.clean_col = self.db["clean_modis_lst"]

        self.clean_col.create_index(
            [("cod_postal", 1), ("fecha", 1)],
            unique=True,
        )

    def extract(self) -> pl.DataFrame:
        """Descarga el CSV de MODIS LST desde Google Drive a memoria."""
        self.logger.info("Descargando CSV de MODIS LST desde Google Drive...")

        url = f"https://drive.google.com/uc?id={MODIS_FILE_ID}"
        buffer = io.BytesIO()
        gdown.download(url, buffer, quiet=False)
        buffer.seek(0)

        df = pl.read_csv(buffer)
        self.logger.info(f"Descargadas {len(df):,} filas desde Google Drive.")
        return df

    def load_raw(self, df: pl.DataFrame) -> None:
        """Guarda el DataFrame raw en MongoDB (drop + insert completo)."""
        self.raw_col.drop()
        self.raw_col.insert_many(df.to_dicts())
        self.logger.info(f"Raw cargado: {df.shape[0]:,} registros")

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Lee de raw MongoDB, aplica factor de escala MODIS y convierte a Celsius."""
        self.logger.info("Leyendo datos raw de MongoDB...")
        records = list(self.raw_col.find({}, {"_id": 0}))

        if not records:
            raise ValueError("raw_modis_lst está vacía. Ejecuta extract + load_raw primero.")

        self.logger.info(f"Registros leídos de raw: {len(records):,}")

        df = pl.DataFrame(records)

        df = (
            df.with_columns(
                [
                    # Factor de escala MODIS: × 0.02 → Kelvin, luego − 273.15 → Celsius
                    ((pl.col("mean") * MODIS_SCALE_FACTOR) - KELVIN_OFFSET)
                    .round(2)
                    .alias("lst_celsius"),
                    pl.col("fecha").str.strptime(pl.Datetime, "%Y-%m-%d").alias("fecha"),
                    pl.col("COD_POSTAL").cast(pl.Utf8).str.zfill(5).alias("cod_postal"),
                ]
            )
            .drop(["mean", "COD_POSTAL"])
            .select(["cod_postal", "fecha", "lst_celsius"])
        )

        self.logger.info(f"Transform: {df.shape[0]:,} registros procesados.")
        return df

    def load_clean(self, df: pl.DataFrame) -> None:
        """Upsert por clave compuesta (cod_postal + fecha) en batches de 10k."""
        records = df.to_dicts()
        total_inserted = 0
        total_modified = 0

        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i : i + BATCH_SIZE]

            operations = [
                UpdateOne(
                    filter={
                        "cod_postal": rec["cod_postal"],
                        "fecha": rec["fecha"],
                    },
                    update={"$set": rec},
                    upsert=True,
                )
                for rec in batch
            ]

            result = self.clean_col.bulk_write(operations, ordered=False)
            total_inserted += result.upserted_count
            total_modified += result.modified_count
            self.logger.info(f"Batch {i // BATCH_SIZE + 1} procesado")

        self.logger.info(
            f"Upsert completado — insertados: {total_inserted}, actualizados: {total_modified}"
        )


if __name__ == "__main__":
    ingester = MODISIngester()
    ingester.run()
