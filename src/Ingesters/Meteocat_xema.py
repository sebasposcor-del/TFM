"""Pipeline ETL para datos meteorológicos de Meteocat (XEMA) via Transparència Catalunya."""

import time

import polars as pl
import requests
from pymongo import UpdateOne

from base.base_etl import BaseETL
from utils.config import (
    BATCH_SIZE,
    METEOCAT_BASE_URL,
    METEOCAT_STATIONS,
    METEOCAT_VARIABLES,
    METEOCAT_YEARS,
    PAGE_SIZE,
)


class MeteocatIngester(BaseETL):
    """Ingesta de datos meteorológicos XEMA de Meteocat via Transparència Catalunya."""

    def __init__(self):
        super().__init__()
        self.raw_col = self.db["raw_meteocat"]
        self.clean_col = self.db["clean_meteocat"]

        self.clean_col.create_index(
            [("codi_estacio", 1), ("codi_variable", 1), ("data_lectura", 1)],
            unique=True,
        )

    def extract(self) -> pl.DataFrame:
        """Descarga registros paginando por estación, año y mes."""
        all_records = []

        for station in METEOCAT_STATIONS:
            for year in METEOCAT_YEARS:
                for month in range(1, 13):  # 1 a 12
                    # Calcular último día del mes
                    if month == 12:
                        next_month_start = f"{year + 1}-01-01T00:00:00"
                    else:
                        next_month_start = f"{year}-{month + 1:02d}-01T00:00:00"

                    month_start = f"{year}-{month:02d}-01T00:00:00"
                    self.logger.info(f"Descargando estacio={station} {year}-{month:02d}")
                    offset = 0

                    while True:
                        url = (
                            f"{METEOCAT_BASE_URL}"
                            f"?$where=codi_estacio='{station}'"
                            f" AND codi_variable in({','.join(repr(v) for v in METEOCAT_VARIABLES)})"
                            f" AND data_lectura between '{month_start}' and '{next_month_start}'"
                            f"&$limit={PAGE_SIZE}"
                            f"&$offset={offset}"
                        )

                        for attempt in range(3):
                            try:
                                response = requests.get(url, timeout=120)
                                response.raise_for_status()
                                break
                            except requests.RequestException as e:
                                self.logger.warning(f"Intento {attempt + 1}/3 fallido: {e}")
                                if attempt == 2:
                                    raise
                                time.sleep(10)

                        batch = response.json()

                        if not batch:
                            break

                        all_records.extend(batch)
                        self.logger.info(f"  offset={offset} → {len(batch)} registros")

                        if len(batch) < PAGE_SIZE:
                            break

                        offset += PAGE_SIZE

        self.logger.info(f"Total extraídos: {len(all_records):,}")
        return pl.DataFrame(all_records)

    def load_raw(self, df: pl.DataFrame) -> None:
        """Guarda el DataFrame raw en MongoDB (drop + insert completo)."""
        self.raw_col.drop()
        self.raw_col.insert_many(df.to_dicts())
        self.logger.info(f"Raw cargado: {df.shape[0]:,} registros")

    def transform(self, df: pl.DataFrame = None) -> pl.DataFrame:
        """Lee de raw MongoDB, limpia y tipifica. Devuelve DataFrame limpio."""
        self.logger.info("Leyendo datos raw de MongoDB...")
        records = list(self.raw_col.find({}, {"_id": 0}))
        self.logger.info(f"Registros leídos de raw: {len(records):,}")

        dfpl = pl.DataFrame(records)

        dfpl = (
            dfpl.with_columns(
                [
                    pl.col("data_lectura").str.to_datetime("%Y-%m-%dT%H:%M:%S%.f"),
                    pl.col("valor_lectura").cast(pl.Float64),
                    pl.col("codi_variable").cast(pl.Int32),
                ]
            )
            .filter(pl.col("codi_estat") == "V")
            .drop(["id", "codi_base", "codi_estat"])
        )

        self.logger.info(f"Transform: {dfpl.shape[0]:,} registros válidos")
        return dfpl

    def load_clean(self, df: pl.DataFrame) -> None:
        """Upsert por clave compuesta (estació + variable + fecha) en batches de 10k."""
        records = df.to_dicts()
        total_inserted = 0
        total_modified = 0

        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i : i + BATCH_SIZE]

            operations = []
            for rec in batch:
                operations.append(
                    UpdateOne(
                        filter={
                            "codi_estacio": rec["codi_estacio"],
                            "codi_variable": rec["codi_variable"],
                            "data_lectura": rec["data_lectura"],
                        },
                        update={"$set": rec},
                        upsert=True,
                    )
                )

            result = self.clean_col.bulk_write(operations, ordered=False)
            total_inserted += result.upserted_count
            total_modified += result.modified_count
            self.logger.info(f"Batch {i // BATCH_SIZE + 1} procesado")

        self.logger.info(
            f"Upsert completado — insertados: {total_inserted}, actualizados: {total_modified}"
        )


if __name__ == "__main__":
    ingester = MeteocatIngester()
    ingester.run()
