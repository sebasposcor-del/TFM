import polars as pl
import requests
from pymongo import UpdateOne

from base.base_etl import BaseETL
from utils.config import BATCH_SIZE, NAGER_BASE_URL


class FestivosIngester(BaseETL):
    """Ingesta de festivos nacionales y de Cataluña, con foco en BCN."""

    def __init__(self):
        super().__init__()
        self.url = NAGER_BASE_URL
        self.raw_col = self.db["raw_festivos"]  # ← self. para guardarla
        self.clean_col = self.db["clean_festivos"]  # ← self. para guardarla
        self.clean_col.create_index("fecha", unique=True)

    def extract(self) -> pl.DataFrame:
        calendarios = []

        for año in range(2019, 2027):
            datos_festivos_año = requests.get(self.url.format(año=año)).json()
            df_año = pl.DataFrame(datos_festivos_año)
            calendarios.append(df_año)
            self.logger.info(f" {año}: {len(datos_festivos_año)} festivos descargados")

        return pl.concat(calendarios)

    def load_raw(self, df: pl.DataFrame) -> None:
        """Sube datos a MongoDB, reemplaza colección raw_festivos."""
        self.raw_col.drop()
        self.raw_col.insert_many(df.to_dicts())
        self.logger.info(f"Raw cargado: {df.shape[0]} registros")

    def transform(self) -> pl.DataFrame:
        """Filtra festivos de BCN, limpia columnas y añade La Mercè."""
        records = list(self.raw_col.find({}, {"_id": 0}))
        df = pl.DataFrame(records)  # ← aquí nace df

        df_limpio = (
            df.filter(pl.col("global") | pl.col("counties").list.contains("ES-CT"))
            .select("date", "localName", "name")
            .rename({"date": "fecha", "localName": "nombre_local", "name": "nombre_oficial"})
        )

        la_merce = pl.DataFrame(
            {
                "fecha": [f"{año}-09-24" for año in range(2019, 2027)],
                "nombre_local": ["La Mercè"] * 8,
                "nombre_oficial": ["La Mercè (Barcelona)"] * 8,
            }
        )
        return pl.concat([df_limpio, la_merce], how="vertical").sort("fecha")

    def load_clean(self, df: pl.DataFrame) -> None:
        """Upsert del DataFrame limpio en MongoDB, usando 'fecha' como clave única."""
        records = df.to_dicts()
        total_inserted = 0
        total_modified = 0

        for i in range(0, len(records), BATCH_SIZE):  # for each batch

            batch = records[i : i + BATCH_SIZE]
            operations = []

            for rec in batch:  # for each rec in batch
                operations.append(
                    UpdateOne(
                        filter={  # busca registro con misma fehca, nombre u nombre oficial
                            "fecha": rec["fecha"],
                            "nombre_local": rec["nombre_local"],
                            "nombre_oficial": rec["nombre_oficial"],
                        },
                        update={"$set": rec},  # haz esto si lo encuentra
                        upsert=True,  # si no encuentras, haz upsert (insertar nuevo registro)
                    )
                )

            results = self.clean_col.bulk_write(operations)
            total_inserted += results.upserted_count
            total_modified += results.modified_count
            self.logger.info(f"Batch {i // BATCH_SIZE + 1} procesado")
            self.logger.info(
                f"Upsert completado — insertados: {total_inserted}, actualizados: {total_modified}"
            )


if __name__ == "__main__":
    ingester = FestivosIngester()
    ingester.run()
