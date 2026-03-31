"""Pipeline ETL para consumo eléctrico de Barcelona."""

from abc import ABC, abstractmethod

import polars as pl
from pymongo import MongoClient

from utils.logger import get_logger

# TYPE HINT
# def transform(self, df: pl.DataFrame) -> pl.DataFrame:
#                            ↑ entra            ↑ sale


class BaseETL(ABC):
    """Clase base para procesos ETL,
    define la estructura y métodos comunes para los ingesters"""

    def __init__(self, mongo_uri: str = "mongodb://mongo:27017/", db_name: str = "tfm_energy"):
        """Constructor de clase, es el metodo qeu se ejecuta automáticamente
        cuando se llama una clase que hereda este ETL base
           - self es la referencia al propio objeto"""
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.logger = get_logger(
            self.__class__.__name__
        )  # Crea un logger específico para cada clase que herede de BaseETL,
        # usando el nombre de la clase como nombre del logger

    @abstractmethod  # decoder que obliga a clases hijas a implementarlo
    def extract(self) -> pl.DataFrame:
        """Método que descarga data de fuente,devuelve un dataFrame raw,"""

    @abstractmethod
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Método que limpia y transforma el dataFrame raw, devuelve un dataFrame limpio"""

    @abstractmethod
    def load_raw(self, df: pl.DataFrame) -> None:
        """Método que carga el dataFrame raw a la base de datos"""

    @abstractmethod
    def load_clean(self, df: pl.DataFrame) -> None:
        """
        Método que carga el dataFrame limpio a la base de datos
        """

    def run(self) -> None:
        """Orquesta el pipeline ETL, llamando a los métodos en orden:
        extract, transform, load_raw, load_clean"""
        self.logger.info("Iniciando  ETL...")

        raw_pl = self.extract()
        self.load_raw(raw_pl)

        clean_pl = self.transform(raw_pl)
        self.load_clean(clean_pl)

        self.client.close()
        self.logger.info("ETL finalizado.")
