"""Pipeline ETL para consumo eléctrico de Barcelona."""
import logging
import sys  # Para que mpirma en la consola


# Cadena logger -> handler -> formatter
def get_logger(name: str) -> logging.Logger:
    """Funcion para recivir nombre y devolver un objeto logger
    configurado. -> es solo el type hint del tipo de objeto que devuelve"""
    logger = logging.getLogger(name)  # Crea o recupera un logger con el nombre dado
    logger.setLevel(
        logging.INFO
    )  # Establece el nivel de logueo a INFO, lo que significa que se registrarán mensajes de nivel INFO y superiores (WARNING, ERROR, CRITICAL)

    # Handler define a donde van los mensajes
    if (
        not logger.hasHandlers()
    ):  # Verifica si el logger ya tiene handlers configurados para evitar agregar múltiples handlers al mismo logger
        handler = logging.StreamHandler(
            sys.stdout
        )  # Crea un handler que envía los mensajes de logueo a la salida estándar (consola)
        formatter = logging.Formatter(  # Define el formato de los mensajes de logueo, incluyendo la fecha y hora, el nombre del logger, el nivel de logueo y el mensaje en sí
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
