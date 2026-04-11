"""Tests unitarios para la lógica de transformación de OpenDataBcnIngester."""

import polars as pl
import pytest


def apply_electricity_transform(df: pl.DataFrame) -> pl.DataFrame:
    """Replica la lógica del transform() de OpenDataBcnIngester, sin MongoDB."""
    no_consta = df.filter(pl.col("Tram_Horari") == "No consta").shape[0]
    df = df.filter(pl.col("Tram_Horari") != "No consta")

    df = (
        df.with_columns(
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
        .drop("Any", "Data", "Tram_Horari", "Valor")
        .select("Datetime", "Codi_Postal", "Sector_Economic", "MWh")
    )
    return df


# Fixture
@pytest.fixture
def sample_df() -> pl.DataFrame:
    """DataFrame con datos simulados que imitan el CSV raw de Open Data BCN."""
    return pl.DataFrame(
        {
            "Any": [2025, 2025, 2025],
            "Data": ["2025-01-01", "2025-01-01", "2025-01-01"],
            "Codi_Postal": [8001, 8002, 8003],
            "Sector_Economic": ["Indústria", "Residencial", "Serveis"],
            "Tram_Horari": [
                "De 00:00:00 a 05:59:59 h",
                "De 06:00:00 a 11:59:59 h",
                "No consta",  # este debe filtrarse
            ],
            "Valor": [287, 150, 999],
        }
    )


# Tests
def test_output_columns(sample_df):
    """El transform debe producir exactamente estas 4 columnas."""
    result = apply_electricity_transform(sample_df)
    assert result.columns == ["Datetime", "Codi_Postal", "Sector_Economic", "MWh"]


def test_output_dtypes(sample_df):
    """Las columnas deben tener los tipos correctos."""
    result = apply_electricity_transform(sample_df)
    assert result["Datetime"].dtype == pl.Datetime
    assert result["Codi_Postal"].dtype == pl.String
    assert result["MWh"].dtype == pl.Int64


def test_no_consta_filtered(sample_df):
    """Los registros con 'No consta' en Tram_Horari deben eliminarse."""
    result = apply_electricity_transform(sample_df)
    assert len(result) == 2  # 3 filas - 1 "No consta" = 2


def test_cod_postal_zfill(sample_df):
    """Los códigos postales deben tener siempre 5 dígitos con ceros a la izquierda."""
    result = apply_electricity_transform(sample_df)
    assert result["Codi_Postal"][0] == "08001"
    assert all(len(cp) == 5 for cp in result["Codi_Postal"].to_list())


def test_datetime_parsed(sample_df):
    """El Datetime debe parsearse correctamente desde Data + Tram_Horari."""
    result = apply_electricity_transform(sample_df)
    # 2025-01-01 + De 00:00:00 → 2025-01-01 00:00:00
    import datetime
    assert result["Datetime"][0].replace(tzinfo=None) == datetime.datetime(2025, 1, 1, 0, 0, 0)


def test_row_count_after_filter(sample_df):
    """El transform debe eliminar solo los 'No consta', sin duplicar filas."""
    result = apply_electricity_transform(sample_df)
    assert len(result) == 2