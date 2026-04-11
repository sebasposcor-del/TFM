"""Tests unitarios para la lógica de transformación de MeteocatIngester."""

import polars as pl
import pytest


def apply_meteocat_transform(df: pl.DataFrame) -> pl.DataFrame:
    """Replica la lógica del transform() de MeteocatIngester, sin MongoDB."""
    df = (
        df.with_columns(
            [
                pl.col("data_lectura").str.to_datetime("%Y-%m-%dT%H:%M:%S%.f"),
                pl.col("valor_lectura").cast(pl.Float64),
                pl.col("codi_variable").cast(pl.Int32),
            ]
        )
        .filter(pl.col("codi_estat") == "V")
        .drop(["id", "codi_base", "codi_estat"])
    )
    return df


# Fixture
@pytest.fixture
def sample_df() -> pl.DataFrame:
    """DataFrame con datos simulados que imitan el raw de Meteocat."""
    return pl.DataFrame(
        {
            "id": ["D5320101190000", "D5320101190001", "D5320101190002"],
            "codi_estacio": ["D5", "D5", "X2"],
            "codi_variable": ["32", "32", "33"],
            "data_lectura": [
                "2019-01-01T00:00:00.000",
                "2019-01-01T00:30:00.000",
                "2019-01-01T00:00:00.000",
            ],
            "valor_lectura": ["14.1", "13.8", "72.5"],
            "codi_estat": ["V", "V", "N"],  # N debe filtrarse
            "codi_base": ["SH", "SH", "SH"],
        }
    )


# Tests
def test_output_columns(sample_df):
    """El transform debe eliminar id, codi_base y codi_estat."""
    result = apply_meteocat_transform(sample_df)
    assert "id" not in result.columns
    assert "codi_base" not in result.columns
    assert "codi_estat" not in result.columns


def test_output_dtypes(sample_df):
    """Las columnas deben tener los tipos correctos."""
    result = apply_meteocat_transform(sample_df)
    assert result["data_lectura"].dtype == pl.Datetime
    assert result["valor_lectura"].dtype == pl.Float64
    assert result["codi_variable"].dtype == pl.Int32


def test_invalid_estat_filtered(sample_df):
    """Solo deben mantenerse registros con codi_estat == 'V'."""
    result = apply_meteocat_transform(sample_df)
    assert len(result) == 2  # 3 filas - 1 con codi_estat='N' = 2


def test_valor_lectura_parsed(sample_df):
    """valor_lectura debe convertirse de string a Float64 correctamente."""
    result = apply_meteocat_transform(sample_df)
    assert result["valor_lectura"][0] == pytest.approx(14.1, abs=0.01)


def test_row_count_after_filter(sample_df):
    """El transform debe eliminar solo los no válidos, sin duplicar filas."""
    result = apply_meteocat_transform(sample_df)
    assert len(result) == 2
