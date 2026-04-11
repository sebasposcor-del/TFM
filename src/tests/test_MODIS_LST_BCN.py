# Testeas que tu código transforma correctamente los datos
# 1. El transform produce las columnas correctas
# 2. El transform produce los tipos de datos correctos
# 3. El transform produce los valores correctos (si es posible)

import polars as pl
import pytest

# Constantes del ingester MODIS LST
MODIS_SCALE_FACTOR = 0.02
KELVIN_OFFSET = 273.15


def apply_modis_transform(df: pl.DataFrame) -> pl.DataFrame:
    """Replica exacta del transform() de MODISIngester, sin MongoDB."""
    df = (
        df.with_columns(
            [
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
    return df


# Fixture — datos de prueba reutilizables en todos los tests
@pytest.fixture
def sample_df() -> pl.DataFrame:
    """DataFrame con datos simulados que imitan el CSV de MODIS."""
    return pl.DataFrame(
        {
            "COD_POSTAL": [8001, 8002, 8003],
            "fecha": ["2019-01-01", "2019-01-01", "2019-07-15"],
            # 14286 → 14286 * 0.02 = 285.72 K → 12.57°C (enero Barcelona ✅)
            # 14350 → 14350 * 0.02 = 287.00 K → 13.85°C
            # 15000 → 15000 * 0.02 = 300.00 K → 26.85°C (julio Barcelona ✅)
            "mean": [14286, 14350, 15000],
        }
    )


# Tests
def test_output_columns(sample_df):
    """El transform debe producir exactamente estas 3 columnas."""
    result = apply_modis_transform(sample_df)
    assert result.columns == ["cod_postal", "fecha", "lst_celsius"]


def test_scale_factor(sample_df):
    """Verifica que la conversión Kelvin a Celsius es correcta para un valor conocido.
    14286 * 0.02 - 273.15 = 12.57°C
    """
    result = apply_modis_transform(sample_df)
    assert result["lst_celsius"][0] == pytest.approx(12.57, abs=0.01)


def test_output_dtypes(sample_df):
    """Las columnas deben tener los tipos correctos."""
    result = apply_modis_transform(sample_df)
    assert result["lst_celsius"].dtype == pl.Float64
    assert result["cod_postal"].dtype == pl.Utf8
    assert result["fecha"].dtype == pl.Datetime


def test_cod_postal_zfill(sample_df):
    """Los códigos postales deben tener siempre 5 dígitos con ceros a la izquierda.
    8001 a '08001'
    """
    result = apply_modis_transform(sample_df)
    assert result["cod_postal"][0] == "08001"
    assert all(len(cp) == 5 for cp in result["cod_postal"].to_list())


def test_row_count_preserved(sample_df):
    """El transform no debe eliminar ni duplicar filas."""
    result = apply_modis_transform(sample_df)
    assert len(result) == len(sample_df)
