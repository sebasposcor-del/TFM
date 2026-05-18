---

## EDA, Sistema de Predicción Energética y Alerta Temprana en Barrios de Barcelona

> **Contexto:** Dataset con consumo eléctrico por código postal (bloques de 6h, 2019–2025), enriquecido con variables meteorológicas de estaciones Meteocat y temperatura superficial satelital (MODIS LST). El objetivo es **predecir el consumo eléctrico a nivel de barrio con horizonte 24–72h** y generar alertas tempranas de picos de demanda.

### Variables del dataset

| Variable | Tipo | Descripción |
|---|---|---|
| `cod_postal` | Categórica | Código postal de Barcelona (08001–08042) |
| `nombre_postal` | Categórica | Nombre del barrio asociado al código postal |
| `centroide_lon` | Numérica | Longitud del centroide del código postal |
| `centroide_lat` | Numérica | Latitud del centroide del código postal |
| `codi_estacio` | Categórica | Código de estación Meteocat asignada |
| `nombre_estacio` | Categórica | Nombre de la estación Meteocat asignada |
| `estacio_lon` | Numérica | Longitud de la estación Meteocat asignada |
| `estacio_lat` | Numérica | Latitud de la estación Meteocat asignada |
| `datetime` | Temporal | Inicio del bloque de 6 horas |
| `mwh_total` | Target | Consumo eléctrico total en MWh |
| `mwh_industria` | Numérica | Consumo sector industria |
| `mwh_residencial` | Numérica | Consumo sector residencial |
| `mwh_servicios` | Numérica | Consumo sector servicios |
| `mwh_no_especificado` | Numérica | Consumo no clasificado |
| `lst_celsius` | Numérica | Land Surface Temperature — MODIS (°C) |
| `temp_mean` | Numérica | Temperatura media Meteocat (°C) |
| `temp_max` | Numérica | Temperatura máxima Meteocat (°C) |
| `temp_min` | Numérica | Temperatura mínima Meteocat (°C) |
| `humedad_mean` | Numérica | Humedad relativa media (%) |
| `viento_mean` | Numérica | Velocidad media del viento (m/s) |
| `precipitacion_sum` | Numérica | Precipitación acumulada en el bloque (mm) |
| `irradiancia_mean` | Numérica | Irradiancia solar global media (W/m²) |
| `es_festivo` | Binaria | Es día festivo |
| `nombre_local` | Categórica | Nombre del festivo local |
| `hora` | Ordinal | Hora de inicio del bloque (0, 6, 12, 18) |
| `dia_semana` | Ordinal | Día de la semana (0=Lunes, 6=Domingo) |
| `mes` | Ordinal | Mes del año (1–12) |
| `anio` | Numérica | Año |
| `semana_anio` | Numérica | Semana del año (1–53) |
| `es_finde` | Binaria | Es fin de semana |

---
## <font color='#4E8F6E'>  **Librerías** </font>



```python
import polars as pl
import polars.selectors as cs
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import seaborn as sns
import numpy as np
import scipy.stats as stats
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from pymongo import MongoClient
from statsmodels.tsa.seasonal import STL
from datetime import date



plt.rcParams['axes.grid'] = True
plt.rcParams['grid.color'] = '#D3D3D3'
plt.rcParams['grid.linewidth'] = 0.4
plt.rcParams['axes.spines.top'] = False
plt.rcParams['axes.spines.right'] = False
plt.rcParams['axes.xmargin'] = 0.01
plt.rcParams['axes.ymargin'] = 0.01

verde   = '#4E8F6E'
naranja  = '#E07B5A'
amarillo = '#D4B93A'
azul    = '#2B6CB0'
```

---
# <font color='#4E8F6E'>  **Carga y Exploración** </font>



```python
client = MongoClient('mongodb://mongo:27017/')
db     = client['tfm_energy']

docs = list(db['dataset_eda'].find({}, {'_id': 0}))
df   = pl.DataFrame(docs, infer_schema_length=None)

```


```python
df.head(6)
```


```python
df.tail(6)
```


```python
df.filter(pl.col("codi_estacio") == "X2").select(pl.col(["temp_mean", "temp_max", "temp_min", "humedad_mean", "viento_mean", "precipitacion_sum"])).head(6)
```


```python
print(f"Shape: {df.shape}")
print(f"Desde: {df['datetime'].min()}")
print(f"Hasta: {df['datetime'].max()}")
print(f"Códigos postales únicos: {df['cod_postal'].n_unique()}")
print(f"Años cubiertos: {sorted(df['anio'].unique().to_list())}")

```


```python
# Tipos de datos
print("SCHEMA")
for col, dtype in zip(df.columns, df.dtypes):
    print(f"  {col:<30} {dtype}")

```


```python
pl.Config.set_tbl_rows(50)

nulls = (
    pl.DataFrame({
        'columna': df.columns,
        'nulos':   [df[c].null_count() for c in df.columns]
    })
    .with_columns((pl.col('nulos') / len(df) * 100).round(2).alias('pct_nulos'))
    .sort('nulos', descending=True)
    .filter(pl.col('nulos') > 0)
)

print(nulls)
```

# Distribución por código postal


```python
codpos_null = (
df.group_by('anio').agg([
    pl.col('irradiancia_mean').is_null().sum().alias('nulos_irradiancia'),
    pl.col('viento_mean').is_null().sum().alias('nulos_viento'),
    pl.col('temp_mean').is_null().sum().alias('nulos_temp'),
    pl.col('humedad_mean').is_null().sum().alias('nulos_humedad'),
    pl.len().alias('total_registros')
]).sort('anio'))

print(codpos_null)
```

> Los nulls de viento e irradiancia son constantes por todos los años, puede ser que sean dias específicos del años o simepleemente que solo algunas estacionas puedan lograr tomar estas medidas. aproximadamente 17,500 nullos al año

> En cambio partes de 2024 y 2025 son valores un poco diferentes, que curiosamente se acercan a las constantes de nulos de irradiancia y viento

Nulls viento


```python
codpos_null = (df.filter(pl.col('viento_mean').is_null())
    .group_by(['cod_postal','nombre_postal','codi_estacio'])
    .agg(pl.len().alias('nulos_viento'))
    .sort('nulos_viento', descending=True)
    .select(['cod_postal','nombre_postal', 'nulos_viento','codi_estacio']))

print(codpos_null)
```

> Los nulos de irradiancia y viento son estructurales desde 2019: la estación X2
> de Meteocat no dispone de estos sensores para la mayoría de códigos postales.
> No es un fallo temporal sino una limitación permanente de cobertura.

> La excepción es el código postal 08038, que tiene asignada otra estación
> con cobertura completa de variables, confirmando que el problema es de
> estación y no de los días o periodos de medición.

> En 2024 y 2025 los nulos de temperatura y humedad escalan bruscamente hasta
> igualarse con los de irradiancia y viento. Esto confirma la degradación
> progresiva y posterior inactividad de la estación X2, que en 2025 deja de
> reportar prácticamente todas las variables.

> Estrategia de imputación pendiente para feature engineering: reasignar X2
> a X4 o aplicar un factor de corrección histórico. MODIS actuará como
> fallback espacial para LST en periodos sin cobertura de estación.


```python
df.group_by(["codi_estacio", "anio"]).agg([
    pl.len().alias("total_registros"),
    pl.col("temp_mean").is_null().sum().alias("nulos_temp"),
    pl.col("viento_mean").is_null().sum().alias("nulos_viento"),
    pl.col("irradiancia_mean").is_null().sum().alias("nulos_irradiancia"),
]).sort(["codi_estacio", "anio"])
```

#### Estaciones con mejor cobertura


```python
df.group_by("codi_estacio").agg([
    pl.len().alias("total"),
    pl.col("temp_mean").is_null().sum().alias("nulos_temp"),
    (pl.col("temp_mean").is_null().sum() / pl.len() * 100).round(2).alias("pct_nulos_temp")
]).sort("pct_nulos_temp")
```

- X4 : 0.57% nulos 
- X8 : 0.62% nulos 
- D5 : 0.80% nulos  
- X2 : 17.23% nulos 
- AN : 100% nulos

> **Estación AN — Parc de la Ciutadella:** Al cruzar el mapeo geográfico con los datos reales de MongoDB, se descubrió que AN y X2 (Zoo de Barcelona) se solapan físicamente, a menos de 200 metros dentro del mismo parque. Más importante aún, AN tiene el 100% de nulos en todas sus variables meteorológicas en todos los años (2019–2025), lo que confirma que AN existe como punto de referencia geográfico en Meteocat pero no dispone de sensores activos. Por esta razón, tomo la decisión de reasignar todos los registros originalmente asignados a AN hacia X2, que sí tiene datos de temperatura.

> **Estación X2 — Zoo de Barcelona:** X2 presenta dos limitaciones estructurales. Primero, nunca ha tenido sensores de viento ni irradiancia ya que ambas variables tienen el 100% de nulos en todos los años, lo cual no es un fallo sino una característica de la estación. Segundo, a partir de 2024 comienza a degradarse y en 2025 deja de funcionar completamente, con 2.664 nulos de temperatura sobre 2.664 registros totales. Para los CPs asignados a X2, las variables de viento e irradiancia deberán ser imputadas en la fase de feature engineering, ya sea mediante la estación más cercana con cobertura completa o mediante un factor de corrección basado en la diferencia de temperatura superficial con MODIS.

---
# <font color='#4E8F6E'>  **Limpieza inicial** </font>


> A partir de los hallazgos anteriores comenzaré con una limpieza inicial

### <font color='#D4B93A'><b>Datos Faltantes</b></font>

#### Datos sin registros en mwh_total


```python
mwh_problematicos = df.filter(pl.col('mwh_total') <= 0)
print(f"Registros con mwh_total mayor o igual a 0: {len(mwh_problematicos):,}")

n_dupes = len(df) - df.unique(subset=['datetime', 'cod_postal']).shape[0]
print(f"Duplicados datetime + cod_postal: {n_dupes:,}")

```

> Existen 378 filas donde no existen valores iguales o menores a 0 en el mwh_total lo cual se puede tratar de un problema de recolección de datos


```python
print(mwh_problematicos.select([
    "cod_postal", "nombre_postal", "datetime",
    "mwh_total", "mwh_industria", "mwh_residencial",
    "mwh_servicios", "mwh_no_especificado"
]))
```


```python
print(mwh_problematicos.select("datetime").unique().sort("datetime"))
```


```python
fechas_problema = ["2025-06-26", "2025-08-30", "2025-09-07"]

df.filter(
    (pl.col("datetime").dt.date().cast(pl.Utf8).is_in(fechas_problema)) &
    (pl.col("datetime").dt.hour() == 18)
).select(["cod_postal", "datetime", "mwh_total"]).head(10)
```


```python
mwh_problematicos.group_by("datetime") \
    .agg(pl.n_unique("cod_postal").alias("n_codigos")) \
    .sort("datetime")
```

> Este error en lso datos existe durante todos lo días para los 42 códigos postales

> Se detectaron 378 registros con mwh_total = 0, concentrados en exactamente 
> 3 fechas de 2025 (26-jun, 30-ago, 07-sep). En cada caso, los bloques 00:00, 
> 06:00 y 12:00 de los 42 códigos postales reportaron cero simultáneamente, 
> mientras el bloque 18:00 del mismo día registró valores normales. 
> Patrón consistente con un fallo parcial de reporte en la fuente, no con 
> un corte real de suministro.

### <font color='#D4B93A'><b>Verificar continuidad temporal</b></font>


```python
# Fechas completas esperadas cada 6 horas para cada código postal
fechas_completas = pl.datetime_range( 
    start=df['datetime'].min(),
    end=df['datetime'].max(),
    interval='6h',
    eager=True
).alias('datetime')

codigos_postales = df.select('cod_postal').unique()

# el corss join es para crear la combinacion completa de fechas para cada código postal
fechas_esperadas = codigos_postales.join(
    fechas_completas.to_frame(), how='cross'
)

print(f"Registros esperados: {len(fechas_esperadas):,}")
print(f"Registros reales:    {len(df):,}")
print(f"Registros faltantes: {len(fechas_esperadas) - len(df):,}")

#El antijoin para ver que valores de datetime + cod_postal no están en el dataset original
faltantes = fechas_esperadas.join(
    df.select(['cod_postal', 'datetime']),
    on=['cod_postal', 'datetime'],
    how='anti'
)

print("\nFechas con mayor número de CP faltantes:")
print(
    faltantes
    .group_by(pl.col('datetime').dt.date().alias('fecha'))
    .agg(pl.len().alias('cp_faltantes'))
    .sort('cp_faltantes', descending=True)
    .head(20)
)
```


```python
faltantes_por_dia = faltantes.with_columns(
    pl.col("datetime").dt.date().alias("fecha")
).group_by(["cod_postal", "fecha"]).count()

faltantes_por_dia.filter(
    pl.col("count") != 4  # 4 registros por día (cada 6h)
)
```

> A todos estos días les faltan 4 los 4 horarios

08011 (7–20 ago 2025): 52 bloques faltantes (13 días × 4). Problema de reporte específico del CP.
2025-08-19: día completo sin datos para los 42 CPs (168 bloques). Fallo de reporte en la fuente.

### <font color='#D4B93A'><b>Conversión de tipo de datos</b></font>


```python
df = df.with_columns([
    pl.col('datetime').cast(pl.Datetime),
    pl.col('cod_postal').cast(pl.Utf8),
    pl.col('es_festivo').cast(pl.Int8),
    pl.col('es_finde').cast(pl.Int8),
    pl.col('hora').cast(pl.Int8),
    pl.col('dia_semana').cast(pl.Int8),
    pl.col('mes').cast(pl.Int8),
    pl.col('anio').cast(pl.Int16),
])
df.head(3)

```

> Se identificaron 378 registros con mwh_total = 0, correspondientes a los bloques de 00:00, 06:00 y 12:00 de tres fechas puntuales en 2025, estos son: 26 de junio, 30 de agosto y 7 de septiembre. 

> En los tres casos el patrón es idéntico: afectan a los 42 códigos postales simultáneamente y el bloque de 18:00 del mismo día reporta valores normales (por ejemplo, 08001 registró 9.128 MWh a las 18:00 del 26 de junio). 

> Esto descarta un apagón real y confirma un fallo parcial de reporte en la fuente Open Data BCN, limitado a los primeros tres bloques del día. Estos registros serán tratados como nulos en la fase de feature engineering.

### <font color='#D4B93A'><b>Trato de Nulls</b></font>

1. **Reasignación AN → X2** — variables meteorológicas rellenadas con datos reales de X2 desde MongoDB  
2. **Grilla temporal completa** — bloques faltantes (08011 ago 2025 + 19-ago global) rellenados con mediana histórica  
3. **Sectores de consumo** — `mwh_industria`, `mwh_residencial` y `mwh_servicios` imputados a 0 (sector no activo ese bloque); se recalcula `mwh_total`  
4. **Días con mwh_total = 0** — 3 fechas de fallo de reporte imputadas con mediana histórica 2019-2024  
5. **Nulls meteorológicos restantes** — `viento`, `irradiancia`, `temp`, `humedad`: estructurales, se mantienen para feature engineering


```python
# ── 1. Reasignación AN → X2 ──────────────────────────────────────────────────
# AN y X2 se solapan físicamente (Zoo de Barcelona). AN tiene 100% nulos.
# Se renombra AN → X2 y se rellenan variables meteo con datos reales de X2.

df = df.with_columns([
    pl.when(pl.col("codi_estacio") == "AN").then(pl.lit("X2")).otherwise(pl.col("codi_estacio")).alias("codi_estacio"),
    pl.when(pl.col("codi_estacio") == "AN").then(pl.lit("Barcelona - Zoo")).otherwise(pl.col("nombre_estacio")).alias("nombre_estacio"),
    pl.when(pl.col("codi_estacio") == "AN").then(pl.lit(2.1898)).otherwise(pl.col("estacio_lon")).alias("estacio_lon"),
    pl.when(pl.col("codi_estacio") == "AN").then(pl.lit(41.3842)).otherwise(pl.col("estacio_lat")).alias("estacio_lat"),
])

docs_meteocat = list(db["clean_meteocat"].find({"codi_estacio": "X2"}, {"_id": 0}))
meteocat_x2 = (
    pl.DataFrame(docs_meteocat)
    .rename({"data_lectura": "datetime", "temp": "temp_mean", "humedad": "humedad_mean",
             "viento": "viento_mean", "precipitacion": "precipitacion_sum", "irradiancia": "irradiancia_mean"})
    .select(["datetime", "temp_mean", "humedad_mean", "viento_mean", "precipitacion_sum", "irradiancia_mean"])
)

df = df.join(meteocat_x2, on="datetime", how="left", suffix="_x2")

cols_meteo = ["temp_mean", "humedad_mean", "viento_mean", "precipitacion_sum", "irradiancia_mean"]
df = df.with_columns([
    pl.when(pl.col(c).is_null()).then(pl.col(f"{c}_x2")).otherwise(pl.col(c)).alias(c)
    for c in cols_meteo
]).drop([f"{c}_x2" for c in cols_meteo])

print("Reasignación AN → X2 completada")
print(df.group_by("codi_estacio").agg(pl.col("temp_mean").null_count().alias("nulos_temp"), pl.len().alias("total")).sort("codi_estacio"))
```

### Estrategia de nulos meteorológicos, decisión EDA

**Variables: viento_mean, irradiancia_mean, precipitacion_sum**  
X2 no mide estas variables en ningún período.
- Mantener nulos. Si el EDA muestra diferencias significativas entre barrios, explorar fuentes alternativas (otras estaciones, ERA5).

**Variables: temp_mean, humedad_mean, período 2024 parcial y 2025**  
X2 inactiva. Datos reales disponibles para 2019–2023.
- Mantener nulos por ahora. Si el EDA confirma diferencias de comportamiento entre barrios (consumo vs temperatura), aplicar factor de corrección basado en la relación histórica X2/X4 de años anteriores.

**Criterio de decisión (post-EDA):**
- Si variación entre barrios es baja: nulos sin imputar, el modelo los gestiona
- Si variación entre barrios es alta: factor de corrección histórico X2/X4


```python
# ── 2. Grilla temporal completa ───────────────────────────────────────────────
# 08011 tiene 52 bloques faltantes en ago 2025 y el 19-ago-2025 falta para todos los CPs.
# Se imputan con mediana histórica por CP + hora + dia_semana + mes (2019-2024).

fechas_esperadas = df.select('cod_postal').unique().join(
    pl.datetime_range(df['datetime'].min(), df['datetime'].max(), interval='6h', eager=True)
      .alias('datetime').to_frame(), how='cross'
)
faltantes = fechas_esperadas.join(df.select(['cod_postal', 'datetime']), on=['cod_postal', 'datetime'], how='anti')
print(f"Faltantes detectados: {len(faltantes):,}")
print(faltantes.filter(pl.col('datetime').dt.date() != date(2025, 8, 19))
               .group_by('cod_postal').agg(pl.len().alias('n')).sort('n', descending=True))

docs_festivos = list(db["clean_festivos"].find({}, {"_id": 0}))
fechas_festivas = set(
    pl.DataFrame(docs_festivos)
    .with_columns(pl.col("fecha").str.to_date("%Y-%m-%d"))["fecha"].to_list()
)

referencia = (
    df.filter(pl.col("anio") < 2025)
    .group_by(["cod_postal", "hora", "dia_semana", "mes"])
    .agg(pl.col("mwh_total").median().alias("mwh_total"))
)

df_faltantes = (
    faltantes
    .with_columns([
        pl.col("datetime").dt.hour().cast(pl.Int8).alias("hora"),
        pl.col("datetime").dt.weekday().cast(pl.Int8).alias("dia_semana"),
        pl.col("datetime").dt.month().cast(pl.Int8).alias("mes"),
    ])
    .join(referencia, on=["cod_postal", "hora", "dia_semana", "mes"], how="left")
    .with_columns(
        pl.col("datetime").dt.date()
          .map_elements(lambda d: 1 if d in fechas_festivas else 0, return_dtype=pl.Int64)
          .alias("es_festivo")
    )
)

df = pl.concat([df, df_faltantes], how="diagonal_relaxed").sort(["cod_postal", "hora", "datetime"])
print(f"Shape: {df.shape} | Nulls mwh_total: {df['mwh_total'].null_count()}")
```

> **08011 (7–20 ago 2025):** 52 bloques faltantes (13 días × 4). Problema de reporte específico del CP.

> **2025-08-19:** día completo sin datos para los 42 CPs (168 bloques). Fallo de reporte en la fuente.

> Ambos se imputan con mediana histórica por CP + hora + día semana + mes, usando 2019-2024.


```python
# ── 3. Fill null sectores + recálculo mwh_total ───────────────────────────────
# Sectores sin dato = 0 (sector no activo ese bloque horario)

df = df.with_columns([
    pl.col("mwh_industria").fill_null(0),
    pl.col("mwh_residencial").fill_null(0),
    pl.col("mwh_servicios").fill_null(0),
])

df = df.with_columns(
    (pl.col("mwh_industria") + pl.col("mwh_residencial") +
     pl.col("mwh_servicios") + pl.col("mwh_no_especificado").fill_null(0)
    ).alias("mwh_total")
)

# Verificar nulos restantes
nulos = (
    df.null_count()
    .unpivot(variable_name="columna", value_name="nulos")
    .filter(pl.col("nulos") > 0)
    .with_columns((pl.col("nulos") / len(df) * 100).round(2).alias("pct"))
    .sort("nulos", descending=True)
)
print(f"Shape: {df.shape}")
print(nulos)
```


```python
# ── 4. Imputar mwh_total = 0 (3 fechas de fallo de reporte en 2025) ───────────
# 26-jun, 30-ago y 07-sep 2025: mwh_total = 0 para los 42 CPs por error en OpenData.
# Se imputa con mediana histórica por CP + hora + dia_semana + mes (2019-2024).

referencia_ceros = (
    df.filter((pl.col("anio") < 2025) & (pl.col("mwh_total") > 0))
    .group_by(["cod_postal", "hora", "dia_semana", "mes"])
    .agg(pl.col("mwh_total").median().alias("mwh_ref"))
)

df = (
    df.join(referencia_ceros, on=["cod_postal", "hora", "dia_semana", "mes"], how="left")
    .with_columns(
        pl.when(pl.col("mwh_total") == 0)
          .then(pl.col("mwh_ref"))
          .otherwise(pl.col("mwh_total"))
          .alias("mwh_total")
    )
    .drop("mwh_ref")
)

print(f"Ceros restantes: {df.filter(pl.col('mwh_total') == 0).shape[0]}")
print(f"Nulls mwh_total: {df['mwh_total'].null_count()}")
```

> **Resumen del tratamiento de nulos:**
>
> - **Reasignación AN → X2** — los registros de la estación AN (100% nulos, Zoo de Barcelona) se reasignaron a X2 y se rellenaron con datos reales de Meteocat para temp y humedad (2019–2023).
>
> - **Sectores de consumo** — `mwh_industria`, `mwh_residencial` y `mwh_servicios` nulos imputados a 0 (sector no activo ese bloque). `mwh_total` recalculado como suma de los 4 sectores.
>
> - **Continuidad temporal** — 220 bloques añadidos: 52 del CP 08011 (7–20 ago 2025, fallo de reporte específico) y 168 del 19-ago-2025 global (42 CPs × 4 bloques). Imputados con mediana histórica por CP + hora + día semana + mes (2019–2024).
>
> - **mwh_total = 0** — 378 registros en 3 fechas de 2025 (26-jun, 30-ago, 07-sep). Fallo parcial de reporte en OpenData — bloques 00:00, 06:00 y 12:00 en cero mientras el 18:00 era normal. Imputados con mediana histórica por CP + hora + día semana + mes (2019–2024).
>
> - **Nulos meteorológicos restantes** — viento e irradiancia (X2 sin sensores en todo el período), temp/humedad 2024–2025 (X2 inactiva). Estructurales y conocidos, se mantienen para feature engineering.

Limitación conocida: los 378 registros con mwh_total = 0 imputados con mediana histórica
presentan inconsistencia con sus sectores componentes (mwh_industria, mwh_residencial,
mwh_servicios permanecen en 0). Al representar el 0.09% del dataset y no usarse los
sectores como features del modelo, el impacto en el forecasting es despreciable.
Se documenta como deuda técnica para versiones futuras.

---
# <font color='#4E8F6E'>  **Análisis Descriptivo** </font>


## <font color='#4E8F6E'>  **Variable Objetivo: mwh_total** </font>



```python
df.select('mwh_total').describe()
```

- Media (101,467 MWh) > mediana (90,608 MWh), diferencia de ~11k, sesgo positivo confirmado,
  la cola derecha de outliers extremos jala la media hacia arriba.
- IQR: 57,120 a 132,708 MWh, rango de consumo "normal" entre barrios y bloques horarios.
- Mínimo 130 MWh, registros de madrugada en barrios pequeños, valor plausible.
- Máximo 1,486,114 MWh, outlier extremo a revisar
- Std (59,563) representa el 59% de la media, alta dispersión esperada dado que mezcla 42 códigos postales con perfiles muy distintos (industrial, residencial, turístico).


```python
vals_target = df['mwh_total'].drop_nulls().to_numpy()

fig, axes = plt.subplots(1, 2, figsize=(14, 4))

axes[0].hist(vals_target, bins=80, color='#4E8F6E', edgecolor='white', alpha=0.85)
axes[0].set_title('Distribución de mwh_total', fontweight='bold')
axes[0].set_xlabel('MWh')
axes[0].set_ylabel('Frecuencia')

axes[1].boxplot(vals_target, vert=False, patch_artist=True,
                boxprops=dict(facecolor='#4E8F6E', alpha=0.7),
                medianprops=dict(color='navy', linewidth=2),
                flierprops=dict(marker='o', markersize=2, alpha=0.3, color='#D4B93A'))
axes[1].set_title('Boxplot de mwh_total', fontweight='bold')
axes[1].set_xlabel('MWh')

plt.tight_layout()
plt.show()
```


```python
print(f"sum(is.na(mwh_total)): {df['mwh_total'].null_count()}")

```

> **Observaciones:**
> La distribución de mwh_total presenta sesgo positivo con cola derecha pronunciada. 

- La mediana está muy por debajo de la media, indicando que la mayoría de bloques tienen consumo moderado pero existen picos elevados de alta magnitud. 

- Estos outliers identificados son de 1.48 M lo cual muestra uqe estos no pueden ser datos reales.

- La desviación estándar de 59 k muestra la alta dispersión , los valores están muy dispersos.

> No se sigue una distribución normal así que se usará Spearman para correlaciones y Kruskal-Wallis / Mann-Whitney para tests de hipótesis.


#### Outliers

Detección con umbral de 5× la mediana por CP. Se usa la mediana porque no se infla por los propios outliers que intenta detectar.


```python
# ── Detección: umbral 5× mediana + contexto de casos ─────────────────────────
umbrales = df.group_by("cod_postal").agg(pl.col("mwh_total").median().alias("mediana"))

print(df.join(umbrales, on="cod_postal")
    .filter(pl.col("mwh_total") > pl.col("mediana") * 5)
    .group_by("cod_postal")
    .agg(pl.len().alias("n_registros"))
    .sort("n_registros", descending=True))

# Contexto de los 3 casos erróneos del umbral mediana × 5
casos = {
    "08030 — Sostenido (may 2023)": df.filter(
        (pl.col("cod_postal") == "08030") &
        (pl.col("datetime") >= pl.datetime(2023, 5, 5)) &
        (pl.col("datetime") < pl.datetime(2023, 5, 12))
    ).select(["datetime", "mwh_total", "mwh_industria"]).sort("datetime"),
    "08037 — Puntual (nov/dic 2024, feb 2025)": df.filter(
        (pl.col("cod_postal") == "08037") &
        ((pl.col("datetime") == pl.datetime(2024, 11, 20, 0)) |
         (pl.col("datetime") == pl.datetime(2024, 12, 22, 0)) |
         (pl.col("datetime") == pl.datetime(2025, 2, 24, 0)))
    ).select(["datetime", "mwh_total", "mwh_servicios"]),
    "08022 — Parcial (may 2023)": df.filter(
        (pl.col("cod_postal") == "08022") &
        (pl.col("datetime") >= pl.datetime(2023, 5, 29)) &
        (pl.col("datetime") < pl.datetime(2023, 5, 31))
    ).select(["datetime", "mwh_total", "mwh_residencial"]).sort("datetime"),
}
for nombre, resultado in casos.items():
    print(f"\n{'='*60}\n  {nombre}\n{'='*60}")
    print(resultado)

# Casos adicionales detectados con umbral 400k
print("\n" + "="*60 + "\n  Valores > 400k\n" + "="*60)
print(df.filter(pl.col("mwh_total") > 400000)
    .select(["datetime", "cod_postal", "nombre_postal", "mwh_total"])
    .sort("mwh_total", descending=True).head(10))

for cp in ["08013", "08036", "08009", "08006", "08011", "08030"]:
    fecha_max = df.filter(pl.col("cod_postal") == cp).sort("mwh_total", descending=True).head(1)["datetime"][0]
    print(f"\n{'='*50}\nCP {cp}\n{'='*50}")
    print(df.filter(
        (pl.col("cod_postal") == cp) &
        (pl.col("datetime") >= fecha_max - pl.duration(days=3)) &
        (pl.col("datetime") <= fecha_max + pl.duration(days=3))
    ).select(["datetime", "mwh_total"]).sort("datetime"))
```

**Errores de reporte — se imputan con mediana histórica 2022-2024:**

- **08030 (5–11 may 2023, 28 bloques):** `mwh_industria` de 6-12× el habitual (~50k) durante 7 días consecutivos. Caída brusca al día siguiente sin gradualidad — fallo de acumulación en reporte.
- **08037 (nov-2024/dic-2024/feb-2025, 3 bloques):** `mwh_servicios` de 1.46M vs ~35k habitual. Patrón idéntico en los 3 casos en el mismo bloque horario (00:00h) — posible error de acumulación periódica.
- **08022 (29–30 may 2023, 6 bloques):** bloques 00:00h normales pero 06:00, 12:00 y 18:00h inflados 10-15×. Posible error de distribución del consumo diario entre bloques.
- **08030 (9 ene 2025, 4 bloques):** salto abrupto de ~300k a 558-595k en un solo día sin contexto climático que lo justifique. Vuelta a normal al día siguiente.
- **08037 (jul–nov 2025, ~475 bloques):** todos los sectores 6-7× el histórico (~80-92k MWh) durante 5 meses consecutivos. Imposible atribuirlo a demanda real.
- **08009 (2 jun 2025, 4 bloques):** todos los sectores inflados proporcionalmente (~7× habitual) en un único día. Sin contexto climático ni evento que lo justifique.

**Picos reales — se mantienen:**

- **08013 (jul 2025):** crecimiento gradual durante varios días (~563k máx). Patrón consistente con ola de calor de inicio de verano.
- **08036 (ago 2023):** valores sostenidos 3-4 días (~535k máx). Gradualidad clara de subida y bajada, consistente con ola de calor.
- **08006 (jul 2025):** pico de 4-5 días con valores crecientes y decrecientes. Mismo patrón de ola de calor que 08013.
- **08011 (ene 2020):** valores altos sostenidos durante más de una semana. Patrón consistente con ola de frío invernal.


```python
#### Imputación de Outliers
# Funciones de imputación por mediana histórica (2022-2024)

def imputar_sector(df, cp, sector, filtro, keys=["hora", "mes"]):
    """Imputa un sector específico con mediana histórica excluyendo el periodo erróneo."""
    med = (
        df.filter((pl.col("cod_postal") == cp) & ~filtro)
        .group_by(keys)
        .agg(pl.col(sector).median().alias("_med"))
    )
    return (df.join(med, on=keys, how="left")
        .with_columns(
            pl.when((pl.col("cod_postal") == cp) & filtro)
              .then(pl.col("_med")).otherwise(pl.col(sector)).alias(sector)
        ).drop("_med"))

def imputar_todos_sectores(df, cp, cond, anios_ref, mes_ref, keys=["hora"]):
    """Imputa todos los sectores con mediana histórica por referencia de años."""
    mes_filter = pl.col("mes").is_in(mes_ref) if isinstance(mes_ref, list) else pl.col("mes") == mes_ref
    med = (
        df.filter((pl.col("cod_postal") == cp) & pl.col("anio").is_in(anios_ref) & mes_filter)
        .group_by(keys)
        .agg([pl.col("mwh_industria").median().alias("_ind"),
              pl.col("mwh_residencial").median().alias("_res"),
              pl.col("mwh_servicios").median().alias("_ser")])
    )
    return (df.join(med, on=keys, how="left")
        .with_columns([
            pl.when(cond).then(pl.col("_ind")).otherwise(pl.col("mwh_industria")).alias("mwh_industria"),
            pl.when(cond).then(pl.col("_res")).otherwise(pl.col("mwh_residencial")).alias("mwh_residencial"),
            pl.when(cond).then(pl.col("_ser")).otherwise(pl.col("mwh_servicios")).alias("mwh_servicios"),
        ]).drop(["_ind", "_res", "_ser"]))

# 08030 — mwh_industria, 5-11 may 2023 (28 bloques)
df = imputar_sector(df, "08030", "mwh_industria",
    (pl.col("datetime") >= pl.datetime(2023, 5, 5)) & (pl.col("datetime") < pl.datetime(2023, 5, 12)))

# 08037 — mwh_servicios, 3 bloques puntuales
df = imputar_sector(df, "08037", "mwh_servicios",
    (pl.col("datetime") == pl.datetime(2024, 11, 20, 0)) |
    (pl.col("datetime") == pl.datetime(2024, 12, 22, 0)) |
    (pl.col("datetime") == pl.datetime(2025, 2, 24, 0)))

# 08022 — mwh_residencial, 29-30 may 2023 (6 bloques)
df = imputar_sector(df, "08022", "mwh_residencial",
    (pl.col("datetime") >= pl.datetime(2023, 5, 29)) & (pl.col("datetime") < pl.datetime(2023, 5, 31)))

# 08030 — todos sectores, 9 ene 2025 (4 bloques)
df = imputar_todos_sectores(df, "08030",
    cond=(pl.col("cod_postal") == "08030") & (pl.col("datetime").dt.date() == pl.date(2025, 1, 9)),
    anios_ref=[2022, 2023, 2024], mes_ref=1)

# 08037 — todos sectores, jul-nov 2025 (~475 bloques, umbral > 100k)
df = imputar_todos_sectores(df, "08037",
    cond=(pl.col("cod_postal") == "08037") & (pl.col("anio") == 2025) &
         (pl.col("mes").is_in([7, 8, 9, 10, 11])) & (pl.col("mwh_total") > 100000),
    anios_ref=[2022, 2023, 2024], mes_ref=[7, 8, 9, 10, 11], keys=["hora", "mes", "dia_semana"])

# Fix mwh_no_especificado anómalo en 08037 (nunca tuvo este sector en histórico)
df = df.with_columns(
    pl.when((pl.col("cod_postal") == "08037") & (pl.col("anio") == 2025) &
            (pl.col("mes").is_in([7, 8, 9, 10, 11])) & (pl.col("mwh_no_especificado").is_not_null()))
    .then(pl.lit(None).cast(pl.Int64))
    .otherwise(pl.col("mwh_no_especificado"))
    .alias("mwh_no_especificado")
)

# 08009 — todos sectores, 2 jun 2025 (4 bloques)
df = imputar_todos_sectores(df, "08009",
    cond=(pl.col("cod_postal") == "08009") & (pl.col("datetime").dt.date() == pl.date(2025, 6, 2)),
    anios_ref=[2022, 2023, 2024], mes_ref=6)

print("Todos los outliers positivos imputados")
```


```python
# Recálculo mwh_total + verificación final
df = df.with_columns(
    (pl.col("mwh_industria").fill_null(0) + pl.col("mwh_residencial").fill_null(0) +
     pl.col("mwh_servicios").fill_null(0) + pl.col("mwh_no_especificado").fill_null(0)
    ).alias("mwh_total")
)

print(f"Valores > 400k restantes: {df.filter(pl.col('mwh_total') > 400000).shape[0]}")
print(df.filter(pl.col("mwh_total") > 400000)
    .select(["datetime", "cod_postal", "nombre_postal", "mwh_total", "mwh_industria", "mwh_residencial", "mwh_servicios"])
    .sort("mwh_total", descending=True).head(20))

vals_target = df['mwh_total'].drop_nulls().to_numpy()
fig, axes = plt.subplots(1, 2, figsize=(14, 4))
axes[0].hist(vals_target, bins=80, color='#1B3A5C', edgecolor='white', linewidth=0.3, alpha=0.85)
axes[0].set_title('Distribución de mwh_total (tras imputación)', fontweight='bold')
axes[0].set_xlabel('MWh'); axes[0].set_ylabel('Frecuencia')
axes[1].boxplot(vals_target, vert=False, patch_artist=True,
                boxprops=dict(facecolor='#1B3A5C', alpha=0.7),
                medianprops=dict(color='#E07B5A', linewidth=2),
                flierprops=dict(marker='o', markersize=2, alpha=0.3, color='#1B3A5C'),
                whiskerprops=dict(color='#1B3A5C'), capprops=dict(color='#1B3A5C'))
axes[1].set_title('Boxplot de mwh_total', fontweight='bold')
axes[1].set_xlabel('MWh')
plt.tight_layout(); plt.show()
```

> Tras imputar los 520 registros erróneos con mediana histórica (2022-2024), la distribución muestra una forma log-normal coherente con el comportamiento esperado del consumo eléctrico urbano. La mayoría de bloques se concentran entre 20k–150k MWh, con una cola derecha moderada que refleja los picos reales de demanda estival. Los valores máximos legítimos (olas de calor 08013, 08036, 08006; ola de frío 08011) se mantienen como parte de la variabilidad natural del sistema.

## Descomposición de consumo energético


```python
import matplotlib.dates as mdates
```


```python
# Serie temporal global — suma de todos los CPs
serie_global = (
    df.group_by("datetime")
    .agg([
        pl.col("mwh_industria").sum(),
        pl.col("mwh_residencial").sum(),
        pl.col("mwh_servicios").sum(),
    ])
    .sort("datetime")
)

fechas = serie_global["datetime"].to_numpy()
ind    = serie_global["mwh_industria"].to_numpy()
res    = serie_global["mwh_residencial"].to_numpy()
ser    = serie_global["mwh_servicios"].to_numpy()

fig, ax = plt.subplots(figsize=(24, 5))
ax.stackplot(fechas, ind, res, ser,
             labels=["Industria", "Residencial", "Servicios"],
             colors=["#264653", "#2A9D8F", "#E9C46A"],
             alpha=0.8)
ax.set_title("Consumo por sector — Barcelona (todos los CPs)", fontweight="bold")
ax.set_ylabel("MWh")
ax.legend(loc="upper right")
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
plt.xticks(rotation=45, ha="right", fontsize=8)
plt.tight_layout()
plt.show()
```


```python
(
    df.group_by("cod_postal")
    .agg([
        pl.col("mwh_industria").sum(),
        pl.col("mwh_residencial").sum(),
        pl.col("mwh_servicios").sum(),
        pl.col("mwh_no_especificado").sum(),
    ])
    .with_columns([
        (pl.col("mwh_industria") / (pl.col("mwh_industria") + pl.col("mwh_residencial") + pl.col("mwh_servicios")) * 100).round(1).alias("pct_industria"),
        (pl.col("mwh_residencial") / (pl.col("mwh_industria") + pl.col("mwh_residencial") + pl.col("mwh_servicios")) * 100).round(1).alias("pct_residencial"),
        (pl.col("mwh_servicios") / (pl.col("mwh_industria") + pl.col("mwh_residencial") + pl.col("mwh_servicios")) * 100).round(1).alias("pct_servicios"),
    ])
    .select(["cod_postal", "pct_industria", "pct_residencial", "pct_servicios"])
    .sort("pct_industria", descending=True)
)
```

mwh_total es una variable compuesta, es la suma de consumo industrial, residencial, de servicios y no especificado. 

Ningún código postal tiene un perfil puro: 

- servicios domina en 38 de 42 CPs, siendo el sector principal en toda Barcelona. 

- La industria solo es relevante en 3-4 CPs (08039, 08004, 08038).

- Residencial alcanza su máximo en zonas periféricas como 08032 (70%). Esta composición explica por qué los patrones temporales difieren entre barrios, un CP con alto peso industrial será más probable en tener picos en horario laboral de lunes a viernes, mientras uno con alto peso residencial mostrará picos vespertinos y mayor consumo en fin de semana. 

El modelo que cree deberá capturar estas diferencias de perfil entre CPs, no solo la magnitud del consumo.

#### Serie Temporal de mwh


```python
# Serie temporal global — suma de todos los CPs
import matplotlib.dates as mdates

serie_global = (
    df.group_by("datetime")
    .agg([
        pl.col("mwh_industria").sum(),
        pl.col("mwh_residencial").sum(),
        pl.col("mwh_servicios").sum(),
    ])
    .sort("datetime")
)

fechas = serie_global["datetime"].to_numpy()
ind    = serie_global["mwh_industria"].to_numpy()
res    = serie_global["mwh_residencial"].to_numpy()
ser    = serie_global["mwh_servicios"].to_numpy()

fig, ax = plt.subplots(figsize=(24, 5))
ax.stackplot(fechas, ind, res, ser,
             labels=["Industria", "Residencial", "Servicios"],
             colors=["#264653", "#2A9D8F", "#E9C46A"],
             alpha=0.8)
ax.set_title("Consumo por sector — Barcelona (todos los CPs)", fontweight="bold")
ax.set_ylabel("MWh")
ax.legend(loc="upper right")
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
plt.xticks(rotation=45, ha="right", fontsize=8)
plt.tight_layout()
plt.show()
```

> El consumo eléctrico de Barcelona muestra una estacionalidad anual clara con picos en verano (julio–agosto) y en invierno (diciembre–enero), consistente con el efecto combinado de aire acondicionado y calefacción. Los servicios dominan el consumo en todos los períodos, seguidos del residencial. La industria tiene un peso marginal y estable.

> Se observa una caída notable en 2020 (COVID-19) y una recuperación progresiva hasta 2022. A partir de 2023 el consumo se estabiliza con niveles ligeramente superiores al período pre-pandemia.


```python
(df.group_by(["cod_postal", "nombre_postal"])
 .agg([
     pl.len().alias("n_registros"),
     pl.col("mwh_total").null_count().alias("nulls"),
     pl.col("mwh_total").mean().alias("mwh_mean"),
     pl.col("mwh_industria").sum().alias("sum_industria"),
     pl.col("mwh_residencial").sum().alias("sum_residencial"),
     pl.col("mwh_servicios").sum().alias("sum_servicios"),
     pl.col("mwh_total").sum().alias("sum_total"),
 ])
 .with_columns([
     ((pl.col("sum_industria") / pl.col("sum_total") * 100).round(1).cast(pl.Utf8) + pl.lit("%")).alias("pct_industria"),
     ((pl.col("sum_residencial") / pl.col("sum_total") * 100).round(1).cast(pl.Utf8) + pl.lit("%")).alias("pct_residencial"),
     ((pl.col("sum_servicios") / pl.col("sum_total") * 100).round(1).cast(pl.Utf8) + pl.lit("%")).alias("pct_servicios"),
 ])
 .drop(["sum_industria", "sum_residencial", "sum_servicios", "sum_total"])
 .sort("mwh_mean", descending=True)
)
```

Se seleccionan 4 códigos postales representativos de perfiles de consumo distintos,
con cobertura completa (10.104 registros) y mínimos nulos:

- **08038 Montjuïc / Zona Franca**  por perfil industrial (36.7% industria), mayor proporción del dataset
- **08005 El Poblenou** por perfil mixto equilibrado, referente en literatura smart city Barcelona, 4 nulos
- **08032 El Carmel / El Guinardó** — perfil residencial dominante (70.3% residencial)
- **08002 Barri Gòtic** por perfil servicios/turístico (75.6% servicios), consumo estacional marcado


```python
CPS = {
    "08038": "Montjuïc / Zona Franca (Industrial)",
    "08005": "El Poblenou (Mixto)",
    "08032": "El Carmel / El Guinardó (Residencial)",
    "08002": "Barri Gòtic (Servicios/Turístico)",
}
```


```python
for cp, nombre in CPS.items():
    serie_cp = (
        df.filter(pl.col("cod_postal") == cp)
        .sort("datetime")
        .select(["datetime", "mwh_industria", "mwh_residencial", "mwh_servicios"])
    )

    fechas = serie_cp["datetime"].to_numpy()
    ind    = serie_cp["mwh_industria"].to_numpy()
    res    = serie_cp["mwh_residencial"].to_numpy()
    ser    = serie_cp["mwh_servicios"].to_numpy()

    fig, ax = plt.subplots(figsize=(16, 4))
    ax.stackplot(fechas, ind, res, ser,
                 labels=["Industria", "Residencial", "Servicios"],
                 colors=["#264653", "#2A9D8F", "#E9C46A"],
                 alpha=0.8)
    ax.set_title(f"Consumo por sector — CP {cp} ({nombre})", fontweight="bold")
    ax.set_ylabel("MWh")
    ax.legend(loc="upper right")
    plt.tight_layout()
    plt.show()
```

> **Observaciones iniciales — Consumo por sector**
 
> - 08038 Zona Franca: perfil industrial visible y sostenido. Pico 2020 probablemente asociado a actividad que no se detuvieron durante el confinamiento.
> - 08005 El Poblenou servicios domina sobre industria y residencial, reflejo de la reconversión del distrito hacia tecnología y oficinas.
> - 08032 El Carmel balance residencial-servicios con industria casi nula. Aparente estacionalidad de verano muy marcada.
> - 08002 Barri Gòtic servicios aplastante. El desplome de 2020–2021 es el más pronunciado de los 4 CPs, señal directa del impacto del COVID en el turismo.


```python
fig, axes = plt.subplots(2, 2, figsize=(16, 8), sharey=False)
fig.suptitle("Estacionalidad anual — consumo medio mensual por CP", fontweight="bold")

meses = ["Ene", "Feb", "Mar", "Abr", "May", "Jun",
         "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]

for ax, (cp, nombre) in zip(axes.flatten(), CPS.items()):
    mensual = (
        df.filter(
            (pl.col("cod_postal") == cp) &
            (pl.col("anio") < 2026)
        )
        .group_by("mes")
        .agg(pl.col("mwh_total").mean().alias("mwh_medio"))
        .sort("mes")
    )

    ax.bar(mensual["mes"].to_numpy(), mensual["mwh_medio"].to_numpy(),
           color="#2A9D8F", alpha=0.85)
    ax.set_title(f"{cp} — {nombre}", fontsize=9, fontweight="bold")
    ax.set_ylabel("MWh medio")
    ax.set_xticks(range(1, 13))
    ax.set_xticklabels(meses, fontsize=8)

plt.tight_layout()
plt.show()
```

> Los 4 CPs muestran patrones estacionales distintos según su perfil de consumo.
- Industrial (08038) y turístico (08002) pican en julio-agosto con 195k y 155k MWh respectivamente con caídas en marzo-abril, patrón consistente con mayor actividad de temporada y demanda de climatización.
- El residencial (08032) invierte el patrón: picos en enero y diciembre 65k MWh y mínimos en abril-mayo (48k MWh), sugiriendo mayor dependencia de calefacción que de refrigeración.
- El mixto (08005) se mantiene más estable a lo largo del año con un pico moderado en julio-agosto (150k MWh) y un segundo repunte en enero por consumo invernal.
- En magnitud absoluta, Zona Franca lidera con diferencia mientras El Carmel se mantiene en el rango más bajo al carecer de actividad industrial o turística significativa.


```python
fig, axes = plt.subplots(2, 2, figsize=(16, 8), sharey=False)
fig.suptitle("Patrón semanal — consumo medio por día y hora", fontweight="bold")

dias = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]

for ax, (cp, nombre) in zip(axes.flatten(), CPS.items()):
    semanal = (
        df.filter(pl.col("cod_postal") == cp)
        .group_by(["dia_semana", "hora"])
        .agg(pl.col("mwh_total").mean().alias("mwh_medio"))
        .sort(["dia_semana", "hora"])
    )

    for hora_val, color in zip([0, 6, 12, 18], ["#264653", "#2A9D8F", "#E9C46A", "#E76F51"]):
        subset = semanal.filter(pl.col("hora") == hora_val)
        ax.plot(subset["dia_semana"].to_numpy(),
                subset["mwh_medio"].to_numpy(),
                marker="o", label=f"{hora_val}h", color=color)

    ax.set_title(f"{cp} — {nombre}", fontsize=9, fontweight="bold")
    ax.set_ylabel("MWh medio")
    ax.set_xticks(range(7))
    ax.set_xticklabels(dias, fontsize=8)
    ax.legend(fontsize=7)
    ax.axvspan(4.5, 6.5, alpha=0.08, color="gray", label="Fin de semana")

plt.tight_layout()
plt.show()
```

> El patrón semanal revela diferencias claras entre perfiles. 

- La Zona Franca (08038) registra la caída más pronunciada en fin de semana, con descensos de 60k MWh en los bloques 6h y 12h del domingo respecto a los días laborables, señal directa de paralización de actividad industrial. El Barri Gòtic (08002) también cae en domingo pero mantiene el bloque 18h elevado, coherente con actividad de restauración y ocio nocturno que no para el fin de semana.
- El Carmel (08032) es el más estable a lo largo de la semana: la diferencia entre martes y domingo es mínima (5k MWh), confirmando que el consumo residencial no
  responde al calendario laboral. El Poblenou (08005) presenta un comportamiento intermedio con caída moderada en fin de semana.
- En cuanto a magnitudes, Zona Franca lidera con bloques de 12h que superan los 200k MWh en días laborables, seguida de Barri Gòtic (165k MWh) y Poblenou
  (158k MWh). El Carmel se mantiene muy por debajo (~69k MWh en 18h), reflejo
  de su perfil mayoritariamente residencial.
- En todos los CPs el bloque 12h concentra el mayor consumo y el 0h el mínimo, confirmando el patrón intradiario como una de las señales más predictivas del dataset.

> Conlsuciones de decomposición de consumo:

- Los patrones temporales confirman que el consumo eléctrico en Barcelona responde a tres ciclos superpuestos: 
    - intradiario (pico 12h, valle 0h)
    - semanal (caída en fin de semana variable según perfil)
    - anual con:
        - pico verano en industrial y turístico
        - pico invierno en residencial.
- Estos ciclos son consistentes y repetibles, lo que valida la viabilidad del forecasting: si el consumo fuera aleatorio, predecir sería imposible. La señal estructural existe y es suficientemente fuerte.
- La diferencia de comportamiento entre perfiles (industrial, residencial, turístico) confirma que cod_postal y la composición sectorial son features clave para el modelo ya que sin ellas, el modelo confundiría patrones de barrios con dinámicas completamente distintas.
- Las variables de calendario (hora, dia_semana, mes, es_finde, es_festivo) ya presentes en el dataset son directamente justificadas por este análisis como features de alta importancia esperada para el modelado

## Decomposición STL

#### Decomposición STL Barrio Gotic Residencial- SERVICIOS


```python
cp = "08032"
serie_cp = (
    df.filter(pl.col("cod_postal") == cp)
    .sort("datetime")
    .select(["datetime", "mwh_total"])
)

fechas     = serie_cp["datetime"].to_numpy()
serie_vals = serie_cp["mwh_total"].to_numpy()
```


```python
print(
    df.filter(
        (pl.col("cod_postal") == "08032") &
        (pl.col("datetime") >= pl.datetime(2025, 7, 1)) &
        (pl.col("datetime") < pl.datetime(2025, 11, 1))
    )
    .select(["datetime", "mwh_total", "mwh_industria", "mwh_residencial", "mwh_servicios"])
    .sort("mwh_total")
    .head(20)
)
```


```python
import matplotlib.dates as mdates

res = STL(serie_vals, period=28, robust=True).fit()

fig, axes = plt.subplots(4, 1, figsize=(16, 10), sharex=True)
fig.suptitle(f"Descomposición STL — CP {cp} ({CPS[cp]})", fontweight="bold")

for ax, (nombre_comp, vals) in zip(axes, {
    "Serie original": serie_vals,
    "Tendencia":      res.trend,
    "Estacionalidad": res.seasonal,
    "Residuo":        res.resid,
}.items()):
    ax.plot(fechas, vals, color="#264653", linewidth=0.6)
    ax.set_ylabel(nombre_comp, fontsize=9)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))

plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
```

- La descomposición STL del Barri Gòtic (08002) revela una serie con estructura predecible pero con picos externos importantes. La tendencia muestra el impacto más pronunciado del COVID de los 4 CPs analizados: caída desde 200k hasta 100k MWh, entre enero y abril 2020, reflejo directo del colapso del turismo y la actividad comercial. La recuperación fue gradual y no se completó hasta 2023.
- La estacionalidad capturada corresponde al ciclo semanal (period=28). El ciclo anual queda absorbido en la tendencia como ondulaciones lentas recurrentes. En el Barri Gòtic, al ser un perfil de servicios/turístico, el pico anual se concentra en verano sin un repunte invernal significativo. Este comportamiento contrasta con lo esperado en perfiles residenciales como El Carmel (08032), donde la tendencia debería mostrar picos en enero-diciembre por calefacción.
- La estacionalidad semanal es clara y consistente, con amplitud variable: más pronunciada en verano oscilando 50k MWh cuando la actividad turística intensifica los contrastes entre bloques horarios. 
- El residuo se mantiene cercano a 0 la mayor parte del tiempo, con picos puntuales en 2022-2024 atribuibles a eventos masivos no recurrentes (Tal vez conciertos o festivales). Esto confirma que la serie tiene estructura predecible y que los errores del modelo se concentrarán en eventos atípicos difíciles de anticipar sin información externa.

#### Decomposición STL resto de barrios


```python
for cp, nombre in {k: v for k, v in CPS.items() if k != "08002"}.items():
    serie_cp = (
        df.filter(pl.col("cod_postal") == cp)
        .sort("datetime")
        .select(["datetime", "mwh_total"])
    )

    fechas     = serie_cp["datetime"].to_numpy()
    serie_vals = serie_cp["mwh_total"].to_numpy()

    res = STL(serie_vals, period=28, robust=True).fit()

    fig, axes = plt.subplots(4, 1, figsize=(16, 10), sharex=True)
    fig.suptitle(f"Descomposición STL — CP {cp} ({nombre})", fontweight="bold")

    for ax, (nombre_comp, vals) in zip(axes, {
        "Serie original": serie_vals,
        "Tendencia":      res.trend,
        "Estacionalidad": res.seasonal,
        "Residuo":        res.resid,
    }.items()):
        ax.plot(fechas, vals, color="#264653", linewidth=0.6)
        ax.set_ylabel(nombre_comp, fontsize=9)
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))

    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
```

>08038 Zona Franca (Industrial):

- Tendencia: inestable con caída pronunciada en COVID 2020 (~100k MWh) y picos en 2023-2024 (230k MWh)
- Estacionalidad: semanal pronunciada (±50k MWh), con amplitud que colapsa durante COVID y se recupera progresivamente. El ciclo anual queda absorbido en la tendencia como ondulaciones de verano.
- Residuo: cercano a 0 la mayor parte del tiempo, con picos en 2022-2024.

08005 El Poblenou (Mixto):
- Tendencia: caída COVID moderada (80k MWh) con recuperación lenta, el distrito tardó en retomar actividad. Oscilaciones anuales visibles pero menos pronunciadas que en perfiles industriales o turísticos.
- Estacionalidad: semanal estable y consistente ( oscila aproximadamente 40k MWh) a lo largo de toda la serie, sin grandes variaciones de amplitud entre años.
- Residuo: el más limpio de los 4 CPs, con picos puntuales en 2022-2023 Confirma que El Poblenou es el CP más predecible del conjunto.

>08032 El Carmel (Residencial):
- Tendencia: oscilaciones anuales más marcadas que los otros CPs, picos de invierno y valles de primavera claramente visibles. Impacto COVID el más suave de los 4, coherente con un perfil que no depende de actividad económica externa.
- Estacionalidad: semanal de menor amplitud (menor oscilación de apróximadamente 20k MWh) y más estable que los perfiles industriales o turísticos , el consumo residencial varía menos entre días laborables y fin de semana como vimos anteriormente
- Residuo: muy cercano a 0 con picos mínimos. La serie residencial es la más regular y estable del conjunto, lo que anticipa menores errores de predicción.

### ADF Y KPSS

- ADF: Augmented Dickey-Fuller. Detecta si la serie tiene raíz unitaria (tendencia que no vuelve a un valor fijo). H0: la serie NO es estacionaria.

- KPSS: Kwiatkowski-Phillips-Schmidt-Shin. Lo contrario: H0: la serie SÍ es estacionaria.

Se usan juntos porque se contradicen, si ambos coinciden la conclusión es clara, si no coinciden hay estacionariedad parcial.


```python
from arch.unitroot import ADF, KPSS
import pandas as pd

resultados = []

for cp, nombre in CPS.items():
    serie = (
        df.filter(pl.col("cod_postal") == cp)
        .sort("datetime")["mwh_total"]
        .to_numpy()
    )

    adf   = ADF(serie, method="aic")
    kpss_ = KPSS(serie)

    resultados.append({
        "CP": cp,
        "Barrio": nombre,
        "ADF stat": round(adf.stat, 3),
        "ADF p-valor": round(adf.pvalue, 4),
        "ADF estacionaria": "Si" if adf.pvalue < 0.05 else "No",
        "KPSS stat": round(kpss_.stat, 3),
        "KPSS p-valor": round(kpss_.pvalue, 4),
        "KPSS estacionaria": "Si" if kpss_.pvalue > 0.05 else "No",
    })

pd.DataFrame(resultados)
```

**ADF** evalúa si la serie "se va a la deriva" sin volver a un nivel base. 
Al rechazar la hipótesis nula en los 4 casos (p < 0.05), confirma que las series siempre orbitan alrededor de un valor central estable, son estacionarias en media. A pesar de tener cambios diferenciados en el consumo, se vuelven a centrarse

**KPSS** evalúa si el tamaño de los saltos es constante a lo largo del tiempo. Se rechaza la hipótesis nula (p < 0.05), confirmando que la dispersión cambia según el período, el consume varía difente entre periodos. caída COVID-2020, recuperación 2022, estabilización posterior y por tanto no son estacionarias en varianza.

La combinación de ambos resultados es coherente: las series tienen un nivel promedio estable pero con cambios estructurales en su variabilidad.

- XGBoost y LSTM/GRU son robustos a esta condición. 
- SARIMA requerirá diferenciación estacional para cumplir sus supuestos.

### ACF/PACF


```python
import numpy as np
import matplotlib.pyplot as plt

serie = (
    df.group_by("datetime")
    .agg(pl.col("mwh_total").sum())
    .sort("datetime")["mwh_total"]
    .to_numpy()
)

def acf_manual(x, nlags=112):
    n = len(x)
    x = x - x.mean()
    acf_vals = [np.dot(x[:n-k], x[k:]) / np.dot(x, x) for k in range(nlags+1)]
    return np.array(acf_vals)

def pacf_manual(x, nlags=112):
    acf_vals = acf_manual(x, nlags)
    pacf_vals = [1.0]
    for k in range(1, nlags+1):
        mat = np.array([[acf_vals[abs(i-j)] for j in range(k)] for i in range(k)])
        vec = acf_vals[1:k+1]
        try:
            coef = np.linalg.solve(mat, vec)
            pacf_vals.append(coef[-1])
        except:
            pacf_vals.append(0)
    return np.array(pacf_vals)

lags      = np.arange(113)
acf_vals  = acf_manual(serie, nlags=112)
pacf_vals = pacf_manual(serie, nlags=112)
ci        = 1.96 / np.sqrt(len(serie))

fig, axes = plt.subplots(2, 1, figsize=(16, 8))

axes[0].vlines(lags, 0, acf_vals, color="#1B3A5C", linewidth=0.8)
axes[0].axhline(0,   color="black", linewidth=0.5)
axes[0].axhline(ci,  color="gray",  linestyle="--", linewidth=0.8)
axes[0].axhline(-ci, color="gray",  linestyle="--", linewidth=0.8)
axes[0].axvline(x=4,  color="#E07B5A", linestyle="--", alpha=0.7, label="lag 4 = 24h")
axes[0].axvline(x=28, color="#2A9D8F", linestyle="--", alpha=0.7, label="lag 28 = 7 dias")
axes[0].set_title("ACF — Barcelona Global", fontweight="bold")
axes[0].set_xlabel("Lag (bloques de 6h)")
axes[0].legend()

axes[1].vlines(lags, 0, pacf_vals, color="#1B3A5C", linewidth=0.8)
axes[1].axhline(0,   color="black", linewidth=0.5)
axes[1].axhline(ci,  color="gray",  linestyle="--", linewidth=0.8)
axes[1].axhline(-ci, color="gray",  linestyle="--", linewidth=0.8)
axes[1].axvline(x=4,  color="#E07B5A", linestyle="--", alpha=0.7, label="lag 4 = 24h")
axes[1].axvline(x=28, color="#2A9D8F", linestyle="--", alpha=0.7, label="lag 28 = 7 dias")
axes[1].set_title("PACF — Barcelona Global", fontweight="bold")
axes[1].set_xlabel("Lag (bloques de 6h)")
axes[1].legend()

plt.tight_layout()
plt.show()
```


```python
print(f"{'lag':>5} │ {'ACF':>8} │ {'PACF':>8}")
print("──────┼──────────┼──────────")
for i in range(30):
    print(f"  {i:3d} │ {acf_vals[i]:>8.4f} │ {pacf_vals[i]:>8.4f}")
```

#### Análisis Correlación Cruzada


```python
from scipy.signal import correlate
import matplotlib.pyplot as plt
import numpy as np

# ── Un CP representativo — usamos el global agregado ─────────────────
serie = (
    df.group_by("datetime")
    .agg([
        pl.col("mwh_total").mean().alias("mwh"),
        pl.col("temp_mean").mean().alias("temp"),
    ])
    .sort("datetime")
    .drop_nulls()
)

mwh  = serie["mwh"].to_numpy()
temp = serie["temp"].to_numpy()

# Normalizar para que la correlación sea comparable
mwh_n  = (mwh  - mwh.mean())  / mwh.std()
temp_n = (temp - temp.mean()) / temp.std()

# Cross-correlation
lags   = np.arange(-20, 21)   # -20 bloques a +20 bloques (±120h)
xcorr  = [np.corrcoef(mwh_n[max(0,-l):len(mwh_n)-max(0,l)],
                      temp_n[max(0,l):len(temp_n)-max(0,-l)])[0,1]
           for l in lags]

fig, ax = plt.subplots(figsize=(12, 4))
ax.bar(lags, xcorr, color=["#E07B5A" if x > 0 else "#2B6CB0" for x in xcorr],
       alpha=0.8, width=0.7)
ax.axhline(0,  color="white",  linewidth=0.8)
ax.axhline(0.1,  color="grey", linewidth=0.6, linestyle="--", alpha=0.5)
ax.axhline(-0.1, color="grey", linewidth=0.6, linestyle="--", alpha=0.5)
ax.axvline(0, color="white", linewidth=1, linestyle=":")
ax.set_xlabel("Lag (bloques de 6h) — negativo: temp precede consumo")
ax.set_ylabel("Correlación de Spearman")
ax.set_title("Correlación cruzada mwh_total × temp_mean por desfase temporal",
             fontweight="bold")
ax.set_xticks(lags[::2])
ax.set_xticklabels([f"{l*6}h" for l in lags[::2]], fontsize=8)
plt.tight_layout()
plt.show()

# Lag de máxima correlación
best_lag = lags[np.argmax(np.abs(xcorr))]
print(f"Lag de máxima correlación: {best_lag} bloques ({best_lag*6}h)")
```

**ACF, correlaciones acumuladas:**
- **Lag 4 — 24h (0.83):** el consumo de hace 24h es un predictor muy fuerte del 
  actual. El mismo bloque horario del día anterior explica gran parte de la variabilidad.
- **Lag 28 — 7 días (0.87):** el mismo bloque de hace 7 días es el predictor más 
  potente de toda la serie. El patrón semanal domina sobre el diario.
- **Lags 8 (2d), 12 (3d), 16 (4d), 20 (5d), 24 (6d) — entre 0.70–0.78:** picos 
  repetidos cada 4 lags confirman el ciclo diario de 24h persistente a lo largo 
  de toda la semana.
- **Lag 2 — 12h (-0.05):** correlación ligeramente negativa. El bloque de hace 
  12h suele ser el opuesto al actual (si ahora es mediodía, hace 12h era madrugada), 
  lo que genera una leve correlación inversa.
- **Lag 56 y 84:** ecos del ciclo semanal (mismos día hace 2 y 3 semanas). La correlación se 
  mantiene significativa pero decrece progresivamente, lag 56 ( aprox0.80) y 
  lag 84 (aprox0.80) no aportan valor adicional relevante para el modelo.

**PACF — relaciones directas (sin efecto cadena):**
- **Lag 1 — 6h (0.37):** relación directa moderada con el bloque inmediatamente anterior.
- **Lag 2 — 12h (-0.22):** corrección negativa — el bloque de hace 12h tiene  efecto inverso directo, consistente con el patrón opuesto entre madrugada y mediodía.
- **Lag 3 — 18h (0.47):** relación directa relevante, el bloque de hace 18h aporta señal propia más allá de los lags anteriores.
- **Lag 4 — 24h (0.75):** el más alto de todos. Relación directa muy fuerte con el mismo bloque horario del día anterior, no es efecto cadena.
- **Lag 5 — 30h (-0.65):** corrección negativa fuerte tras el pico de lag 4. El modelo ajusta hacia abajo para no sobreestimar el efecto del día anterior.
- **Lag 29 — ~7 días (-0.40):** ajuste estacional del ciclo semanal justo antes de completar la semana.
- A partir de lag 6 hasta lag 28 las correlaciones son moderadas o ruido, confirmando que los lags clave son 1, 3, 4 y 5.

Los lags más informativos para el modelo son **lag_1, lag_4 y lag_28**, validando empíricamente los features de rezago planificados para XGBoost.

La PACF confirma que lag_4 (0.75) es la relación directa más fuerte, seguido de lag_3 (0.47) y lag_1 (0.37). Adicionalmente, lag_3 y lag_5 aportan señal directa relevante que lag_8 y lag_24 de la ACF no capturan por separado, estos últimos son en gran parte efecto acumulado del ciclo diario, no relaciones 
directas independientes.

## <font color='#4E8F6E'>  **Variables Explicativas Numéricas** </font>


#### <font color='#D4B93A'><b>Revisión</b></font>



```python
VARS_NUM = ['mwh_industria', 'mwh_residencial', 'mwh_servicios', 'mwh_no_especificado',
            'lst_celsius', 'temp_mean', 'temp_max', 'temp_min',
            'humedad_mean', 'viento_mean', 'precipitacion_sum', 'irradiancia_mean']

df_num = df.select(VARS_NUM)
df_num.head(6)

```


```python
df_num.describe()
```

Sectores de consumo
- mwh_industria, std (19k) mayor que la media (8.7k) y max (434k) es 50× la mediana (1.7k). Refleja la heterogeneidad entre barrios: Zona Franca vs barrios sin actividad industrial.
- mwh_residencial y mwh_servicios son más simétricas ya que la media y la mediana son parecidas. Servicios domina en magnitud media (56k).
- mwh_no_especificado,  85.35% nulos, count real de solo 62k de 424k registros.

Temperatura
- temp_mean rango -0.26°C a 36°C, media 17.8°C, mediana 17.4°C, distribución casi simétrica, sin anomalías.
- temp_max y temp_min pueden ser colineales con temp_mean, candidatas a descartar en feature engineering.
- lst_celsius media 24.7°C vs temp_mean 17.9°C, diferencia de aproximadamente 7°C esperada por Urban Heat Island (asfalto y tejados siempre más calientes que el aire). No son redundantes porque capturan señales distintas.

Variables problemáticas
- precipitacion_sum, mediana y P75 = 0.0, más del 75% de bloques sin lluvia. Más útil como binaria (llueve/no_llueve).

- irradiancia_mean, mínimo de -4.27 W/m², error de sensor ay qeu esto es físicamente imposible. se debe topar a 0 en feature engineering.

- humedad_mean y viento_mean, sin anomalías destacables.


```python
df.select(VARS_NUM + ['mwh_total']).null_count()
```

- mwh_industria, mwh_residencial, mwh_servicios, mwh_total, 0 nulos, imputación completada en limpieza.
- mwh_no_especificado, 362,256 nulos (85.35%), esto es normal y esperado
- temp_mean, humedad_mean, 28,025 nulos (6.6%), corresponden a X2 inactiva en períodos puntuales.
- temp_max, temp_min, precipitacion_sum, 136,700 nulos (32.2%), estaciones sin cobertura completa.
- viento_mean, irradiancia_mean,, 153,400 nulos (36.1%), X2 sin sensores para estas variables en todo el período.
- lst_celsius, 169,044 nulos (39.8%), cobertura de nubes (MODIS satelital), algo esperado
- Todos los nulos restantes son estructurales y conocidos, se mantienen para feature engineering.

- Nulos estructurales conocidos:
  - Los candidatos a imputación son temp_mean y humedad_mean(28,025 nulos, 6.6%), ya que tienen cobertura histórica 2019-2023 completa y solo fallan en X2 durante 2024-2025. 
  - El resto (viento, irradiancia, precipitacion, lst_celsius) se evaluarán durante el análisis bivariante para decidir si su variación entre barrios puede justificar imputarlos o se gestionan directamente en feature engineering.

#### <font color='#D4B93A'><b>Resumen Descriptivo de variables numéricas</b></font>



```python
plt.style.use('seaborn-v0_8-whitegrid')

VARS_NUM_PLOT = ['mwh_industria', 'mwh_residencial', 'mwh_servicios',
                 'lst_celsius', 'temp_mean', 'temp_max', 'temp_min',
                 'humedad_mean', 'viento_mean', 'precipitacion_sum', 'irradiancia_mean']

n_cols = 3
n_rows = int(np.ceil(len(VARS_NUM_PLOT) / n_cols))

fig = plt.figure(figsize=(16, n_rows * 5))
fig.suptitle('Distribución de variables numéricas', fontsize=13, fontweight='bold', y=1.02)

for i, var in enumerate(VARS_NUM_PLOT):
    v = df[var].drop_nulls().to_numpy()
    
    ax1 = fig.add_subplot(n_rows * 2, n_cols, (i // n_cols) * n_cols * 2 + (i % n_cols) + 1)
    ax1.hist(v, bins=30, color='#264653', edgecolor='white', alpha=0.85)
    ax1.set_title(var, fontsize=9, fontweight='bold')
    ax1.set_ylabel('Frecuencia', fontsize=7)
    ax1.tick_params(axis='x', rotation=45, labelsize=7)
    
    ax2 = fig.add_subplot(n_rows * 2, n_cols, (i // n_cols) * n_cols * 2 + n_cols + (i % n_cols) + 1)
    ax2.boxplot(v, vert=False, patch_artist=True,
                boxprops=dict(facecolor='#264653', alpha=0.7),
                medianprops=dict(color='#E9C46A', linewidth=2),
                flierprops=dict(marker='o', markersize=1.5, alpha=0.3))
    ax2.tick_params(axis='x', rotation=45, labelsize=7)  # ← aquíierprops=dict(marker='o', markersize=1.5, alpha=0.3))

plt.tight_layout()
plt.show()
```

- mwh_industria, mwh_residencial y mwh_servicios, todos con sesgo positivo, producto de la
  heterogeneidad entre barrios y picos energéticos puntuales que jalan la cola derecha.
    - mwh_industria es el más extremo, con la gran mayoría de bloques cerca de 0 y pocos CPscon actividad industrial significativa. 
    - mwh_residencial y mwh_servicios más acampanados pero igualmente sesgados.

- lst_celsius, temp_mean, temp_max, temp_min, distribuciones aparentemente bimodales,
  dos picos visibles que pueden explicarse por diferencias entre barrios o por estacionalidad (invierno vs verano), o ambos efectos superpuestos. Se revisará en el análisis bivariante por CP y por mes.

- humedad_mean, la más cercana a normal de todas las variables, con un leve sesgo negativo hacia valores altos de humedad.

- viento_mean, sesgo positivo claro, la mayoría de bloques con viento bajo y cola derecha de eventos puntuales de viento fuerte.

- precipitacion_sum, casi todo en 0 por clima mediterráneo, BCN simplemente no llueve la mayoría de días. El boxplot revela que cuando sí llueve los valores son dispersos y variables.

- irradiancia_mean, gran masa en 0 por bloques nocturnos (00h y 06h), ceros reales y esperados, no imputados. El resto distribuido ampliamente hasta 917 W/m². Comportamiento bimodal día/noche.

Test de Normalidad


```python
fig, axes = plt.subplots(4, 3, figsize=(16, 14))
axes = axes.flatten()

for i, var in enumerate(VARS_NUM_PLOT):
    v = df[var].drop_nulls().to_numpy()
    stats.probplot(v, dist='norm', plot=axes[i])
    axes[i].get_lines()[0].set(color='#264653', alpha=0.4, markersize=2)
    axes[i].get_lines()[1].set(color='#E76F51', linewidth=2)
    axes[i].set_title(var, fontsize=9, fontweight='bold')

for j in range(i + 1, len(axes)):
    axes[j].set_visible(False)

fig.suptitle('QQ-plots — variables numéricas', fontsize=13, fontweight='bold', y=1.02)
plt.tight_layout()
plt.show()
```

Ninguna variable sigue distribución normal, confirmado por la desviación sistemática
de la línea de referencia en todos los QQ-plots.

- mwh_industria, mwh_residencial, mwh_servicios, curvas en S pronunciadas con colas
  derechas muy pesadas, sesgo positivo extremo ya visto en los histogramas.

- lst_celsius, temp_mean, temp_max, temp_min, son las más cercanas a la línea pero
  muestran un ligero "baile" en forma de S suave, consistente con la bimodalidad
  observada en los histogramas (mezcla de estaciones del año y diferencias entre barrios).

- humedad_mean, la más cercana a normal visualmente, pero con leve desviación
  en las colas confirmando el sesgo izquierdo ya identificado.

- viento_mean, cola derecha clara, los eventos extremos de viento se alejan
  significativamente de la línea.

- precipitacion_sum, escalón casi vertical cerca de 0, reflejo directo de la
  masa de ceros. No tiene distribución continua real.

- irradiancia_mean, escalón pronunciado en 0 por los bloques nocturnos,
  seguido de distribución dispersa. La forma en S invertida confirma la bimodalidad día/noche.

Conclusión: se descarta Pearson para correlaciones, se usará **Spearman**. Para tests de hipótesis, **Kruskal-Wallis y Mann-Whitney** en lugar de ANOVA.

#### <font color='#D4B93A'><b>Análisis Bivariante</b></font>


Los sectores mwh_industria, mwh_residencial y mwh_servicios se excluyen del análisis bivariante por ser componentes directos de mwh_total, su separación Q1/Q3 sería trivial. Se añaden temp_max y temp_min para completar el análisis meteorológico.


```python
q1_t = df['mwh_total'].quantile(0.25)
q3_t = df['mwh_total'].quantile(0.75)

df_bajo = df.filter(pl.col('mwh_total') <= q1_t)
df_alto = df.filter(pl.col('mwh_total') >= q3_t)

VARS_BIV = ['temp_mean', 'humedad_mean', 'viento_mean',
            'irradiancia_mean', 'lst_celsius', 'precipitacion_sum']

n_cols = 3
n_rows = int(np.ceil(len(VARS_BIV) / n_cols))

fig, axes = plt.subplots(n_rows, n_cols, figsize=(16, n_rows * 4))
axes = axes.flatten()

for i, var in enumerate(VARS_BIV):
    v_bajo = df_bajo[var].drop_nulls().to_numpy()
    v_alto = df_alto[var].drop_nulls().to_numpy()

    axes[i].hist(v_bajo, bins=30, alpha=0.6, color='#264653', label='mwh bajo (Q1)', density=True)
    axes[i].hist(v_alto, bins=30, alpha=0.6, color='#E76F51', label='mwh alto (Q3)', density=True)
    axes[i].set_title(var, fontsize=9, fontweight='bold')
    axes[i].set_ylabel('Densidad', fontsize=7)
    axes[i].tick_params(axis='x', rotation=45, labelsize=7)
    axes[i].legend(fontsize=7)

for j in range(i + 1, len(axes)):
    axes[j].set_visible(False)

fig.suptitle('Bivariante: variables meteorológicas vs mwh_total (Q1 vs Q3)',
             fontsize=12, fontweight='bold', y=1.02)
plt.tight_layout()
plt.show()
```

- temp_mean, distribución normal en consumo bajo (temperaturas intermedias, sin necesidad
  de climatización) y bimodal en consumo alto (picos en frío y calor), confirmando una
  relación en U con mwh_total. Señal predictiva fuerte.

- humedad_mean, distribuciones similares pero consumo alto desplazado hacia humedad baja,
  coherente con días calurosos y secos de verano que generan mayor demanda de refrigeración.

- viento_mean, distribuciones muy solapadas, variable con menor poder discriminante del grupo.

- irradiancia_mean, comparación distorsionada por la masa de ceros nocturnos.
  Requiere análisis separado para bloques diurnos.

- lst_celsius, patrón similar a temp_mean pero consumo bajo más centrado entre los dos picos
  de la bimodal. LST mide la temperatura superficial del suelo y tejados (no del aire),
  por lo que captura el efecto Urban Heat Island, el fenómeno por el cual las zonas urbanas
  densas acumulan más calor que las periféricas debido al asfalto, edificios y actividad humana.
  Este efecto es más pronunciado en períodos de alta demanda energética.

- precipitacion_sum, distribuciones prácticamente idénticas, sin poder discriminante aparente.
  Confirma su utilidad limitada como variable continua.

- lst_celsius aporta señal complementaria a temp_mean, no redundante: temp_mean mide
  temperatura del aire cada 6h desde la estación Meteocat (igual para todos los CPs
  de la misma estación), mientras lst_celsius mide temperatura superficial de suelo y
  tejados por satélite una vez al día, variando por CP y capturando diferencias
  intraurbanas por Urban Heat Island. Ambas se mantienen en el modelo.

**Limitación**: se utilizó MOD11A1 (satélite Terra, paso ~10:30h local), asignando el
mismo valor LST a todos los bloques del día. El producto MYD11A1 (satélite Aqua,
~13:30h local) capturaría mejor el pico de Urban Heat Island al coincidir con el
máximo de radiación solar. Ambos productos podrían combinarse asignando Terra al
bloque 06h–12h y Aqua al bloque 12h–18h, obteniendo LST específica por bloque
diurno. Disponibles en GEE como MODIS/061/MOD11A1 y MODIS/061/MYD11A1 con
idéntico factor de escala. Se propone como mejora en trabajo futuro.

Test de Hipótesis


```python
# ─── Mann-Whitney: variables numéricas vs mwh_total (Q1 vs Q3) ──────────────
q1_t = df['mwh_total'].quantile(0.25)
q3_t = df['mwh_total'].quantile(0.75)

VARS_MW = ['temp_mean', 'temp_max', 'temp_min', 'humedad_mean',
           'viento_mean', 'precipitacion_sum', 'irradiancia_mean', 'lst_celsius']

resultados_mw = []
for var in VARS_MW:
    df_pair = df.select([var, 'mwh_total']).drop_nulls()
    g_bajo = df_pair.filter(pl.col('mwh_total') <= q1_t)[var].to_numpy()
    g_alto = df_pair.filter(pl.col('mwh_total') >= q3_t)[var].to_numpy()
    stat_mw, p_mw = stats.mannwhitneyu(g_bajo, g_alto, alternative='two-sided')
    resultados_mw.append({
        'variable':      var,
        'mediana_bajo':  round(float(np.median(g_bajo)), 3),
        'mediana_alto':  round(float(np.median(g_alto)), 3),
        'p_value':       p_mw,
        'significativo': 'si' if p_mw < 0.05 else 'no'
    })

res_mw = sorted(resultados_mw, key=lambda x: x['p_value'])

print(f"{'variable':<25} {'mediana_bajo':>14} {'mediana_alto':>14} {'p_value':>14} {'sig':>5}")
print('-' * 80)
for r in res_mw:
    print(f"{r['variable']:<25} {r['mediana_bajo']:>14.3f} {r['mediana_alto']:>14.3f} {r['p_value']:>14.4e} {r['significativo']:>5}")
```

Todas las variables son estadísticamente significativas (p < 0.05).

- irradiancia_mean, mayor diferencia absoluta (mediana 7 vs 193 W/m²), captura
  el bloque horario día/noche, señal más fuerte del grupo.

- temp_mean, temp_max, temp_min, diferencia consistente de ~2-3°C entre grupos,
  estadísticamente muy sólida. Refleja la relación en U con el consumo ya vista
  en el bivariante.

- humedad_mean, relación inversa confirmada (consumo bajo = mayor humedad),
  coherente con días calurosos y secos de verano.

- lst_celsius, diferencia de ~2.5°C, señal real pero más débil que las temperaturas
  Meteocat. Captura variación espacial entre barrios por Urban Heat Island.

- viento_mean, significativo pero diferencia práctica mínima (2.33 vs 2.10 m/s).
  Poca utilidad predictiva real.

- precipitacion_sum, ambas medianas en 0. Significativo solo por valores extremos
  en la cola. Como variable continua, utilidad limitada para el modelo.

## <font color='#4E8F6E'>  **Variables Explicativas Categóricas** </font>



```python
# ─── Describe variables categóricas/temporales ──────────────────────────────
VARS_CAT = ['hora', 'dia_semana', 'mes', 'anio', 'es_finde', 'es_festivo']

for var in VARS_CAT:
    conteo = (
        df.group_by(var)
        .agg(
            pl.len().alias('n'),
            pl.col('mwh_total').mean().round(1).alias('mwh_medio')
        )
        .sort(var)
        .with_columns((pl.col('n') / len(df) * 100).round(2).alias('pct'))
    )
    print(f"\n{'='*50}")
    print(f"  {var}")
    print(f"{'='*50}")
    print(conteo)

# ─── Describe cod_postal ─────────────────────────────────────────────────────
print(f"\n{'='*50}")
print(f"  cod_postal")
print(f"{'='*50}")
print(
    df.filter(pl.col('nombre_postal').is_not_null())
    .group_by(['cod_postal', 'nombre_postal'])
    .agg([
        pl.len().alias('n'),
        pl.col('mwh_total').mean().round(1).alias('mwh_medio'),
        pl.col('mwh_total').median().round(1).alias('mwh_mediana'),
        pl.col('mwh_total').max().round(1).alias('mwh_max'),
        pl.col('mwh_industria').sum().alias('sum_ind'),
        pl.col('mwh_residencial').sum().alias('sum_res'),
        pl.col('mwh_servicios').sum().alias('sum_serv'),
    ])
    .with_columns([
        (pl.col('sum_ind') / (pl.col('sum_ind') + pl.col('sum_res') + pl.col('sum_serv')) * 100).round(1).alias('pct_ind'),
        (pl.col('sum_res') / (pl.col('sum_ind') + pl.col('sum_res') + pl.col('sum_serv')) * 100).round(1).alias('pct_res'),
        (pl.col('sum_serv') / (pl.col('sum_ind') + pl.col('sum_res') + pl.col('sum_serv')) * 100).round(1).alias('pct_serv'),
    ])
    .drop(['sum_ind', 'sum_res', 'sum_serv'])
    .sort('mwh_medio', descending=True)
)
```

Patrones temporales

- Hora, valle en 00h (70,883 MWh) y pico en 12h (119,821 MWh), diferencia del 69%
  entre mínimo y máximo. Distribución perfectamente balanceada (25% por bloque),
  confirma grilla temporal completa.

- Dia semana, caída progresiva hacia el fin de semana: laborables ~105-106k MWh vs sábado 91k y domingo 86k. Diferencia de ~20k MWh entre miércoles y domingo, reflejo de la reducción de actividad industrial y de servicios.

- Mes, estacionalidad en U: pico en enero (110,564 MWh) y julio (118,150 MWh), valle en abril (88,877 MWh). Confirma doble estacionalidad por calefacción invernal y refrigeración estival.

- Anio, caída notable en 2020 (99,698 MWh) vs 2019 (113,277 MWh) por impacto COVID. Recuperación parcial en 2021-2022 y tendencia descendente desde 2023, posiblemente por mejoras de eficiencia energética o cambios en actividad económica.

- Fin de semana reduce el consumo medio un 11% (106,143 vs 94,058 MWh). Festivos reducen un 15% (101,517 vs 86,276 MWh), mayor impacto que el fin de semana, coherente con paralización de actividad industrial y comercial.

**Código postal:** El rango de consumo medio entre barrios es extremo muy alto. Va desde 29,683 MWh en 08033 Vallbona/Ciutat Meridiana a 233,425 MWh en 08040 La Zona Franca, una diferencia 
de 8x entre mínimo y máximo. Parecería ser la variable más discriminante del dataset.

Los CPs con mayor componente industrial lideran el consumo: La - Zona Franca (08040, 29% ind.), El Bon Pastor (08030, 16% ind.) y Montjuïc/Zona Franca (08038, 37% ind.), los únicos 
con industria >15%. 

En el extremo opuesto, los barrios periféricos residenciales concentran los consumos más bajos: Vallbona (08033, 57% res.) El Carmel (08032, 70% res.) y Vilapicina (08031, 64% res.).

Tres patrones anómalos merecen atención:
- **08037 Vila de Gràcia:** media baja (42,294 MWh) pero mwh_max de 142,210 MWh — 
  señal directa de los outliers ya imputados en la sección de calidad de datos.
- **08009 Dreta de l'Eixample:** media baja (46,049 MWh) pero mwh_max de 321,545 MWh. 
  No es outlier problemático — el perfil de servicios dominante (~59%) genera picos 
  reales en el bloque 12h durante enero y octubre, meses de máxima actividad comercial.
- **08036 L'Antiga Esquerra:** mediana (96,368 MWh) muy por debajo de la media 
  (112,190 MWh), distribución sesgada por picos extremos de verano ya documentados. 
  El 70% de servicios explica la concentración de consumo en bloques diurnos estivales.

La heterogeneidad entre CPs — tanto en volumen como en composición sectorial — justifica 
tratar cod_postal como variable categórica en el modelo. Cada CP tiene un perfil de 
consumo propio que el modelo aprenderá directamente del histórico, sin necesidad de 
features adicionales de categorización externa.

### Bivariante


```python
# ─── Bivariante categórico: boxplot + barplot mwh_total por variable ─────────
VARS_CAT_PLOT = {
    'hora':       [0, 6, 12, 18],
    'dia_semana': [1, 2, 3, 4, 5, 6, 7],
    'mes':        list(range(1, 13)),
    'anio':       [2019, 2020, 2021, 2022, 2023, 2024, 2025],
    'es_finde':   [0, 1],
    'es_festivo': [0, 1],
}

LABELS = {
    'hora':       ['00h', '06h', '12h', '18h'],
    'dia_semana': ['Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb', 'Dom'],
    'mes':        ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'],
    'anio':       ['2019','2020','2021','2022','2023','2024','2025'],
    'es_finde':   ['Laborable', 'Fin de semana'],
    'es_festivo': ['No festivo', 'Festivo'],
}

for var, valores in VARS_CAT_PLOT.items():
    grupos = [
        df.filter(pl.col(var) == v)['mwh_total'].drop_nulls().to_numpy()
        for v in valores
    ]
    labels = LABELS[var]

    fig, axes = plt.subplots(1, 2, figsize=(16, 4))

    # Barplot (media por grupo)
    medias = [g.mean() for g in grupos]
    axes[0].bar(labels, medias, color='#264653', edgecolor='white', alpha=0.85)
    axes[0].set_title(f'Media mwh_total por {var}', fontsize=10, fontweight='bold')
    axes[0].set_ylabel('MWh medio')
    axes[0].tick_params(axis='x', rotation=45, labelsize=8)

    # Boxplot
    bp = axes[1].boxplot(grupos, labels=labels, patch_artist=True,
                         showfliers=False,
                         medianprops=dict(color='#E9C46A', linewidth=2))
    for patch in bp['boxes']:
        patch.set_facecolor('#264653')
        patch.set_alpha(0.8)
    axes[1].set_title(f'Distribución mwh_total por {var}', fontsize=10, fontweight='bold')
    axes[1].set_ylabel('MWh')
    axes[1].tick_params(axis='x', rotation=45, labelsize=8)

    fig.suptitle(var, fontsize=12, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.show()
```

Patrones temporales

- Hora, valle en 00h (70,883 MWh) y pico en 12h (119,821 MWh), diferencia del 69%
  entre mínimo y máximo. Distribución perfectamente balanceada (25% por bloque),
  confirma grilla temporal completa.

- Dia semana, caída progresiva hacia el fin de semana: laborables ~105-106k MWh vs sábado 91k y domingo 86k. Diferencia de ~20k MWh entre miércoles y domingo, reflejo de la reducción de actividad industrial y de servicios.

- Mes, estacionalidad en U: pico en enero (110,564 MWh) y julio (118,150 MWh), valle en abril (88,877 MWh). Confirma doble estacionalidad por calefacción invernal y refrigeración estival.

- Anio, caída notable en 2020 (99,698 MWh) vs 2019 (113,277 MWh) por impacto COVID. Recuperación parcial en 2021-2022 y tendencia descendente desde 2023, posiblemente por mejoras de eficiencia energética o cambios en actividad económica.

- Fin de semana reduce el consumo medio un 11% (106,143 vs 94,058 MWh). Festivos reducen un 15% (101,517 vs 86,276 MWh), mayor impacto que el fin de semana, coherente con paralización de actividad industrial y comercial.

**Variables binarias de calendario:** `es_finde` y `es_festivo` actúan como 
amplificadores del patrón de día de semana. El fin de semana reduce un 11% el 
consumo medio, pero los festivos recortan un 15% adicional — mayor impacto porque 
paralizan simultáneamente industria, servicios y comercio, mientras que el sábado 
mantiene parte de la actividad de servicios. Ambas variables son estadísticamente 
significativas (Mann-Whitney, p < 0.05) y serán incluidas como features binarias en 
el modelo.

---

**Síntesis:** El análisis de variables categóricas confirma que `mwh_total` está 
gobernado por tres capas de señal superpuestas:

1. **Posición temporal intra-día** (`hora`): ciclo diurno con pico a las 12h y valle 
   nocturno, diferencia del 69% entre bloques.
2. **Ritmo semanal y festivo** (`dia_semana`, `es_finde`, `es_festivo`): actividad 
   industrial y de servicios marca la diferencia; domingos y festivos comprimen la 
   demanda de forma sistemática.
3. **Estacionalidad anual** (`mes`): doble pico verano-invierno, con los meses 
   intermedios (marzo-mayo) como zona de demanda mínima.

Sobre estas tres capas se superpone la señal estructural de `cod_postal`: un factor 
multiplicador fijo por barrio que determina el nivel absoluto de consumo, 
independiente del ciclo temporal. El modelo deberá capturar ambas dimensiones — 
la temporal y la espacial — para producir predicciones robustas a nivel de 
vecindario.

La tendencia descendente desde 2019 (`anio`) añade una cuarta señal de baja 
frecuencia que deberá absorberse vía lags de largo plazo o como variable de control, 
no como feature predictiva directa.


```python
# ─── Describe variables categóricas/temporales ──────────────────────────────
VARS_CAT = ['hora', 'dia_semana', 'mes', 'anio', 'es_finde', 'es_festivo']

for var in VARS_CAT:
    conteo = (
        df.group_by(var)
        .agg(
            pl.len().alias('n'),
            pl.col('mwh_total').mean().round(1).alias('mwh_medio')
        )
        .sort(var)
        .with_columns((pl.col('n') / len(df) * 100).round(2).alias('pct'))
    )
    print(f"\n{'='*50}")
    print(f"  {var}")
    print(f"{'='*50}")
    print(conteo)

# ─── Describe cod_postal ─────────────────────────────────────────────────────
print(f"\n{'='*50}")
print(f"  cod_postal")
print(f"{'='*50}")
print(
    df.filter(pl.col('nombre_postal').is_not_null())
    .group_by(['cod_postal', 'nombre_postal'])
    .agg([
        pl.len().alias('n'),
        pl.col('mwh_total').mean().round(1).alias('mwh_medio'),
        pl.col('mwh_total').median().round(1).alias('mwh_mediana'),
        pl.col('mwh_total').max().round(1).alias('mwh_max'),
        pl.col('mwh_industria').sum().alias('sum_ind'),
        pl.col('mwh_residencial').sum().alias('sum_res'),
        pl.col('mwh_servicios').sum().alias('sum_serv'),
    ])
    .with_columns([
        (pl.col('sum_ind') / (pl.col('sum_ind') + pl.col('sum_res') + pl.col('sum_serv')) * 100).round(1).alias('pct_ind'),
        (pl.col('sum_res') / (pl.col('sum_ind') + pl.col('sum_res') + pl.col('sum_serv')) * 100).round(1).alias('pct_res'),
        (pl.col('sum_serv') / (pl.col('sum_ind') + pl.col('sum_res') + pl.col('sum_serv')) * 100).round(1).alias('pct_serv'),
    ])
    .drop(['sum_ind', 'sum_res', 'sum_serv'])
    .sort('mwh_medio', descending=True)
)
```

### Varainza de barrios


```python
varianza_cp = (
    df.group_by("cod_postal")
    .agg([
        pl.col("mwh_total").std().alias("std"),
        pl.col("mwh_total").mean().alias("media"),
        pl.col("mwh_industria").sum().alias("sum_ind"),
        pl.col("mwh_residencial").sum().alias("sum_res"),
        pl.col("mwh_servicios").sum().alias("sum_serv"),
        pl.col("nombre_postal").first(),
    ])
    .with_columns([
        # Coeficiente de variación — std/media, comparable entre barrios de distinto volumen
        (pl.col("std") / pl.col("media") * 100).round(1).alias("cv_pct"),
        # Perfil sectorial
        (pl.col("sum_ind") / (pl.col("sum_ind") + pl.col("sum_res") + pl.col("sum_serv")) * 100).round(1).alias("pct_ind"),
        (pl.col("sum_res") / (pl.col("sum_ind") + pl.col("sum_res") + pl.col("sum_serv")) * 100).round(1).alias("pct_res"),
        (pl.col("sum_serv") / (pl.col("sum_ind") + pl.col("sum_res") + pl.col("sum_serv")) * 100).round(1).alias("pct_serv"),
    ])
    .select(["cod_postal", "nombre_postal", "media", "std", "cv_pct", "pct_ind", "pct_res", "pct_serv"])
    .sort("cv_pct", descending=True)
)

print(varianza_cp)
```

**Variabilidad por barrio (CV):** Los barrios con mayor volatilidad de consumo son 
dominados por servicios: Sant Antoni (08011, CV=72.4%, 60% serv), L'Antiga Esquerra 
(08036, CV=59.0%, 70% serv) y los tres CPs de Dreta de l'Eixample (08009, 08007, 
08008, CV=53-58%, ~68-70% serv). El patrón es consistente — los barrios de servicios 
concentran su consumo en bloques diurnos laborables y se desploman en festivos y 
madrugadas, generando una dispersión intrínsecamente alta.

En el extremo opuesto, los barrios industriales muestran los CV más bajos: La Verneda 
(08020, CV=22.1%) y previsiblemente Zona Franca y Montjuïc — la actividad industrial 
opera en turnos continuos que suavizan los picos y valles del calendario.

Esta heterogeneidad en volatilidad tiene implicaciones directas para el modelado: 
los barrios de servicios con CV alto serán más difíciles de predecir y concentrarán 
los errores más grandes. La métrica de detección de picos (tasa de detección vs 
falsas alarmas) será especialmente exigente en estos CPs.


```python
# ══════════════════════════════════════════════════════════════════════
# GRÁFICO 2 — Line plot mensual — 6 barrios seleccionados
# ══════════════════════════════════════════════════════════════════════
fig, ax = plt.subplots(figsize=(12, 5))

for cp, nombre in CPS.items():
    serie = (
        mensual.filter(pl.col("cod_postal") == cp)
        .sort("mes")
    )
    meses = serie["mes"].to_list()
    vals  = serie["mwh_medio"].to_list()
    ax.plot(
        meses, vals,
        marker="o", linewidth=2,
        color=COLORES[cp],
        label=f"{cp} · {nombre}"
    )

ax.set_xticks(range(1, 13))
ax.set_xticklabels(meses_label, fontsize=10)
ax.set_ylabel("MWh medio por bloque de 6h")
ax.set_title("Consumo mensual medio por barrio — 2019–2025", fontweight="bold")
ax.legend(loc="upper left", fontsize=9, framealpha=0.9)
ax.grid(axis="y", alpha=0.3, linewidth=0.5)
plt.tight_layout()
plt.show()
```

**Consumo mensual por barrio — 6 perfiles representativos:** El line plot confirma 
visualmente los dos regímenes del heatmap. Los industriales (08040 Zona Franca, 
08038 Montjuïc) mantienen consumo alto y relativamente estable todo el año con leve 
pico invernal — la producción continua amortigua la estacionalidad. Los barrios de 
servicios (08002 Barri Gòtic, 08036 Antiga Esquerra) muestran el pico de julio más 
pronunciado y una caída marcada en abril-mayo, patrón típico de climatización 
estival. Los residenciales (08032 El Carmel, 08033 Vallbona) siguen la curva en U 
clásica — picos en enero y julio — pero con amplitud mucho menor por su bajo 
volumen absoluto.

La separación vertical entre Zona Franca (~270k MWh en julio) y Vallbona (~30k MWh) 
reproduce gráficamente la heterogeneidad de 8x entre barrios ya documentada, y 
justifica que el modelo trate cada CP como una serie temporal independiente.

### Heat Map


```python
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

# ── Barrios seleccionados ────────────────────────────────────────────
CPS = {
    "08040": "Zona Franca",
    "08038": "Montjuïc",
    "08002": "Barri Gòtic",
    "08036": "Antiga Esquerra",
    "08032": "El Carmel",
    "08033": "Vallbona",
}

COLORES = {
    "08040": "#264653",
    "08038": "#2A9D8F",
    "08002": "#E9C46A",
    "08036": "#F4A261",
    "08032": "#E76F51",
    "08033": "#A8DADC",
}

# ── Datos agregados por mes ──────────────────────────────────────────
df_sel = df.filter(pl.col("cod_postal").is_in(list(CPS.keys())))

mensual = (
    df_sel
    .group_by(["cod_postal", "mes"])
    .agg(pl.col("mwh_total").mean().alias("mwh_medio"))
    .sort(["cod_postal", "mes"])
)

# ══════════════════════════════════════════════════════════════════════
# GRÁFICO 1 — Heatmap todos los barrios × mes (normalizado)
# ══════════════════════════════════════════════════════════════════════
todos_cp = (
    df.group_by(["cod_postal", "mes"])
    .agg(pl.col("mwh_total").mean().alias("mwh_medio"))
    .sort(["cod_postal", "mes"])
)

cps_order = (
    df.group_by("cod_postal")
    .agg(pl.col("mwh_total").mean().alias("mwh_global"))
    .sort("mwh_global", descending=True)
    ["cod_postal"].to_list()
)

matriz = np.zeros((len(cps_order), 12))
for i, cp in enumerate(cps_order):
    fila = todos_cp.filter(pl.col("cod_postal") == cp).sort("mes")
    vals = fila["mwh_medio"].to_numpy()
    if len(vals) == 12:
        vmin, vmax = vals.min(), vals.max()
        matriz[i] = (vals - vmin) / (vmax - vmin) if vmax > vmin else vals

nombres_cp = (
    df.select(["cod_postal", "nombre_postal"])
    .unique()
    .to_pandas()
    .set_index("cod_postal")["nombre_postal"]
    .to_dict()
)
labels_y = [f"{cp} · {(nombres_cp.get(cp) or '')[:18]}" for cp in cps_order]
meses_label = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"]

fig, ax = plt.subplots(figsize=(12, 13))
im = ax.imshow(matriz, aspect="auto", cmap="RdYlBu_r", vmin=0, vmax=1)
ax.set_xticks(range(12))
ax.set_xticklabels(meses_label, fontsize=10)
ax.set_yticks(range(len(cps_order)))
ax.set_yticklabels(labels_y, fontsize=8)
ax.set_title("Patrón estacional por barrio — consumo normalizado (0=mín, 1=máx)",
             fontweight="bold", pad=12)
plt.colorbar(im, ax=ax, label="Intensidad relativa", shrink=0.4)
plt.tight_layout()
plt.show()

x
```

**Patrón estacional por barrio:** El heatmap normalizado revela que no existe un 
único patrón estacional en Barcelona — hay al menos dos regímenes diferenciados. 
Los barrios de servicios (08036, 08002, 08013) concentran su pico en julio, 
respondiendo a la refrigeración estival. Los barrios con mayor componente industrial 
o portuaria (08040, 08038, 08039) muestran pico en enero, dominados por la demanda 
de calefacción y los turnos continuos de producción.

Un tercer grupo de barrios — Sant Antoni (08011), Pedralbes (08034) — presenta 
patrones fragmentados sin un mes dominante claro, coherente con los CV más altos 
identificados en el análisis de variabilidad.

Esta heterogeneidad estacional refuerza la necesidad de modelos por barrio o con 
`cod_postal` como variable categórica: un modelo global sin distinción espacial 
promediaría regímenes opuestos y perdería la señal estacional de ambos grupos.


```python
from matplotlib.colors import LinearSegmentedColormap

pivot = (
    df.group_by(['dia_semana', 'hora'])
    .agg(pl.col('mwh_total').mean().alias('mwh_medio'))
    .sort(['dia_semana', 'hora'])
)

pivot_pd = pivot.to_pandas().pivot(index='dia_semana', columns='hora', values='mwh_medio')
pivot_pd.index = ['Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb', 'Dom']
pivot_pd.columns = ['00h', '06h', '12h', '18h']

cmap_custom = LinearSegmentedColormap.from_list('uhi', [
    '#0d0221', '#6a0572', '#c1121f', '#ff6d00', '#ffea00'
])

fig, ax = plt.subplots(figsize=(8, 5))
im = ax.imshow(pivot_pd.values, aspect='auto', cmap=cmap_custom)
plt.colorbar(im, ax=ax, label='MWh medio')

ax.set_xticks(range(4))
ax.set_xticklabels(pivot_pd.columns)
ax.set_yticks(range(7))
ax.set_yticklabels(pivot_pd.index)

umbral = pivot_pd.values.max() * 0.65
for i in range(7):
    for j in range(4):
        val = pivot_pd.values[i, j]
        ax.text(j, i, f'{val:,.0f}',
                ha='center', va='center', fontsize=8,
                color='white' if val > umbral else '#ffea00')

ax.set_title('Consumo medio (MWh) — hora × día de semana', fontweight='bold')
ax.set_xlabel('Hora del bloque')
ax.set_ylabel('Día de la semana')
ax.set_facecolor('#0d0221')
plt.tight_layout()
plt.show()
```

El código postal actúa como variable proxy del perfil socioeconómico y de uso del suelo del barrio. La heterogeneidad entre CPs queda capturada implícitamente por el modelo sin necesidad de features adicionales de categorización.

**Interacción hora × día de semana:** El heatmap revela que la interacción entre 
ambas variables no es aditiva — el efecto del día de semana depende del bloque 
horario, y viceversa.

En el bloque nocturno (00h) el consumo es prácticamente constante a lo largo de 
toda la semana: rango de 69,111 (lunes) a 71,775 MWh (viernes), diferencia inferior 
al 4%. La actividad mínima de madrugada homogeniza los patrones independientemente 
del tipo de día.

A medida que avanza el día, la separación laborable–fin de semana se amplifica. En 
el bloque de 12h la diferencia entre miércoles (128,313 MWh) y domingo (96,857 MWh) 
alcanza los 31,456 MWh — una brecha del 24% que no existía a las 00h. Este efecto 
de amplificación diurna es la firma del consumo industrial y de servicios, que se 
concentra en horas centrales y desaparece los domingos.

El sábado presenta un perfil intermedio pero asimétrico: el bloque de 06h cae 
abruptamente (89,550 MWh vs ~111-112k los laborables), mientras que el de 12h 
recupera parcialmente (105,107 MWh). Refleja que la actividad comercial del sábado 
arranca más tarde y con menor intensidad que los días laborables.

Esta no-linealidad justifica incluir `hora_x_finde` (interacción hora × es_finde) 
como feature de ingeniería en la fase de modelado. Un modelo que reciba `hora` y 
`dia_semana` como variables independientes no capturará automáticamente que el 
perfil horario del domingo es cualitativamente diferente al del miércoles — necesita 
la interacción explícita para aprender ese comportamiento.

### Mapas


```python
import json
import plotly.express as px

with open('/home/app/src/data/BARCELONA.geojson', 'r') as f:
    geojson_bcn = json.load(f)

# ─── Consumo medio — animado por año ─────────────────────────────────────────
consumo_anio = (
    df.filter(pl.col('nombre_postal').is_not_null())
    .filter(pl.col('anio').is_not_null())
    .group_by(['cod_postal', 'nombre_postal', 'anio'])
    .agg(pl.col('mwh_total').mean().round(1).alias('mwh_medio'))
    .sort(['cod_postal', 'anio'])
    .with_columns(pl.col('anio').cast(pl.Utf8))
    .to_pandas()
)

fig = px.choropleth_map(
    consumo_anio,
    geojson=geojson_bcn,
    locations='cod_postal',
    featureidkey='properties.COD_POSTAL',
    color='mwh_medio',
    animation_frame='anio',
    color_continuous_scale='plasma',
    map_style='carto-darkmatter',
    zoom=11,
    center={'lat': 41.3874, 'lon': 2.1686},
    opacity=0.85,
    hover_name='nombre_postal',
    hover_data={'cod_postal': True, 'mwh_medio': ':.0f'},
    labels={'mwh_medio': 'MWh medio'},
    title='Consumo medio por código postal (MWh por bloque 6h)',
    range_color=[consumo_anio['mwh_medio'].min(), consumo_anio['mwh_medio'].quantile(0.95)]
)
fig.update_layout(
    height=700, width=1000,
    margin={'r': 0, 't': 50, 'l': 0, 'b': 0},
    title_font_size=16, title_font_color='white',
    paper_bgcolor='#1a1a2e',
    coloraxis_colorbar=dict(
        title=dict(text='MWh medio', font=dict(color='white')),
        thickness=15, len=0.6, tickfont=dict(color='white')
    )
)
fig.show()

# ─── LST MODIS — animado por año ─────────────────────────────────────────────
lst_cp = (
    df.filter(pl.col('nombre_postal').is_not_null())
    .filter(pl.col('lst_celsius').is_not_null())
    .filter(pl.col('anio').is_not_null())
    .group_by(['cod_postal', 'nombre_postal', 'anio'])
    .agg(pl.col('lst_celsius').mean().round(2).alias('lst_media'))
    .sort(['cod_postal', 'anio'])
    .with_columns(pl.col('anio').cast(pl.Utf8).alias('anio'))
    .to_pandas()
)

fig_lst = px.choropleth_map(
    lst_cp,
    geojson=geojson_bcn,
    locations='cod_postal',
    featureidkey='properties.COD_POSTAL',
    color='lst_media',
    animation_frame='anio',
    color_continuous_scale='RdYlBu_r',
    map_style='carto-darkmatter',
    zoom=11,
    center={'lat': 41.3874, 'lon': 2.1686},
    opacity=0.85,
    hover_name='nombre_postal',
    hover_data={'lst_media': ':.2f', 'anio': True},
    labels={'lst_media': 'LST media (°C)'},
    title='Temperatura superficial media (LST MODIS) por código postal',
    range_color=[lst_cp['lst_media'].quantile(0.10), lst_cp['lst_media'].quantile(0.95)]
)
fig_lst.update_layout(
    height=700, width=1000,
    margin={'r': 0, 't': 50, 'l': 0, 'b': 0},
    title_font_size=16, title_font_color='white',
    paper_bgcolor='#1a1a2e',
    coloraxis_colorbar=dict(
        title=dict(text='LST media (°C)', font=dict(color='white')),
        thickness=15, len=0.6, tickfont=dict(color='white')
    )
)
fig_lst.show()

# ─── Temperatura Meteocat — animada por año ───────────────────────────────────
temp_cp = (
    df.filter(pl.col('nombre_postal').is_not_null())
    .filter(pl.col('anio').is_not_null())
    .filter(pl.col('temp_mean').is_not_null())
    .group_by(['cod_postal', 'nombre_postal', 'anio'])
    .agg(pl.col('temp_mean').mean().round(2).alias('temp_media'))
    .sort(['cod_postal', 'anio'])
    .with_columns(pl.col('anio').cast(pl.Utf8).alias('anio'))
    .to_pandas()
)

fig_temp = px.choropleth_map(
    temp_cp,
    geojson=geojson_bcn,
    locations='cod_postal',
    featureidkey='properties.COD_POSTAL',
    color='temp_media',
    animation_frame='anio',
    color_continuous_scale='RdBu_r',
    map_style='carto-darkmatter',
    zoom=11,
    center={'lat': 41.3874, 'lon': 2.1686},
    opacity=0.85,
    hover_name='nombre_postal',
    hover_data={'temp_media': ':.2f', 'anio': True},
    labels={'temp_media': 'Temp media (°C)'},
    title='Temperatura media Meteocat por código postal',
    range_color=[temp_cp['temp_media'].min(), temp_cp['temp_media'].quantile(0.95)]
)
fig_temp.update_layout(
    height=700, width=1000,
    margin={'r': 0, 't': 50, 'l': 0, 'b': 0},
    title_font_size=16, title_font_color='white',
    paper_bgcolor='#1a1a2e',
    coloraxis_colorbar=dict(
        title=dict(text='Temp media (°C)', font=dict(color='white')),
        thickness=15, len=0.6, tickfont=dict(color='white')
    )
)
fig_temp.show()
```

### Tests estadísticos — Kruskal-Wallis y Mann-Whitney


```python
from scipy.stats import kruskal, mannwhitneyu

VARS_KW = ['hora', 'dia_semana', 'mes', 'anio']

print(f"{'variable':<15} {'H_stat':>12} {'p_value':>14} {'sig':>5}")
print('-' * 50)

for var in VARS_KW:
    grupos = [
        df.filter(pl.col(var) == v)['mwh_total'].drop_nulls().to_numpy()
        for v in sorted(df[var].drop_nulls().unique().to_list())
    ]
    h_stat, p_val = kruskal(*grupos)
    sig = '✓' if p_val < 0.05 else '✗'
    print(f"{var:<15} {h_stat:>12.3f} {p_val:>14.4e} {sig:>5}")

print(f"\n{'variable':<15} {'mediana_0':>12} {'mediana_1':>12} {'p_value':>14} {'sig':>5}")
print('-' * 60)

for var in ['es_finde', 'es_festivo']:
    g0 = df.filter(pl.col(var) == 0)['mwh_total'].drop_nulls().to_numpy()
    g1 = df.filter(pl.col(var) == 1)['mwh_total'].drop_nulls().to_numpy()
    stat, p_val = mannwhitneyu(g0, g1, alternative='two-sided')
    sig = '✓' if p_val < 0.05 else '✗'
    print(f"{var:<15} {np.median(g0):>12.1f} {np.median(g1):>12.1f} {p_val:>14.4e} {sig:>5}")

# Kruskal-Wallis: cod_postal
grupos_cp = [
    df.filter(pl.col('cod_postal') == cp)['mwh_total'].drop_nulls().to_numpy()
    for cp in sorted(df['cod_postal'].unique().to_list())
]
h_stat, p_val = kruskal(*grupos_cp)
print(f"\n{'variable':<15} {'H_stat':>12} {'p_value':>14} {'sig':>5}")
print('-' * 50)
print(f"{'cod_postal':<15} {h_stat:>12.3f} {p_val:>14.4e} {'✓' if p_val < 0.05 else '✗':>5}")
```

**Tests estadísticos:** Kruskal-Wallis confirma que las diferencias entre grupos son 
estadísticamente significativas para todas las variables categóricas (p < 0.001). 
`hora` registra el estadístico más alto (H=48,745), seguido de `cod_postal` 
(H=277,718) — aunque este último no es comparable directamente por tener 42 grupos 
vs 4 de `hora`. Mann-Whitney confirma `es_finde` y `es_festivo` con p≈0 y 
p=1.94e-198 respectivamente. Los patrones visuales observados no son ruido: todas 
las variables tienen poder discriminativo real sobre `mwh_total`.

---
## <font color='#4E8F6E'>  **Análisis de Correlación — Spearman** </font>


```python
VARS_CORR = ['mwh_total', 'temp_mean', 'temp_max', 'temp_min',
             'humedad_mean', 'viento_mean', 'precipitacion_sum',
             'irradiancia_mean', 'lst_celsius']

df_corr = df.select(VARS_CORR).drop_nulls().to_pandas()
corr_matrix = df_corr.corr(method='spearman')

fig, ax = plt.subplots(figsize=(10, 8))
sns.heatmap(
    corr_matrix,
    annot=True,
    fmt='.2f',
    cmap='RdBu_r',
    center=0,
    vmin=-1, vmax=1,
    ax=ax,
    linewidths=0.5
)
ax.set_title('Correlación de Spearman — variables numéricas', fontweight='bold')
plt.tight_layout()
plt.show()
```

**Correlación de Spearman — variables numéricas:** La correlación de `mwh_total` 
con las variables climáticas es baja pero significativa: temperatura (0.14–0.15), 
irradiancia (0.16) y MODIS LST (0.07). La débil correlación lineal con temperatura 
es esperada — la relación consumo-temperatura es en forma de U (más frío y más calor 
implican más consumo), no lineal, por lo que Spearman la subestima. XGBoost y LSTM 
capturarán esta no-linealidad sin necesidad de transformación.

El bloque de temperaturas muestra alta colinealidad interna: temp_mean, temp_max y 
temp_min correlacionan entre sí a 0.95–0.99, y lst_celsius correlaciona 0.87 con 
temp_mean. Esto confirma que no tiene sentido incluir las tres temperaturas 
simultáneamente — se usará `temp_mean` como representativa y `lst_celsius` como 
feature complementaria por su dimensión espacial (UHI por barrio), no como 
sustituta.

`humedad_mean` e `irradiancia_mean` aportan dimensiones independientes: correlación 
mutua de -0.38, sin colinealidad problemática con temperatura. Ambas entran al 
modelo. `precipitacion_sum` y `viento_mean` muestran correlaciones cercanas a 0 con 
`mwh_total` (-0.03 y -0.08) — se incluyen con baja prioridad y se revisará su 
aportación vía SHAP en la fase de modelado.
