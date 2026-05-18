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
import json
import plotly.express as px

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
print(f"Shape: {df.shape}")
print(f"Desde: {df['datetime'].min()}")
print(f"Hasta: {df['datetime'].max()}")
print(f"Códigos postales únicos: {df['cod_postal'].n_unique()}")
print(f"Años cubiertos: {sorted(df['anio'].unique().to_list())}")
```


```python
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

### <font color='#D4B93A'><b>Verificar estación X2</b></font>


```python
df.filter(pl.col('codi_estacio') == 'X2').select(
    pl.col(['temp_mean', 'temp_max', 'temp_min', 'humedad_mean', 'viento_mean', 'precipitacion_sum'])
).head(6)
```

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

### <font color='#D4B93A'><b>Detección mwh_total = 0</b></font>


```python
mwh_problematicos = df.filter(
    (pl.col('mwh_total') == 0) & (pl.col('anio') == 2025)
)
print(f"Registros con mwh_total = 0: {len(mwh_problematicos)}")
print(mwh_problematicos.select('datetime').unique().sort('datetime'))
```


```python
fechas_problema = ['2025-06-26', '2025-08-30', '2025-09-07']

df.filter(
    (pl.col('datetime').dt.date().cast(pl.Utf8).is_in(fechas_problema)) &
    (pl.col('datetime').dt.hour() == 18)
).select(['cod_postal', 'datetime', 'mwh_total']).head(10)
```


```python
mwh_problematicos.group_by('datetime') \
    .agg(pl.n_unique('cod_postal').alias('n_codigos')) \
    .sort('datetime')
```

> Se detectaron 378 registros con mwh_total = 0, concentrados en exactamente 3 fechas de 2025 (26-jun, 30-ago, 07-sep). En cada caso, los bloques 00:00, 06:00 y 12:00 de los 42 códigos postales reportaron cero simultáneamente, mientras el bloque 18:00 del mismo día registró valores normales. Patrón consistente con un fallo parcial de reporte en la fuente, no con un corte real de suministro.

### <font color='#D4B93A'><b>Verificar continuidad temporal</b></font>


```python
fechas_completas = pl.datetime_range(
    start=df['datetime'].min(),
    end=df['datetime'].max(),
    interval='6h',
    eager=True
).alias('datetime')

codigos_postales = df.select('cod_postal').unique()
fechas_esperadas = codigos_postales.join(fechas_completas.to_frame(), how='cross')

print(f"Registros esperados: {len(fechas_esperadas):,}")
print(f"Registros reales:    {len(df):,}")
print(f"Registros faltantes: {len(fechas_esperadas) - len(df):,}")

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

### <font color='#D4B93A'><b>Imputación: grilla temporal + sectores + mwh_total = 0</b></font>


```python
# ── 1. Reasignación AN → X2 (ya realizada en ETL) ────────────────────────────

# ── 2. Grilla temporal completa ───────────────────────────────────────────────
fechas_esperadas = df.select('cod_postal').unique().join(
    pl.datetime_range(df['datetime'].min(), df['datetime'].max(), interval='6h', eager=True)
      .alias('datetime').to_frame(), how='cross'
)
faltantes = fechas_esperadas.join(df.select(['cod_postal', 'datetime']), on=['cod_postal', 'datetime'], how='anti')
print(f"Faltantes detectados: {len(faltantes):,}")

docs_festivos = list(db['clean_festivos'].find({}, {'_id': 0}))
fechas_festivas = set(
    pl.DataFrame(docs_festivos)
    .with_columns(pl.col('fecha').str.to_date('%Y-%m-%d'))['fecha'].to_list()
)

referencia = (
    df.filter(pl.col('anio') < 2025)
    .group_by(['cod_postal', 'hora', 'dia_semana', 'mes'])
    .agg(pl.col('mwh_total').median().alias('mwh_total'))
)

df_faltantes = (
    faltantes
    .with_columns([
        pl.col('datetime').dt.hour().cast(pl.Int8).alias('hora'),
        pl.col('datetime').dt.weekday().cast(pl.Int8).alias('dia_semana'),
        pl.col('datetime').dt.month().cast(pl.Int8).alias('mes'),
    ])
    .join(referencia, on=['cod_postal', 'hora', 'dia_semana', 'mes'], how='left')
    .with_columns(
        pl.col('datetime').dt.date()
          .map_elements(lambda d: 1 if d in fechas_festivas else 0, return_dtype=pl.Int64)
          .alias('es_festivo')
    )
)

df = pl.concat([df, df_faltantes], how='diagonal_relaxed').sort(['cod_postal', 'hora', 'datetime'])
print(f"Shape: {df.shape} | Nulls mwh_total: {df['mwh_total'].null_count()}")
```


```python
# ── 3. Fill null sectores + recálculo mwh_total ───────────────────────────────
df = df.with_columns([
    pl.col('mwh_industria').fill_null(0),
    pl.col('mwh_residencial').fill_null(0),
    pl.col('mwh_servicios').fill_null(0),
])

df = df.with_columns(
    (pl.col('mwh_industria') + pl.col('mwh_residencial') +
     pl.col('mwh_servicios') + pl.col('mwh_no_especificado').fill_null(0)
    ).alias('mwh_total')
)

nulos = (
    df.null_count()
    .unpivot(variable_name='columna', value_name='nulos')
    .filter(pl.col('nulos') > 0)
    .with_columns((pl.col('nulos') / len(df) * 100).round(2).alias('pct'))
    .sort('nulos', descending=True)
)
print(f"Shape: {df.shape}")
print(nulos)
```


```python
# ── 4. Imputar mwh_total = 0 (3 fechas de fallo de reporte en 2025) ───────────
referencia_ceros = (
    df.filter((pl.col('anio') < 2025) & (pl.col('mwh_total') > 0))
    .group_by(['cod_postal', 'hora', 'dia_semana', 'mes'])
    .agg(pl.col('mwh_total').median().alias('mwh_ref'))
)

df = (
    df.join(referencia_ceros, on=['cod_postal', 'hora', 'dia_semana', 'mes'], how='left')
    .with_columns(
        pl.when(pl.col('mwh_total') == 0)
          .then(pl.col('mwh_ref'))
          .otherwise(pl.col('mwh_total'))
          .alias('mwh_total')
    )
    .drop('mwh_ref')
)

print(f"Ceros restantes: {df.filter(pl.col('mwh_total') == 0).shape[0]}")
print(f"Nulls mwh_total: {df['mwh_total'].null_count()}")
```


```python
# ── 5. Verificar consistencia mwh_total == suma sectores ─────────────────────
check = df.with_columns([
    (pl.col('mwh_industria') + pl.col('mwh_residencial') +
     pl.col('mwh_servicios') + pl.col('mwh_no_especificado').fill_null(0)
    ).alias('suma_sectores')
]).select([
    (pl.col('mwh_total') - pl.col('suma_sectores')).alias('diferencia')
]).filter(pl.col('diferencia') != 0).shape

print(f"Registros con inconsistencia: {check[0]}")
print("(Los ~598 corresponden a registros imputados con mediana histórica — deuda técnica documentada)")
```

> **Resumen del tratamiento de nulos:**
>
> - **Reasignación AN → X2** — los registros de la estación AN (100% nulos, Zoo de Barcelona) se reasignaron a X2 y se rellenaron con datos reales de Meteocat para temp y humedad (2019–2023).
>
> - **Sectores de consumo** — `mwh_industria`, `mwh_residencial` y `mwh_servicios` nulos imputados a 0 (sector no activo ese bloque). `mwh_total` recalculado como suma de los 4 sectores.
>
> - **Continuidad temporal** — 220 bloques añadidos: 52 del CP 08011 (7–20 ago 2025, fallo de reporte específico) y 168 del 19-ago-2025 global (42 CPs × 4 bloques). Imputados con mediana histórica por CP + hora + día semana + mes (2019–2024).
>
> - **mwh_total = 0** — 378 registros en 3 fechas de 2025 (26-jun, 30-ago, 07-sep). Fallo parcial de reporte en OpenData. Imputados con mediana histórica.
>
> - **mwh_no_especificado** — 85.35% nulos estructurales. Se verificó que mwh_total == suma sectores en el 99.9% de registros. A partir de sep-2025 aumenta a ~14,000 MWh por cambio metodológico en Open Data BCN — no afecta mwh_total. Variable excluida del modelado.
>
> - **Nulos meteorológicos restantes** — viento e irradiancia (X2 sin sensores), temp/humedad 2024–2025 (X2 inactiva). Estructurales y conocidos, se mantienen para feature engineering.

Limitación conocida: los ~598 registros imputados con mediana histórica presentan inconsistencia con sus sectores componentes (permanecen en 0). Al representar el 0.14% del dataset y no usarse los sectores como features del modelo, el impacto en el forecasting es despreciable. Se documenta como deuda técnica para versiones futuras.

---
# <font color='#4E8F6E'>  **Análisis Descriptivo** </font>

## <font color='#4E8F6E'>  **Variable Objetivo: mwh_total** </font>


```python
df.select('mwh_total').describe()
```

- Media (101,467 MWh) > mediana (90,608 MWh), sesgo positivo confirmado.
- IQR: 57,120 a 132,708 MWh, rango de consumo normal entre barrios y bloques horarios.
- Mínimo 130 MWh, registros de madrugada en barrios pequeños, valor plausible.
- Máximo 1,486,114 MWh — outlier extremo a revisar.
- Std (59,563) representa el 59% de la media, alta dispersión esperada dado que mezcla 42 CPs con perfiles muy distintos.


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

> La distribución de mwh_total presenta sesgo positivo con cola derecha pronunciada. La mediana está muy por debajo de la media, indicando que la mayoría de bloques tienen consumo moderado pero existen picos elevados de alta magnitud.

> No sigue distribución normal — se usará **Spearman** para correlaciones y **Kruskal-Wallis / Mann-Whitney** para tests de hipótesis.

#### Outliers

Detección con umbral de 5× la mediana por CP.


```python
umbrales = df.group_by('cod_postal').agg(pl.col('mwh_total').median().alias('mediana'))

print(df.join(umbrales, on='cod_postal')
    .filter(pl.col('mwh_total') > pl.col('mediana') * 5)
    .group_by('cod_postal')
    .agg(pl.len().alias('n_registros'))
    .sort('n_registros', descending=True))

casos = {
    '08030 — Sostenido (may 2023)': df.filter(
        (pl.col('cod_postal') == '08030') &
        (pl.col('datetime') >= pl.datetime(2023, 5, 5)) &
        (pl.col('datetime') < pl.datetime(2023, 5, 12))
    ).select(['datetime', 'mwh_total', 'mwh_industria']).sort('datetime'),
    '08037 — Puntual (nov/dic 2024, feb 2025)': df.filter(
        (pl.col('cod_postal') == '08037') &
        ((pl.col('datetime') == pl.datetime(2024, 11, 20, 0)) |
         (pl.col('datetime') == pl.datetime(2024, 12, 22, 0)) |
         (pl.col('datetime') == pl.datetime(2025, 2, 24, 0)))
    ).select(['datetime', 'mwh_total', 'mwh_servicios']),
    '08022 — Parcial (may 2023)': df.filter(
        (pl.col('cod_postal') == '08022') &
        (pl.col('datetime') >= pl.datetime(2023, 5, 29)) &
        (pl.col('datetime') < pl.datetime(2023, 5, 31))
    ).select(['datetime', 'mwh_total', 'mwh_residencial']).sort('datetime'),
}
for nombre, resultado in casos.items():
    print(f"\n{'='*60}\n  {nombre}\n{'='*60}")
    print(resultado)

print("\n" + "="*60 + "\n  Valores > 400k\n" + "="*60)
print(df.filter(pl.col('mwh_total') > 400000)
    .select(['datetime', 'cod_postal', 'nombre_postal', 'mwh_total'])
    .sort('mwh_total', descending=True).head(10))
```

> Tras imputar los 520 registros erróneos con mediana histórica (2022-2024), la distribución muestra una forma log-normal coherente con el comportamiento esperado del consumo eléctrico urbano. La mayoría de bloques se concentran entre 20k–150k MWh, con una cola derecha moderada que refleja los picos reales de demanda estival.

## Descomposición de consumo energético


```python
import matplotlib.dates as mdates

serie_global = (
    df.group_by('datetime')
    .agg([
        pl.col('mwh_industria').sum(),
        pl.col('mwh_residencial').sum(),
        pl.col('mwh_servicios').sum(),
    ])
    .sort('datetime')
)

fechas = serie_global['datetime'].to_numpy()
ind    = serie_global['mwh_industria'].to_numpy()
res    = serie_global['mwh_residencial'].to_numpy()
ser    = serie_global['mwh_servicios'].to_numpy()

fig, ax = plt.subplots(figsize=(24, 5))
ax.stackplot(fechas, ind, res, ser,
             labels=['Industria', 'Residencial', 'Servicios'],
             colors=['#264653', '#2A9D8F', '#E9C46A'],
             alpha=0.8)
ax.set_title('Consumo por sector — Barcelona (todos los CPs)', fontweight='bold')
ax.set_ylabel('MWh')
ax.legend(loc='upper right')
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
plt.xticks(rotation=45, ha='right', fontsize=8)
plt.tight_layout()
plt.show()
```

> El consumo eléctrico de Barcelona muestra una estacionalidad anual clara con picos en verano (julio–agosto) y en invierno (diciembre–enero). Los servicios dominan el consumo en todos los períodos, seguidos del residencial. Se observa una caída notable en 2020 (COVID-19) y una recuperación progresiva hasta 2022. A partir de 2023 el consumo se estabiliza con niveles ligeramente inferiores al período pre-pandemia.

#### Mix sectorial por código postal


```python
(
    df.group_by('cod_postal')
    .agg([
        pl.col('mwh_industria').sum(),
        pl.col('mwh_residencial').sum(),
        pl.col('mwh_servicios').sum(),
        pl.col('mwh_no_especificado').sum(),
    ])
    .with_columns([
        (pl.col('mwh_industria') / (pl.col('mwh_industria') + pl.col('mwh_residencial') + pl.col('mwh_servicios')) * 100).round(1).alias('pct_industria'),
        (pl.col('mwh_residencial') / (pl.col('mwh_industria') + pl.col('mwh_residencial') + pl.col('mwh_servicios')) * 100).round(1).alias('pct_residencial'),
        (pl.col('mwh_servicios') / (pl.col('mwh_industria') + pl.col('mwh_residencial') + pl.col('mwh_servicios')) * 100).round(1).alias('pct_servicios'),
    ])
    .select(['cod_postal', 'pct_industria', 'pct_residencial', 'pct_servicios'])
    .sort('pct_industria', descending=True)
)
```

Ningún código postal tiene un perfil puro. Servicios domina en 38 de 42 CPs. La industria solo es relevante en 3-4 CPs (08039, 08004, 08038). Residencial alcanza su máximo en zonas periféricas como 08032 (70%).

#### Serie Temporal de mwh


```python
(
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

#### Decomposición STL — 4 barrios representativos


```python
CPS = {
    '08002': 'Barri Gòtic (Servicios/Turístico)',
    '08005': 'El Poblenou (Mixto)',
    '08032': 'El Carmel / El Guinardó (Residencial)',
    '08038': 'Montjuïc / Zona Franca (Industrial)',
}

cp = '08002'
serie_cp = (
    df.filter(pl.col('cod_postal') == cp)
    .sort('datetime')
    .select(['datetime', 'mwh_total'])
)

fechas     = serie_cp['datetime'].to_numpy()
serie_vals = serie_cp['mwh_total'].to_numpy()

res = STL(serie_vals, period=28, robust=True).fit()

fig, axes = plt.subplots(4, 1, figsize=(16, 10), sharex=True)
fig.suptitle(f"Descomposición STL — CP {cp} ({CPS[cp]})", fontweight='bold')

for ax, (nombre_comp, vals) in zip(axes, {
    'Serie original': serie_vals,
    'Tendencia':      res.trend,
    'Estacionalidad': res.seasonal,
    'Residuo':        res.resid,
}.items()):
    ax.plot(fechas, vals, color='#264653', linewidth=0.6)
    ax.set_ylabel(nombre_comp, fontsize=9)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))

plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
```


```python
for cp, nombre in {k: v for k, v in CPS.items() if k != '08002'}.items():
    serie_cp = (
        df.filter(pl.col('cod_postal') == cp)
        .sort('datetime')
        .select(['datetime', 'mwh_total'])
    )
    fechas     = serie_cp['datetime'].to_numpy()
    serie_vals = serie_cp['mwh_total'].to_numpy()
    res = STL(serie_vals, period=28, robust=True).fit()
    fig, axes = plt.subplots(4, 1, figsize=(16, 10), sharex=True)
    fig.suptitle(f"Descomposición STL — CP {cp} ({nombre})", fontweight='bold')
    for ax, (nombre_comp, vals) in zip(axes, {
        'Serie original': serie_vals,
        'Tendencia':      res.trend,
        'Estacionalidad': res.seasonal,
        'Residuo':        res.resid,
    }.items()):
        ax.plot(fechas, vals, color='#264653', linewidth=0.6)
        ax.set_ylabel(nombre_comp, fontsize=9)
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
```

### ADF Y KPSS

- **ADF:** Augmented Dickey-Fuller. H0: la serie NO es estacionaria.
- **KPSS:** H0: la serie SÍ es estacionaria.

Se usan juntos porque se contradicen — si ambos coinciden la conclusión es clara.


```python
from arch.unitroot import ADF, KPSS
import pandas as pd

resultados = []

for cp, nombre in CPS.items():
    serie = (
        df.filter(pl.col('cod_postal') == cp)
        .sort('datetime')['mwh_total']
        .to_numpy()
    )
    adf   = ADF(serie, method='aic')
    kpss_ = KPSS(serie)
    resultados.append({
        'CP': cp,
        'Barrio': nombre,
        'ADF stat': round(adf.stat, 3),
        'ADF p-valor': round(adf.pvalue, 4),
        'ADF estacionaria': 'Si' if adf.pvalue < 0.05 else 'No',
        'KPSS stat': round(kpss_.stat, 3),
        'KPSS p-valor': round(kpss_.pvalue, 4),
        'KPSS estacionaria': 'Si' if kpss_.pvalue > 0.05 else 'No',
    })

pd.DataFrame(resultados)
```

**ADF** rechaza H0 en los 4 casos (p < 0.05) → las series son estacionarias en media.

**KPSS** rechaza H0 (p < 0.05) → la dispersión cambia según el período (COVID-2020, recuperación 2022, estabilización posterior) → no estacionarias en varianza.

La combinación es coherente: nivel promedio estable pero con cambios estructurales en variabilidad.
- XGBoost y LSTM/GRU son robustos a esta condición.
- SARIMA requerirá diferenciación estacional para cumplir sus supuestos.

### ACF/PACF


```python
serie = (
    df.group_by('datetime')
    .agg(pl.col('mwh_total').sum())
    .sort('datetime')['mwh_total']
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

axes[0].vlines(lags, 0, acf_vals, color='#1B3A5C', linewidth=0.8)
axes[0].axhline(0,   color='black', linewidth=0.5)
axes[0].axhline(ci,  color='gray',  linestyle='--', linewidth=0.8)
axes[0].axhline(-ci, color='gray',  linestyle='--', linewidth=0.8)
axes[0].axvline(x=4,  color='#E07B5A', linestyle='--', alpha=0.7, label='lag 4 = 24h')
axes[0].axvline(x=28, color='#2A9D8F', linestyle='--', alpha=0.7, label='lag 28 = 7 dias')
axes[0].set_title('ACF — Barcelona Global', fontweight='bold')
axes[0].set_xlabel('Lag (bloques de 6h)')
axes[0].legend()

axes[1].vlines(lags, 0, pacf_vals, color='#1B3A5C', linewidth=0.8)
axes[1].axhline(0,   color='black', linewidth=0.5)
axes[1].axhline(ci,  color='gray',  linestyle='--', linewidth=0.8)
axes[1].axhline(-ci, color='gray',  linestyle='--', linewidth=0.8)
axes[1].axvline(x=4,  color='#E07B5A', linestyle='--', alpha=0.7, label='lag 4 = 24h')
axes[1].axvline(x=28, color='#2A9D8F', linestyle='--', alpha=0.7, label='lag 28 = 7 dias')
axes[1].set_title('PACF — Barcelona Global', fontweight='bold')
axes[1].set_xlabel('Lag (bloques de 6h)')
axes[1].legend()

plt.tight_layout()
plt.show()
```

Los lags más informativos para el modelo son **lag_1, lag_4 y lag_28**, validando empíricamente los features de rezago planificados para XGBoost.
- **Lag 4 — 24h (ACF 0.83, PACF 0.75):** el mismo bloque horario del día anterior es el predictor más fuerte individual.
- **Lag 28 — 7 días (ACF 0.87):** el patrón semanal domina sobre el diario.
- **Lag 1 — 6h (PACF 0.37):** relación directa moderada con el bloque inmediatamente anterior.

## <font color='#4E8F6E'>  **Variables Explicativas Numéricas** </font>

#### <font color='#D4B93A'><b>Revisión de nulos</b></font>


```python
VARS_NUM = ['mwh_industria', 'mwh_residencial', 'mwh_servicios', 'mwh_no_especificado',
            'lst_celsius', 'temp_mean', 'temp_max', 'temp_min',
            'humedad_mean', 'viento_mean', 'precipitacion_sum', 'irradiancia_mean']

df.select(VARS_NUM + ['mwh_total']).null_count()
```

**mwh_no_especificado:** 85.35% nulos estructurales. Se verificó que mwh_total == suma sectores en el 99.9% de registros. A partir de sep-2025 aumenta a ~14,000 MWh por cambio metodológico en Open Data BCN — no afecta mwh_total. Variable excluida del análisis descriptivo y del modelado.

Los nulos meteorológicos restantes son estructurales y conocidos — se mantienen para feature engineering.

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
    ax2.tick_params(axis='x', rotation=45, labelsize=7)

plt.tight_layout()
plt.show()
```

#### QQ-Plots — Test de Normalidad


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

Ninguna variable sigue distribución normal. Conclusión: se descarta Pearson para correlaciones, se usará **Spearman**. Para tests de hipótesis, **Kruskal-Wallis y Mann-Whitney** en lugar de ANOVA.

#### <font color='#D4B93A'><b>Análisis Bivariante</b></font>


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

#### Test Mann-Whitney — variables numéricas vs mwh_total


```python
from scipy.stats import mannwhitneyu

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

**hora:** Patrón diario consistente y pronunciado. El mínimo se produce en madrugada (00h: 70,883 MWh) y el pico en mediodía (12h: 119,821 MWh), con caída moderada en tarde (18h: 108,910 MWh). La diferencia entre mínimo y máximo es del 69%. El boxplot confirma que la variabilidad también aumenta en los bloques diurnos — el IQR de 12h es casi el doble que el de 00h. Kruskal-Wallis H=48,745 (p ≈ 0).

**dia_semana:** Consumo estable de lunes a viernes (~105,000–106,600 MWh) con caída progresiva hacia el fin de semana (sábado: 91,468, domingo: 85,891 MWh). La reducción del domingo respecto al pico laboral es del 19%. Kruskal-Wallis H=5,982 (p ≈ 0).

**mes:** Patrón bimodal claro — pico estival en julio (118,150 MWh) y pico invernal en enero (110,564 MWh), con valles en primavera (abril: 88,877 MWh) y otoño (octubre: 94,170 MWh). El boxplot de julio muestra el IQR más amplio del año, confirmando que el verano no solo consume más sino que genera mayor variabilidad entre barrios. Kruskal-Wallis H=6,251 (p ≈ 0).

**anio:** Caída notable en 2020 (99,698 MWh, −12% vs 2019) por COVID-19. Recuperación gradual hasta 2022 (103,058 MWh) sin recuperar el nivel pre-pandemia. A partir de 2023 el consumo se estabiliza ligeramente por debajo de 2022, posiblemente por eficiencia energética estructural y teletrabajo. El boxplot muestra que la variabilidad también se redujo post-COVID. Los 220 registros null corresponden a bloques imputados por gaps de continuidad.

**es_finde:** Días laborables consumen un 13% más que fines de semana (106,143 vs 94,058 MWh). El boxplot muestra además menor variabilidad en fin de semana — IQR más estrecho — coherente con la ausencia de actividad industrial y comercial. Mann-Whitney p ≈ 0.

**es_festivo:** Los festivos reducen el consumo un 15% respecto a días no festivos (86,276 vs 101,517 MWh), efecto más pronunciado que el fin de semana. Solo el 4% de los registros son festivos. Mann-Whitney p ≈ 0.

**Código postal:** El rango de consumo medio entre barrios es extremo — de 29,683 MWh (08033 Vallbona/Ciutat Meridiana) a 233,425 MWh (08040 La Zona Franca), una diferencia de 8x entre mínimo y máximo. Confirmado estadísticamente por Kruskal-Wallis (H=277,718, p ≈ 0), es la variable más discriminante del dataset.

Los CPs con mayor componente industrial lideran el consumo: La Zona Franca (08040, 29% ind.), El Bon Pastor (08030, 16% ind.) y Montjuïc/Zona Franca (08038, 37% ind.). En el extremo opuesto, los barrios periféricos residenciales: Vallbona (08033, 57% res.), El Carmel (08032, 70% res.) y Vilapicina (08031, 64% res.).

Tres patrones anómalos merecen atención:
- **08037 Vila de Gràcia:** media baja (42,294 MWh) pero mwh_max de 142,210 MWh — señal directa de los outliers ya imputados.
- **08009 Dreta de l'Eixample:** media baja (46,049 MWh) pero mwh_max de 321,545 MWh. No es outlier problemático — el perfil de servicios dominante (~59%) genera picos reales en el bloque 12h durante enero y octubre.
- **08036 L'Antiga Esquerra:** mediana (96,368 MWh) muy por debajo de la media (112,190 MWh), distribución sesgada por picos extremos de verano. El 70% de servicios explica la concentración de consumo en bloques diurnos estivales.

La heterogeneidad entre CPs justifica tratar cod_postal como variable categórica en el modelo. Cada CP tiene un perfil de consumo propio que el modelo aprenderá directamente del histórico, sin necesidad de features adicionales de categorización externa. El código postal actúa como proxy implícito del perfil socioeconómico y de uso del suelo del barrio.

### Bivariante categórico


```python
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

    medias = [g.mean() for g in grupos]
    axes[0].bar(labels, medias, color='#264653', edgecolor='white', alpha=0.85)
    axes[0].set_title(f'Media mwh_total por {var}', fontsize=10, fontweight='bold')
    axes[0].set_ylabel('MWh medio')
    axes[0].tick_params(axis='x', rotation=45, labelsize=8)

    bp = axes[1].boxplot(grupos, tick_labels=labels, patch_artist=True,
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

### Heatmap hora × día de semana


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

---
## <font color='#4E8F6E'>  **Mapas Geoespaciales** </font>

> Mapas coropléticos animados por año. Animación mensual disponible ejecutando `src/scripts/generar_mapas.py` externamente.


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
