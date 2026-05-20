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




<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (6, 30)</small><table border="1" class="dataframe"><thead><tr><th>cod_postal</th><th>nombre_postal</th><th>centroide_lon</th><th>centroide_lat</th><th>codi_estacio</th><th>nombre_estacio</th><th>estacio_lon</th><th>estacio_lat</th><th>datetime</th><th>mwh_total</th><th>mwh_industria</th><th>mwh_residencial</th><th>mwh_servicios</th><th>mwh_no_especificado</th><th>lst_celsius</th><th>temp_mean</th><th>temp_max</th><th>temp_min</th><th>humedad_mean</th><th>viento_mean</th><th>precipitacion_sum</th><th>irradiancia_mean</th><th>es_festivo</th><th>nombre_local</th><th>hora</th><th>dia_semana</th><th>mes</th><th>anio</th><th>semana_anio</th><th>es_finde</th></tr><tr><td>str</td><td>str</td><td>f64</td><td>f64</td><td>str</td><td>str</td><td>f64</td><td>f64</td><td>datetime[μs]</td><td>i64</td><td>i64</td><td>i64</td><td>i64</td><td>i64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>i64</td><td>str</td><td>i64</td><td>i64</td><td>i64</td><td>i64</td><td>i64</td><td>i64</td></tr></thead><tbody><tr><td>&quot;08001&quot;</td><td>&quot;Las Ramblas / El Raval&quot;</td><td>2.171735</td><td>41.38032</td><td>&quot;X4&quot;</td><td>&quot;Barcelona - el Raval&quot;</td><td>2.1656</td><td>41.3793</td><td>2025-01-01 00:00:00</td><td>70044</td><td>287</td><td>32270</td><td>37487</td><td>null</td><td>11.45</td><td>10.183333</td><td>10.9</td><td>9.4</td><td>77.833333</td><td>1.058333</td><td>0.0</td><td>-2.0</td><td>1</td><td>&quot;Año Nuevo&quot;</td><td>0</td><td>3</td><td>1</td><td>2025</td><td>1</td><td>0</td></tr><tr><td>&quot;08001&quot;</td><td>&quot;Las Ramblas / El Raval&quot;</td><td>2.171735</td><td>41.38032</td><td>&quot;X4&quot;</td><td>&quot;Barcelona - el Raval&quot;</td><td>2.1656</td><td>41.3793</td><td>2025-01-01 06:00:00</td><td>84422</td><td>366</td><td>38319</td><td>45737</td><td>null</td><td>11.45</td><td>11.25</td><td>14.2</td><td>9.1</td><td>70.0</td><td>0.841667</td><td>0.0</td><td>176.083333</td><td>1</td><td>&quot;Año Nuevo&quot;</td><td>6</td><td>3</td><td>1</td><td>2025</td><td>1</td><td>0</td></tr><tr><td>&quot;08001&quot;</td><td>&quot;Las Ramblas / El Raval&quot;</td><td>2.171735</td><td>41.38032</td><td>&quot;X4&quot;</td><td>&quot;Barcelona - el Raval&quot;</td><td>2.1656</td><td>41.3793</td><td>2025-01-01 12:00:00</td><td>106367</td><td>417</td><td>53880</td><td>52070</td><td>null</td><td>11.45</td><td>12.916667</td><td>13.5</td><td>11.9</td><td>70.083333</td><td>1.083333</td><td>0.0</td><td>94.916667</td><td>1</td><td>&quot;Año Nuevo&quot;</td><td>12</td><td>3</td><td>1</td><td>2025</td><td>1</td><td>0</td></tr><tr><td>&quot;08001&quot;</td><td>&quot;Las Ramblas / El Raval&quot;</td><td>2.171735</td><td>41.38032</td><td>&quot;X4&quot;</td><td>&quot;Barcelona - el Raval&quot;</td><td>2.1656</td><td>41.3793</td><td>2025-01-01 18:00:00</td><td>103864</td><td>342</td><td>54490</td><td>49032</td><td>null</td><td>11.45</td><td>11.036364</td><td>11.5</td><td>10.6</td><td>74.818182</td><td>1.1</td><td>0.0</td><td>-3.0</td><td>1</td><td>&quot;Año Nuevo&quot;</td><td>18</td><td>3</td><td>1</td><td>2025</td><td>1</td><td>0</td></tr><tr><td>&quot;08002&quot;</td><td>&quot;Barri Gòtic&quot;</td><td>2.174238</td><td>41.382826</td><td>&quot;X4&quot;</td><td>&quot;Barcelona - el Raval&quot;</td><td>2.1656</td><td>41.3793</td><td>2025-01-01 00:00:00</td><td>63203</td><td>788</td><td>20754</td><td>41661</td><td>null</td><td>11.44</td><td>10.183333</td><td>10.9</td><td>9.4</td><td>77.833333</td><td>1.058333</td><td>0.0</td><td>-2.0</td><td>1</td><td>&quot;Año Nuevo&quot;</td><td>0</td><td>3</td><td>1</td><td>2025</td><td>1</td><td>0</td></tr><tr><td>&quot;08002&quot;</td><td>&quot;Barri Gòtic&quot;</td><td>2.174238</td><td>41.382826</td><td>&quot;X4&quot;</td><td>&quot;Barcelona - el Raval&quot;</td><td>2.1656</td><td>41.3793</td><td>2025-01-01 06:00:00</td><td>74533</td><td>1024</td><td>23581</td><td>49928</td><td>null</td><td>11.44</td><td>11.25</td><td>14.2</td><td>9.1</td><td>70.0</td><td>0.841667</td><td>0.0</td><td>176.083333</td><td>1</td><td>&quot;Año Nuevo&quot;</td><td>6</td><td>3</td><td>1</td><td>2025</td><td>1</td><td>0</td></tr></tbody></table></div>




```python
df.tail(6)
```




<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (6, 30)</small><table border="1" class="dataframe"><thead><tr><th>cod_postal</th><th>nombre_postal</th><th>centroide_lon</th><th>centroide_lat</th><th>codi_estacio</th><th>nombre_estacio</th><th>estacio_lon</th><th>estacio_lat</th><th>datetime</th><th>mwh_total</th><th>mwh_industria</th><th>mwh_residencial</th><th>mwh_servicios</th><th>mwh_no_especificado</th><th>lst_celsius</th><th>temp_mean</th><th>temp_max</th><th>temp_min</th><th>humedad_mean</th><th>viento_mean</th><th>precipitacion_sum</th><th>irradiancia_mean</th><th>es_festivo</th><th>nombre_local</th><th>hora</th><th>dia_semana</th><th>mes</th><th>anio</th><th>semana_anio</th><th>es_finde</th></tr><tr><td>str</td><td>str</td><td>f64</td><td>f64</td><td>str</td><td>str</td><td>f64</td><td>f64</td><td>datetime[μs]</td><td>i64</td><td>i64</td><td>i64</td><td>i64</td><td>i64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>i64</td><td>str</td><td>i64</td><td>i64</td><td>i64</td><td>i64</td><td>i64</td><td>i64</td></tr></thead><tbody><tr><td>&quot;08041&quot;</td><td>&quot;Sant Andreu&quot;</td><td>2.174434</td><td>41.418288</td><td>&quot;AN&quot;</td><td>&quot;Barcelona - Parc de la Ciutade…</td><td>2.1859</td><td>41.3907</td><td>2019-12-31 12:00:00</td><td>66201</td><td>1767</td><td>45480</td><td>18954</td><td>null</td><td>13.18</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>0</td><td>null</td><td>12</td><td>2</td><td>12</td><td>2019</td><td>1</td><td>0</td></tr><tr><td>&quot;08041&quot;</td><td>&quot;Sant Andreu&quot;</td><td>2.174434</td><td>41.418288</td><td>&quot;AN&quot;</td><td>&quot;Barcelona - Parc de la Ciutade…</td><td>2.1859</td><td>41.3907</td><td>2019-12-31 18:00:00</td><td>61506</td><td>1394</td><td>46538</td><td>13574</td><td>null</td><td>13.18</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>0</td><td>null</td><td>18</td><td>2</td><td>12</td><td>2019</td><td>1</td><td>0</td></tr><tr><td>&quot;08042&quot;</td><td>&quot;Torre Baró / Nou Barris&quot;</td><td>2.172027</td><td>41.444412</td><td>&quot;AN&quot;</td><td>&quot;Barcelona - Parc de la Ciutade…</td><td>2.1859</td><td>41.3907</td><td>2019-12-31 00:00:00</td><td>41996</td><td>759</td><td>22909</td><td>18328</td><td>null</td><td>12.21</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>0</td><td>null</td><td>0</td><td>2</td><td>12</td><td>2019</td><td>1</td><td>0</td></tr><tr><td>&quot;08042&quot;</td><td>&quot;Torre Baró / Nou Barris&quot;</td><td>2.172027</td><td>41.444412</td><td>&quot;AN&quot;</td><td>&quot;Barcelona - Parc de la Ciutade…</td><td>2.1859</td><td>41.3907</td><td>2019-12-31 06:00:00</td><td>69314</td><td>1058</td><td>31561</td><td>36695</td><td>null</td><td>12.21</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>0</td><td>null</td><td>6</td><td>2</td><td>12</td><td>2019</td><td>1</td><td>0</td></tr><tr><td>&quot;08042&quot;</td><td>&quot;Torre Baró / Nou Barris&quot;</td><td>2.172027</td><td>41.444412</td><td>&quot;AN&quot;</td><td>&quot;Barcelona - Parc de la Ciutade…</td><td>2.1859</td><td>41.3907</td><td>2019-12-31 12:00:00</td><td>84494</td><td>879</td><td>45909</td><td>37706</td><td>null</td><td>12.21</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>0</td><td>null</td><td>12</td><td>2</td><td>12</td><td>2019</td><td>1</td><td>0</td></tr><tr><td>&quot;08042&quot;</td><td>&quot;Torre Baró / Nou Barris&quot;</td><td>2.172027</td><td>41.444412</td><td>&quot;AN&quot;</td><td>&quot;Barcelona - Parc de la Ciutade…</td><td>2.1859</td><td>41.3907</td><td>2019-12-31 18:00:00</td><td>82006</td><td>778</td><td>48307</td><td>32921</td><td>null</td><td>12.21</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>0</td><td>null</td><td>18</td><td>2</td><td>12</td><td>2019</td><td>1</td><td>0</td></tr></tbody></table></div>




```python
df.filter(pl.col("codi_estacio") == "X2").select(pl.col(["temp_mean", "temp_max", "temp_min", "humedad_mean", "viento_mean", "precipitacion_sum"])).head(6)
```




<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (6, 6)</small><table border="1" class="dataframe"><thead><tr><th>temp_mean</th><th>temp_max</th><th>temp_min</th><th>humedad_mean</th><th>viento_mean</th><th>precipitacion_sum</th></tr><tr><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td></tr></thead><tbody><tr><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td></tr><tr><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td></tr><tr><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td></tr><tr><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td></tr><tr><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td></tr><tr><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td></tr></tbody></table></div>




```python
print(f"Shape: {df.shape}")
print(f"Desde: {df['datetime'].min()}")
print(f"Hasta: {df['datetime'].max()}")
print(f"Códigos postales únicos: {df['cod_postal'].n_unique()}")
print(f"Años cubiertos: {sorted(df['anio'].unique().to_list())}")

```

    Shape: (424148, 30)
    Desde: 2019-01-01 00:00:00
    Hasta: 2025-11-30 18:00:00
    Códigos postales únicos: 42
    Años cubiertos: [2019, 2020, 2021, 2022, 2023, 2024, 2025]



```python
# Tipos de datos
print("SCHEMA")
for col, dtype in zip(df.columns, df.dtypes):
    print(f"  {col:<30} {dtype}")

```

    SCHEMA
      cod_postal                     String
      nombre_postal                  String
      centroide_lon                  Float64
      centroide_lat                  Float64
      codi_estacio                   String
      nombre_estacio                 String
      estacio_lon                    Float64
      estacio_lat                    Float64
      datetime                       Datetime(time_unit='us', time_zone=None)
      mwh_total                      Int64
      mwh_industria                  Int64
      mwh_residencial                Int64
      mwh_servicios                  Int64
      mwh_no_especificado            Int64
      lst_celsius                    Float64
      temp_mean                      Float64
      temp_max                       Float64
      temp_min                       Float64
      humedad_mean                   Float64
      viento_mean                    Float64
      precipitacion_sum              Float64
      irradiancia_mean               Float64
      es_festivo                     Int64
      nombre_local                   String
      hora                           Int64
      dia_semana                     Int64
      mes                            Int64
      anio                           Int64
      semana_anio                    Int64
      es_finde                       Int64



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

    shape: (12, 3)
    ┌─────────────────────┬────────┬───────────┐
    │ columna             ┆ nulos  ┆ pct_nulos │
    │ ---                 ┆ ---    ┆ ---       │
    │ str                 ┆ i64    ┆ f64       │
    ╞═════════════════════╪════════╪═══════════╡
    │ nombre_local        ┆ 407184 ┆ 96.0      │
    │ mwh_no_especificado ┆ 362004 ┆ 85.35     │
    │ lst_celsius         ┆ 168824 ┆ 39.8      │
    │ viento_mean         ┆ 153201 ┆ 36.12     │
    │ irradiancia_mean    ┆ 153196 ┆ 36.12     │
    │ temp_mean           ┆ 136527 ┆ 32.19     │
    │ temp_max            ┆ 136527 ┆ 32.19     │
    │ temp_min            ┆ 136527 ┆ 32.19     │
    │ humedad_mean        ┆ 136516 ┆ 32.19     │
    │ precipitacion_sum   ┆ 136428 ┆ 32.17     │
    │ mwh_industria       ┆ 2444   ┆ 0.58      │
    │ mwh_residencial     ┆ 40     ┆ 0.01      │
    └─────────────────────┴────────┴───────────┘


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

    shape: (7, 6)
    ┌──────┬───────────────────┬──────────────┬────────────┬───────────────┬─────────────────┐
    │ anio ┆ nulos_irradiancia ┆ nulos_viento ┆ nulos_temp ┆ nulos_humedad ┆ total_registros │
    │ ---  ┆ ---               ┆ ---          ┆ ---        ┆ ---           ┆ ---             │
    │ i64  ┆ u32               ┆ u32          ┆ u32        ┆ u32           ┆ u32             │
    ╞══════╪═══════════════════╪══════════════╪════════════╪═══════════════╪═════════════════╡
    │ 2019 ┆ 21900             ┆ 21900        ┆ 18980      ┆ 18980         ┆ 61320           │
    │ 2020 ┆ 22504             ┆ 22504        ┆ 19616      ┆ 19616         ┆ 61488           │
    │ 2021 ┆ 22440             ┆ 22440        ┆ 19560      ┆ 19560         ┆ 61320           │
    │ 2022 ┆ 22116             ┆ 22123        ┆ 19219      ┆ 19219         ┆ 61320           │
    │ 2023 ┆ 21900             ┆ 21900        ┆ 18980      ┆ 18980         ┆ 61320           │
    │ 2024 ┆ 22094             ┆ 22078        ┆ 19923      ┆ 19919         ┆ 61488           │
    │ 2025 ┆ 20242             ┆ 20256        ┆ 20249      ┆ 20242         ┆ 55892           │
    └──────┴───────────────────┴──────────────┴────────────┴───────────────┴─────────────────┘


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

    shape: (42, 4)
    ┌────────────┬─────────────────────────────────┬──────────────┬──────────────┐
    │ cod_postal ┆ nombre_postal                   ┆ nulos_viento ┆ codi_estacio │
    │ ---        ┆ ---                             ┆ ---          ┆ ---          │
    │ str        ┆ str                             ┆ u32          ┆ str          │
    ╞════════════╪═════════════════════════════════╪══════════════╪══════════════╡
    │ 08019      ┆ El Besòs i el Maresme           ┆ 10100        ┆ AN           │
    │ 08005      ┆ El Poblenou                     ┆ 10100        ┆ X2           │
    │ 08013      ┆ La Sagrada Família              ┆ 10100        ┆ AN           │
    │ 08025      ┆ El Guinardó                     ┆ 10100        ┆ AN           │
    │ 08042      ┆ Torre Baró / Nou Barris         ┆ 10100        ┆ AN           │
    │ 08009      ┆ Dreta de l'Eixample             ┆ 10100        ┆ AN           │
    │ 08003      ┆ La Barceloneta                  ┆ 10100        ┆ X2           │
    │ 08016      ┆ Nou Barris                      ┆ 10100        ┆ AN           │
    │ 08027      ┆ La Sagrera                      ┆ 10100        ┆ AN           │
    │ 08041      ┆ Sant Andreu                     ┆ 10100        ┆ AN           │
    │ 08026      ┆ El Clot / Camp de l'Arpa        ┆ 10100        ┆ AN           │
    │ 08030      ┆ El Bon Pastor / Sant Andreu     ┆ 10100        ┆ AN           │
    │ 08018      ┆ Fort Pienc                      ┆ 10100        ┆ AN           │
    │ 08010      ┆ Pl. Catalunya / Gran Via        ┆ 10100        ┆ AN           │
    │ 08020      ┆ La Verneda                      ┆ 10100        ┆ AN           │
    │ 08022      ┆ Les Tres Torres / Bonanova      ┆ 79           ┆ D5           │
    │ 08032      ┆ El Carmel / El Guinardó         ┆ 79           ┆ D5           │
    │ 08023      ┆ Vallcarca i els Penitents       ┆ 79           ┆ D5           │
    │ 08021      ┆ Sant Gervasi - Galvany          ┆ 79           ┆ D5           │
    │ 08035      ┆ Sant Genís dels Agudells / Val… ┆ 79           ┆ D5           │
    │ 08033      ┆ Vallbona / Ciutat Meridiana     ┆ 79           ┆ D5           │
    │ 08031      ┆ Vilapicina / Torre Llobeta      ┆ 79           ┆ D5           │
    │ 08036      ┆ L'Antiga Esquerra de l'Eixampl… ┆ 58           ┆ X4           │
    │ 08011      ┆ Sant Antoni                     ┆ 58           ┆ X4           │
    │ 08008      ┆ Dreta de l'Eixample             ┆ 58           ┆ X4           │
    │ 08001      ┆ Las Ramblas / El Raval          ┆ 58           ┆ X4           │
    │ 08039      ┆ El Port / La Barceloneta        ┆ 58           ┆ X4           │
    │ 08006      ┆ Sarrià - Sant Gervasi           ┆ 58           ┆ X4           │
    │ 08012      ┆ Vila de Gràcia                  ┆ 58           ┆ X4           │
    │ 08038      ┆ Montjuïc / Zona Franca          ┆ 58           ┆ X4           │
    │ 08024      ┆ Gràcia Nova                     ┆ 58           ┆ X4           │
    │ 08015      ┆ Esquerra de l'Eixample          ┆ 58           ┆ X4           │
    │ 08002      ┆ Barri Gòtic                     ┆ 58           ┆ X4           │
    │ 08004      ┆ Montjuïc / Poble Sec            ┆ 58           ┆ X4           │
    │ 08037      ┆ Vila de Gràcia                  ┆ 58           ┆ X4           │
    │ 08029      ┆ Nova Esquerra de l'Eixample     ┆ 58           ┆ X4           │
    │ 08040      ┆ La Zona Franca                  ┆ 58           ┆ X4           │
    │ 08007      ┆ Dreta de l'Eixample / Pg. de G… ┆ 58           ┆ X4           │
    │ 08014      ┆ Sants - Montjuïc                ┆ 55           ┆ X8           │
    │ 08034      ┆ Pedralbes / Sarrià              ┆ 55           ┆ X8           │
    │ 08028      ┆ Zona Universitària / Les Corts  ┆ 55           ┆ X8           │
    │ 08017      ┆ Sarrià - Sant Gervasi           ┆ 55           ┆ X8           │
    └────────────┴─────────────────────────────────┴──────────────┴──────────────┘


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




<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (35, 6)</small><table border="1" class="dataframe"><thead><tr><th>codi_estacio</th><th>anio</th><th>total_registros</th><th>nulos_temp</th><th>nulos_viento</th><th>nulos_irradiancia</th></tr><tr><td>str</td><td>i64</td><td>u32</td><td>u32</td><td>u32</td><td>u32</td></tr></thead><tbody><tr><td>&quot;AN&quot;</td><td>2019</td><td>18980</td><td>18980</td><td>18980</td><td>18980</td></tr><tr><td>&quot;AN&quot;</td><td>2020</td><td>19032</td><td>19032</td><td>19032</td><td>19032</td></tr><tr><td>&quot;AN&quot;</td><td>2021</td><td>18980</td><td>18980</td><td>18980</td><td>18980</td></tr><tr><td>&quot;AN&quot;</td><td>2022</td><td>18980</td><td>18980</td><td>18980</td><td>18980</td></tr><tr><td>&quot;AN&quot;</td><td>2023</td><td>18980</td><td>18980</td><td>18980</td><td>18980</td></tr><tr><td>&quot;AN&quot;</td><td>2024</td><td>19032</td><td>19032</td><td>19032</td><td>19032</td></tr><tr><td>&quot;AN&quot;</td><td>2025</td><td>17316</td><td>17316</td><td>17316</td><td>17316</td></tr><tr><td>&quot;D5&quot;</td><td>2019</td><td>10220</td><td>0</td><td>0</td><td>0</td></tr><tr><td>&quot;D5&quot;</td><td>2020</td><td>10248</td><td>140</td><td>140</td><td>140</td></tr><tr><td>&quot;D5&quot;</td><td>2021</td><td>10220</td><td>140</td><td>140</td><td>140</td></tr><tr><td>&quot;D5&quot;</td><td>2022</td><td>10220</td><td>63</td><td>63</td><td>56</td></tr><tr><td>&quot;D5&quot;</td><td>2023</td><td>10220</td><td>0</td><td>0</td><td>0</td></tr><tr><td>&quot;D5&quot;</td><td>2024</td><td>10248</td><td>35</td><td>14</td><td>14</td></tr><tr><td>&quot;D5&quot;</td><td>2025</td><td>9324</td><td>189</td><td>196</td><td>182</td></tr><tr><td>&quot;X2&quot;</td><td>2019</td><td>2920</td><td>0</td><td>2920</td><td>2920</td></tr><tr><td>&quot;X2&quot;</td><td>2020</td><td>2928</td><td>40</td><td>2928</td><td>2928</td></tr><tr><td>&quot;X2&quot;</td><td>2021</td><td>2920</td><td>40</td><td>2920</td><td>2920</td></tr><tr><td>&quot;X2&quot;</td><td>2022</td><td>2920</td><td>16</td><td>2920</td><td>2920</td></tr><tr><td>&quot;X2&quot;</td><td>2023</td><td>2920</td><td>0</td><td>2920</td><td>2920</td></tr><tr><td>&quot;X2&quot;</td><td>2024</td><td>2928</td><td>720</td><td>2928</td><td>2928</td></tr><tr><td>&quot;X2&quot;</td><td>2025</td><td>2664</td><td>2664</td><td>2664</td><td>2664</td></tr><tr><td>&quot;X4&quot;</td><td>2019</td><td>23360</td><td>0</td><td>0</td><td>0</td></tr><tr><td>&quot;X4&quot;</td><td>2020</td><td>23424</td><td>320</td><td>320</td><td>320</td></tr><tr><td>&quot;X4&quot;</td><td>2021</td><td>23360</td><td>320</td><td>320</td><td>320</td></tr><tr><td>&quot;X4&quot;</td><td>2022</td><td>23360</td><td>128</td><td>128</td><td>128</td></tr><tr><td>&quot;X4&quot;</td><td>2023</td><td>23360</td><td>0</td><td>0</td><td>0</td></tr><tr><td>&quot;X4&quot;</td><td>2024</td><td>23424</td><td>96</td><td>96</td><td>80</td></tr><tr><td>&quot;X4&quot;</td><td>2025</td><td>21260</td><td>64</td><td>64</td><td>64</td></tr><tr><td>&quot;X8&quot;</td><td>2019</td><td>5840</td><td>0</td><td>0</td><td>0</td></tr><tr><td>&quot;X8&quot;</td><td>2020</td><td>5856</td><td>84</td><td>84</td><td>84</td></tr><tr><td>&quot;X8&quot;</td><td>2021</td><td>5840</td><td>80</td><td>80</td><td>80</td></tr><tr><td>&quot;X8&quot;</td><td>2022</td><td>5840</td><td>32</td><td>32</td><td>32</td></tr><tr><td>&quot;X8&quot;</td><td>2023</td><td>5840</td><td>0</td><td>0</td><td>0</td></tr><tr><td>&quot;X8&quot;</td><td>2024</td><td>5856</td><td>40</td><td>8</td><td>40</td></tr><tr><td>&quot;X8&quot;</td><td>2025</td><td>5328</td><td>16</td><td>16</td><td>16</td></tr></tbody></table></div>



#### Estaciones con mejor cobertura


```python
df.group_by("codi_estacio").agg([
    pl.len().alias("total"),
    pl.col("temp_mean").is_null().sum().alias("nulos_temp"),
    (pl.col("temp_mean").is_null().sum() / pl.len() * 100).round(2).alias("pct_nulos_temp")
]).sort("pct_nulos_temp")
```




<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (5, 4)</small><table border="1" class="dataframe"><thead><tr><th>codi_estacio</th><th>total</th><th>nulos_temp</th><th>pct_nulos_temp</th></tr><tr><td>str</td><td>u32</td><td>u32</td><td>f64</td></tr></thead><tbody><tr><td>&quot;X4&quot;</td><td>161548</td><td>928</td><td>0.57</td></tr><tr><td>&quot;X8&quot;</td><td>40400</td><td>252</td><td>0.62</td></tr><tr><td>&quot;D5&quot;</td><td>70700</td><td>567</td><td>0.8</td></tr><tr><td>&quot;X2&quot;</td><td>20200</td><td>3480</td><td>17.23</td></tr><tr><td>&quot;AN&quot;</td><td>131300</td><td>131300</td><td>100.0</td></tr></tbody></table></div>



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

    Registros con mwh_total mayor o igual a 0: 378


    Duplicados datetime + cod_postal: 0


> Existen 378 filas donde no existen valores iguales o menores a 0 en el mwh_total lo cual se puede tratar de un problema de recolección de datos


```python
print(mwh_problematicos.select([
    "cod_postal", "nombre_postal", "datetime",
    "mwh_total", "mwh_industria", "mwh_residencial",
    "mwh_servicios", "mwh_no_especificado"
]))
```

    shape: (378, 8)
    ┌────────────┬────────────┬────────────┬───────────┬───────────┬───────────┬───────────┬───────────┐
    │ cod_postal ┆ nombre_pos ┆ datetime   ┆ mwh_total ┆ mwh_indus ┆ mwh_resid ┆ mwh_servi ┆ mwh_no_es │
    │ ---        ┆ tal        ┆ ---        ┆ ---       ┆ tria      ┆ encial    ┆ cios      ┆ pecificad │
    │ str        ┆ ---        ┆ datetime[μ ┆ i64       ┆ ---       ┆ ---       ┆ ---       ┆ o         │
    │            ┆ str        ┆ s]         ┆           ┆ i64       ┆ i64       ┆ i64       ┆ ---       │
    │            ┆            ┆            ┆           ┆           ┆           ┆           ┆ i64       │
    ╞════════════╪════════════╪════════════╪═══════════╪═══════════╪═══════════╪═══════════╪═══════════╡
    │ 08001      ┆ Las        ┆ 2025-06-26 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Ramblas /  ┆ 00:00:00   ┆           ┆           ┆           ┆           ┆           │
    │            ┆ El Raval   ┆            ┆           ┆           ┆           ┆           ┆           │
    │ 08001      ┆ Las        ┆ 2025-06-26 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Ramblas /  ┆ 06:00:00   ┆           ┆           ┆           ┆           ┆           │
    │            ┆ El Raval   ┆            ┆           ┆           ┆           ┆           ┆           │
    │ 08001      ┆ Las        ┆ 2025-06-26 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Ramblas /  ┆ 12:00:00   ┆           ┆           ┆           ┆           ┆           │
    │            ┆ El Raval   ┆            ┆           ┆           ┆           ┆           ┆           │
    │ 08002      ┆ Barri      ┆ 2025-06-26 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Gòtic      ┆ 00:00:00   ┆           ┆           ┆           ┆           ┆           │
    │ 08002      ┆ Barri      ┆ 2025-06-26 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Gòtic      ┆ 06:00:00   ┆           ┆           ┆           ┆           ┆           │
    │ 08002      ┆ Barri      ┆ 2025-06-26 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Gòtic      ┆ 12:00:00   ┆           ┆           ┆           ┆           ┆           │
    │ 08003      ┆ La Barcelo ┆ 2025-06-26 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ neta       ┆ 00:00:00   ┆           ┆           ┆           ┆           ┆           │
    │ 08003      ┆ La Barcelo ┆ 2025-06-26 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ neta       ┆ 06:00:00   ┆           ┆           ┆           ┆           ┆           │
    │ 08003      ┆ La Barcelo ┆ 2025-06-26 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ neta       ┆ 12:00:00   ┆           ┆           ┆           ┆           ┆           │
    │ 08004      ┆ Montjuïc / ┆ 2025-06-26 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Poble Sec  ┆ 00:00:00   ┆           ┆           ┆           ┆           ┆           │
    │ 08004      ┆ Montjuïc / ┆ 2025-06-26 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Poble Sec  ┆ 06:00:00   ┆           ┆           ┆           ┆           ┆           │
    │ 08004      ┆ Montjuïc / ┆ 2025-06-26 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Poble Sec  ┆ 12:00:00   ┆           ┆           ┆           ┆           ┆           │
    │ 08005      ┆ El         ┆ 2025-06-26 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Poblenou   ┆ 00:00:00   ┆           ┆           ┆           ┆           ┆           │
    │ 08005      ┆ El         ┆ 2025-06-26 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Poblenou   ┆ 06:00:00   ┆           ┆           ┆           ┆           ┆           │
    │ 08005      ┆ El         ┆ 2025-06-26 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Poblenou   ┆ 12:00:00   ┆           ┆           ┆           ┆           ┆           │
    │ 08006      ┆ Sarrià -   ┆ 2025-06-26 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Sant       ┆ 00:00:00   ┆           ┆           ┆           ┆           ┆           │
    │            ┆ Gervasi    ┆            ┆           ┆           ┆           ┆           ┆           │
    │ 08006      ┆ Sarrià -   ┆ 2025-06-26 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Sant       ┆ 06:00:00   ┆           ┆           ┆           ┆           ┆           │
    │            ┆ Gervasi    ┆            ┆           ┆           ┆           ┆           ┆           │
    │ 08006      ┆ Sarrià -   ┆ 2025-06-26 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Sant       ┆ 12:00:00   ┆           ┆           ┆           ┆           ┆           │
    │            ┆ Gervasi    ┆            ┆           ┆           ┆           ┆           ┆           │
    │ 08007      ┆ Dreta de   ┆ 2025-06-26 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ l'Eixample ┆ 00:00:00   ┆           ┆           ┆           ┆           ┆           │
    │            ┆ / Pg. de   ┆            ┆           ┆           ┆           ┆           ┆           │
    │            ┆ G…         ┆            ┆           ┆           ┆           ┆           ┆           │
    │ 08007      ┆ Dreta de   ┆ 2025-06-26 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ l'Eixample ┆ 06:00:00   ┆           ┆           ┆           ┆           ┆           │
    │            ┆ / Pg. de   ┆            ┆           ┆           ┆           ┆           ┆           │
    │            ┆ G…         ┆            ┆           ┆           ┆           ┆           ┆           │
    │ 08007      ┆ Dreta de   ┆ 2025-06-26 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ l'Eixample ┆ 12:00:00   ┆           ┆           ┆           ┆           ┆           │
    │            ┆ / Pg. de   ┆            ┆           ┆           ┆           ┆           ┆           │
    │            ┆ G…         ┆            ┆           ┆           ┆           ┆           ┆           │
    │ 08008      ┆ Dreta de   ┆ 2025-06-26 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ l'Eixample ┆ 00:00:00   ┆           ┆           ┆           ┆           ┆           │
    │ 08008      ┆ Dreta de   ┆ 2025-06-26 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ l'Eixample ┆ 06:00:00   ┆           ┆           ┆           ┆           ┆           │
    │ 08008      ┆ Dreta de   ┆ 2025-06-26 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ l'Eixample ┆ 12:00:00   ┆           ┆           ┆           ┆           ┆           │
    │ 08009      ┆ Dreta de   ┆ 2025-06-26 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ l'Eixample ┆ 00:00:00   ┆           ┆           ┆           ┆           ┆           │
    │ …          ┆ …          ┆ …          ┆ …         ┆ …         ┆ …         ┆ …         ┆ …         │
    │ 08034      ┆ Pedralbes  ┆ 2025-09-07 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ / Sarrià   ┆ 12:00:00   ┆           ┆           ┆           ┆           ┆           │
    │ 08035      ┆ Sant Genís ┆ 2025-09-07 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ dels       ┆ 00:00:00   ┆           ┆           ┆           ┆           ┆           │
    │            ┆ Agudells / ┆            ┆           ┆           ┆           ┆           ┆           │
    │            ┆ Val…       ┆            ┆           ┆           ┆           ┆           ┆           │
    │ 08035      ┆ Sant Genís ┆ 2025-09-07 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ dels       ┆ 06:00:00   ┆           ┆           ┆           ┆           ┆           │
    │            ┆ Agudells / ┆            ┆           ┆           ┆           ┆           ┆           │
    │            ┆ Val…       ┆            ┆           ┆           ┆           ┆           ┆           │
    │ 08035      ┆ Sant Genís ┆ 2025-09-07 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ dels       ┆ 12:00:00   ┆           ┆           ┆           ┆           ┆           │
    │            ┆ Agudells / ┆            ┆           ┆           ┆           ┆           ┆           │
    │            ┆ Val…       ┆            ┆           ┆           ┆           ┆           ┆           │
    │ 08036      ┆ L'Antiga   ┆ 2025-09-07 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Esquerra   ┆ 00:00:00   ┆           ┆           ┆           ┆           ┆           │
    │            ┆ de         ┆            ┆           ┆           ┆           ┆           ┆           │
    │            ┆ l'Eixampl… ┆            ┆           ┆           ┆           ┆           ┆           │
    │ 08036      ┆ L'Antiga   ┆ 2025-09-07 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Esquerra   ┆ 06:00:00   ┆           ┆           ┆           ┆           ┆           │
    │            ┆ de         ┆            ┆           ┆           ┆           ┆           ┆           │
    │            ┆ l'Eixampl… ┆            ┆           ┆           ┆           ┆           ┆           │
    │ 08036      ┆ L'Antiga   ┆ 2025-09-07 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Esquerra   ┆ 12:00:00   ┆           ┆           ┆           ┆           ┆           │
    │            ┆ de         ┆            ┆           ┆           ┆           ┆           ┆           │
    │            ┆ l'Eixampl… ┆            ┆           ┆           ┆           ┆           ┆           │
    │ 08037      ┆ Vila de    ┆ 2025-09-07 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Gràcia     ┆ 00:00:00   ┆           ┆           ┆           ┆           ┆           │
    │ 08037      ┆ Vila de    ┆ 2025-09-07 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Gràcia     ┆ 06:00:00   ┆           ┆           ┆           ┆           ┆           │
    │ 08037      ┆ Vila de    ┆ 2025-09-07 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Gràcia     ┆ 12:00:00   ┆           ┆           ┆           ┆           ┆           │
    │ 08038      ┆ Montjuïc / ┆ 2025-09-07 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Zona       ┆ 00:00:00   ┆           ┆           ┆           ┆           ┆           │
    │            ┆ Franca     ┆            ┆           ┆           ┆           ┆           ┆           │
    │ 08038      ┆ Montjuïc / ┆ 2025-09-07 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Zona       ┆ 06:00:00   ┆           ┆           ┆           ┆           ┆           │
    │            ┆ Franca     ┆            ┆           ┆           ┆           ┆           ┆           │
    │ 08038      ┆ Montjuïc / ┆ 2025-09-07 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Zona       ┆ 12:00:00   ┆           ┆           ┆           ┆           ┆           │
    │            ┆ Franca     ┆            ┆           ┆           ┆           ┆           ┆           │
    │ 08039      ┆ El Port /  ┆ 2025-09-07 ┆ 0         ┆ null      ┆ 0         ┆ 0         ┆ null      │
    │            ┆ La Barcelo ┆ 00:00:00   ┆           ┆           ┆           ┆           ┆           │
    │            ┆ neta       ┆            ┆           ┆           ┆           ┆           ┆           │
    │ 08039      ┆ El Port /  ┆ 2025-09-07 ┆ 0         ┆ null      ┆ 0         ┆ 0         ┆ null      │
    │            ┆ La Barcelo ┆ 06:00:00   ┆           ┆           ┆           ┆           ┆           │
    │            ┆ neta       ┆            ┆           ┆           ┆           ┆           ┆           │
    │ 08039      ┆ El Port /  ┆ 2025-09-07 ┆ 0         ┆ null      ┆ 0         ┆ 0         ┆ null      │
    │            ┆ La Barcelo ┆ 12:00:00   ┆           ┆           ┆           ┆           ┆           │
    │            ┆ neta       ┆            ┆           ┆           ┆           ┆           ┆           │
    │ 08040      ┆ La Zona    ┆ 2025-09-07 ┆ 0         ┆ null      ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Franca     ┆ 00:00:00   ┆           ┆           ┆           ┆           ┆           │
    │ 08040      ┆ La Zona    ┆ 2025-09-07 ┆ 0         ┆ null      ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Franca     ┆ 06:00:00   ┆           ┆           ┆           ┆           ┆           │
    │ 08040      ┆ La Zona    ┆ 2025-09-07 ┆ 0         ┆ null      ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Franca     ┆ 12:00:00   ┆           ┆           ┆           ┆           ┆           │
    │ 08041      ┆ Sant       ┆ 2025-09-07 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Andreu     ┆ 00:00:00   ┆           ┆           ┆           ┆           ┆           │
    │ 08041      ┆ Sant       ┆ 2025-09-07 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Andreu     ┆ 06:00:00   ┆           ┆           ┆           ┆           ┆           │
    │ 08041      ┆ Sant       ┆ 2025-09-07 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ Andreu     ┆ 12:00:00   ┆           ┆           ┆           ┆           ┆           │
    │ 08042      ┆ Torre Baró ┆ 2025-09-07 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ / Nou      ┆ 00:00:00   ┆           ┆           ┆           ┆           ┆           │
    │            ┆ Barris     ┆            ┆           ┆           ┆           ┆           ┆           │
    │ 08042      ┆ Torre Baró ┆ 2025-09-07 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ / Nou      ┆ 06:00:00   ┆           ┆           ┆           ┆           ┆           │
    │            ┆ Barris     ┆            ┆           ┆           ┆           ┆           ┆           │
    │ 08042      ┆ Torre Baró ┆ 2025-09-07 ┆ 0         ┆ 0         ┆ 0         ┆ 0         ┆ null      │
    │            ┆ / Nou      ┆ 12:00:00   ┆           ┆           ┆           ┆           ┆           │
    │            ┆ Barris     ┆            ┆           ┆           ┆           ┆           ┆           │
    └────────────┴────────────┴────────────┴───────────┴───────────┴───────────┴───────────┴───────────┘



```python
print(mwh_problematicos.select("datetime").unique().sort("datetime"))
```

    shape: (9, 1)
    ┌─────────────────────┐
    │ datetime            │
    │ ---                 │
    │ datetime[μs]        │
    ╞═════════════════════╡
    │ 2025-06-26 00:00:00 │
    │ 2025-06-26 06:00:00 │
    │ 2025-06-26 12:00:00 │
    │ 2025-08-30 00:00:00 │
    │ 2025-08-30 06:00:00 │
    │ 2025-08-30 12:00:00 │
    │ 2025-09-07 00:00:00 │
    │ 2025-09-07 06:00:00 │
    │ 2025-09-07 12:00:00 │
    └─────────────────────┘



```python
fechas_problema = ["2025-06-26", "2025-08-30", "2025-09-07"]

df.filter(
    (pl.col("datetime").dt.date().cast(pl.Utf8).is_in(fechas_problema)) &
    (pl.col("datetime").dt.hour() == 18)
).select(["cod_postal", "datetime", "mwh_total"]).head(10)
```




<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (10, 3)</small><table border="1" class="dataframe"><thead><tr><th>cod_postal</th><th>datetime</th><th>mwh_total</th></tr><tr><td>str</td><td>datetime[μs]</td><td>i64</td></tr></thead><tbody><tr><td>&quot;08001&quot;</td><td>2025-06-26 18:00:00</td><td>9128</td></tr><tr><td>&quot;08002&quot;</td><td>2025-06-26 18:00:00</td><td>5176</td></tr><tr><td>&quot;08003&quot;</td><td>2025-06-26 18:00:00</td><td>9266</td></tr><tr><td>&quot;08004&quot;</td><td>2025-06-26 18:00:00</td><td>8587</td></tr><tr><td>&quot;08005&quot;</td><td>2025-06-26 18:00:00</td><td>8008</td></tr><tr><td>&quot;08006&quot;</td><td>2025-06-26 18:00:00</td><td>12091</td></tr><tr><td>&quot;08007&quot;</td><td>2025-06-26 18:00:00</td><td>2200</td></tr><tr><td>&quot;08008&quot;</td><td>2025-06-26 18:00:00</td><td>1660</td></tr><tr><td>&quot;08009&quot;</td><td>2025-06-26 18:00:00</td><td>1892</td></tr><tr><td>&quot;08010&quot;</td><td>2025-06-26 18:00:00</td><td>3300</td></tr></tbody></table></div>




```python
mwh_problematicos.group_by("datetime") \
    .agg(pl.n_unique("cod_postal").alias("n_codigos")) \
    .sort("datetime")
```




<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (9, 2)</small><table border="1" class="dataframe"><thead><tr><th>datetime</th><th>n_codigos</th></tr><tr><td>datetime[μs]</td><td>u32</td></tr></thead><tbody><tr><td>2025-06-26 00:00:00</td><td>42</td></tr><tr><td>2025-06-26 06:00:00</td><td>42</td></tr><tr><td>2025-06-26 12:00:00</td><td>42</td></tr><tr><td>2025-08-30 00:00:00</td><td>42</td></tr><tr><td>2025-08-30 06:00:00</td><td>42</td></tr><tr><td>2025-08-30 12:00:00</td><td>42</td></tr><tr><td>2025-09-07 00:00:00</td><td>42</td></tr><tr><td>2025-09-07 06:00:00</td><td>42</td></tr><tr><td>2025-09-07 12:00:00</td><td>42</td></tr></tbody></table></div>



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

    Registros esperados: 424,368
    Registros reales:    424,148
    Registros faltantes: 220
    
    Fechas con mayor número de CP faltantes:
    shape: (14, 2)
    ┌────────────┬──────────────┐
    │ fecha      ┆ cp_faltantes │
    │ ---        ┆ ---          │
    │ date       ┆ u32          │
    ╞════════════╪══════════════╡
    │ 2025-08-19 ┆ 168          │
    │ 2025-08-07 ┆ 4            │
    │ 2025-08-10 ┆ 4            │
    │ 2025-08-14 ┆ 4            │
    │ 2025-08-13 ┆ 4            │
    │ 2025-08-09 ┆ 4            │
    │ 2025-08-16 ┆ 4            │
    │ 2025-08-08 ┆ 4            │
    │ 2025-08-17 ┆ 4            │
    │ 2025-08-11 ┆ 4            │
    │ 2025-08-18 ┆ 4            │
    │ 2025-08-15 ┆ 4            │
    │ 2025-08-20 ┆ 4            │
    │ 2025-08-12 ┆ 4            │
    └────────────┴──────────────┘



```python
faltantes_por_dia = faltantes.with_columns(
    pl.col("datetime").dt.date().alias("fecha")
).group_by(["cod_postal", "fecha"]).count()

faltantes_por_dia.filter(
    pl.col("count") != 4  # 4 registros por día (cada 6h)
)
```

    /tmp/ipykernel_2175/2924495419.py:3: DeprecationWarning: `GroupBy.count` is deprecated. It has been renamed to `len`.
      ).group_by(["cod_postal", "fecha"]).count()





<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (0, 3)</small><table border="1" class="dataframe"><thead><tr><th>cod_postal</th><th>fecha</th><th>count</th></tr><tr><td>str</td><td>date</td><td>u32</td></tr></thead><tbody></tbody></table></div>



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




<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (3, 30)</small><table border="1" class="dataframe"><thead><tr><th>cod_postal</th><th>nombre_postal</th><th>centroide_lon</th><th>centroide_lat</th><th>codi_estacio</th><th>nombre_estacio</th><th>estacio_lon</th><th>estacio_lat</th><th>datetime</th><th>mwh_total</th><th>mwh_industria</th><th>mwh_residencial</th><th>mwh_servicios</th><th>mwh_no_especificado</th><th>lst_celsius</th><th>temp_mean</th><th>temp_max</th><th>temp_min</th><th>humedad_mean</th><th>viento_mean</th><th>precipitacion_sum</th><th>irradiancia_mean</th><th>es_festivo</th><th>nombre_local</th><th>hora</th><th>dia_semana</th><th>mes</th><th>anio</th><th>semana_anio</th><th>es_finde</th></tr><tr><td>str</td><td>str</td><td>f64</td><td>f64</td><td>str</td><td>str</td><td>f64</td><td>f64</td><td>datetime[μs]</td><td>i64</td><td>i64</td><td>i64</td><td>i64</td><td>i64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>i8</td><td>str</td><td>i8</td><td>i8</td><td>i8</td><td>i16</td><td>i64</td><td>i8</td></tr></thead><tbody><tr><td>&quot;08001&quot;</td><td>&quot;Las Ramblas / El Raval&quot;</td><td>2.171735</td><td>41.38032</td><td>&quot;X4&quot;</td><td>&quot;Barcelona - el Raval&quot;</td><td>2.1656</td><td>41.3793</td><td>2025-01-01 00:00:00</td><td>70044</td><td>287</td><td>32270</td><td>37487</td><td>null</td><td>11.45</td><td>10.183333</td><td>10.9</td><td>9.4</td><td>77.833333</td><td>1.058333</td><td>0.0</td><td>-2.0</td><td>1</td><td>&quot;Año Nuevo&quot;</td><td>0</td><td>3</td><td>1</td><td>2025</td><td>1</td><td>0</td></tr><tr><td>&quot;08001&quot;</td><td>&quot;Las Ramblas / El Raval&quot;</td><td>2.171735</td><td>41.38032</td><td>&quot;X4&quot;</td><td>&quot;Barcelona - el Raval&quot;</td><td>2.1656</td><td>41.3793</td><td>2025-01-01 06:00:00</td><td>84422</td><td>366</td><td>38319</td><td>45737</td><td>null</td><td>11.45</td><td>11.25</td><td>14.2</td><td>9.1</td><td>70.0</td><td>0.841667</td><td>0.0</td><td>176.083333</td><td>1</td><td>&quot;Año Nuevo&quot;</td><td>6</td><td>3</td><td>1</td><td>2025</td><td>1</td><td>0</td></tr><tr><td>&quot;08001&quot;</td><td>&quot;Las Ramblas / El Raval&quot;</td><td>2.171735</td><td>41.38032</td><td>&quot;X4&quot;</td><td>&quot;Barcelona - el Raval&quot;</td><td>2.1656</td><td>41.3793</td><td>2025-01-01 12:00:00</td><td>106367</td><td>417</td><td>53880</td><td>52070</td><td>null</td><td>11.45</td><td>12.916667</td><td>13.5</td><td>11.9</td><td>70.083333</td><td>1.083333</td><td>0.0</td><td>94.916667</td><td>1</td><td>&quot;Año Nuevo&quot;</td><td>12</td><td>3</td><td>1</td><td>2025</td><td>1</td><td>0</td></tr></tbody></table></div>



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

    Reasignación AN → X2 completada
    shape: (4, 3)
    ┌──────────────┬────────────┬────────┐
    │ codi_estacio ┆ nulos_temp ┆ total  │
    │ ---          ┆ ---        ┆ ---    │
    │ str          ┆ u32        ┆ u32    │
    ╞══════════════╪════════════╪════════╡
    │ D5           ┆ 560        ┆ 70700  │
    │ X2           ┆ 26165      ┆ 151500 │
    │ X4           ┆ 864        ┆ 161548 │
    │ X8           ┆ 216        ┆ 40400  │
    └──────────────┴────────────┴────────┘


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

    Faltantes detectados: 220
    shape: (1, 2)
    ┌────────────┬─────┐
    │ cod_postal ┆ n   │
    │ ---        ┆ --- │
    │ str        ┆ u32 │
    ╞════════════╪═════╡
    │ 08011      ┆ 52  │
    └────────────┴─────┘
    Shape: (424368, 30) | Nulls mwh_total: 0


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

    Shape: (424368, 30)
    shape: (20, 3)
    ┌─────────────────────┬────────┬───────┐
    │ columna             ┆ nulos  ┆ pct   │
    │ ---                 ┆ ---    ┆ ---   │
    │ str                 ┆ u32    ┆ f64   │
    ╞═════════════════════╪════════╪═══════╡
    │ nombre_local        ┆ 407404 ┆ 96.0  │
    │ mwh_no_especificado ┆ 362224 ┆ 85.36 │
    │ lst_celsius         ┆ 169044 ┆ 39.83 │
    │ viento_mean         ┆ 153421 ┆ 36.15 │
    │ irradiancia_mean    ┆ 153416 ┆ 36.15 │
    │ temp_max            ┆ 136747 ┆ 32.22 │
    │ temp_min            ┆ 136747 ┆ 32.22 │
    │ precipitacion_sum   ┆ 136648 ┆ 32.2  │
    │ temp_mean           ┆ 28025  ┆ 6.6   │
    │ humedad_mean        ┆ 28018  ┆ 6.6   │
    │ nombre_postal       ┆ 220    ┆ 0.05  │
    │ centroide_lon       ┆ 220    ┆ 0.05  │
    │ centroide_lat       ┆ 220    ┆ 0.05  │
    │ codi_estacio        ┆ 220    ┆ 0.05  │
    │ nombre_estacio      ┆ 220    ┆ 0.05  │
    │ estacio_lon         ┆ 220    ┆ 0.05  │
    │ estacio_lat         ┆ 220    ┆ 0.05  │
    │ anio                ┆ 220    ┆ 0.05  │
    │ semana_anio         ┆ 220    ┆ 0.05  │
    │ es_finde            ┆ 220    ┆ 0.05  │
    └─────────────────────┴────────┴───────┘



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

    Ceros restantes: 0
    Nulls mwh_total: 0


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




<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (9, 2)</small><table border="1" class="dataframe"><thead><tr><th>statistic</th><th>mwh_total</th></tr><tr><td>str</td><td>f64</td></tr></thead><tbody><tr><td>&quot;count&quot;</td><td>424368.0</td></tr><tr><td>&quot;null_count&quot;</td><td>0.0</td></tr><tr><td>&quot;mean&quot;</td><td>101467.761962</td></tr><tr><td>&quot;std&quot;</td><td>59563.840815</td></tr><tr><td>&quot;min&quot;</td><td>130.0</td></tr><tr><td>&quot;25%&quot;</td><td>57120.0</td></tr><tr><td>&quot;50%&quot;</td><td>90608.0</td></tr><tr><td>&quot;75%&quot;</td><td>132708.0</td></tr><tr><td>&quot;max&quot;</td><td>1.486114e6</td></tr></tbody></table></div>



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


    
![png](01_eda_electricityBCN_files/01_eda_electricityBCN_55_0.png)
    



```python
print(f"sum(is.na(mwh_total)): {df['mwh_total'].null_count()}")

```

    sum(is.na(mwh_total)): 0


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

    shape: (6, 2)
    ┌────────────┬─────────────┐
    │ cod_postal ┆ n_registros │
    │ ---        ┆ ---         │
    │ str        ┆ u32         │
    ╞════════════╪═════════════╡
    │ 08037      ┆ 450         │
    │ 08009      ┆ 51          │
    │ 08036      ┆ 10          │
    │ 08006      ┆ 8           │
    │ 08011      ┆ 5           │
    │ 08022      ┆ 4           │
    └────────────┴─────────────┘
    
    ============================================================
      08030 — Sostenido (may 2023)
    ============================================================
    shape: (28, 3)
    ┌─────────────────────┬───────────┬───────────────┐
    │ datetime            ┆ mwh_total ┆ mwh_industria │
    │ ---                 ┆ ---       ┆ ---           │
    │ datetime[μs]        ┆ f64       ┆ i64           │
    ╞═════════════════════╪═══════════╪═══════════════╡
    │ 2023-05-05 00:00:00 ┆ 696273.0  ┆ 604699        │
    │ 2023-05-05 06:00:00 ┆ 529464.0  ┆ 379442        │
    │ 2023-05-05 12:00:00 ┆ 733625.0  ┆ 562245        │
    │ 2023-05-05 18:00:00 ┆ 474837.0  ┆ 313451        │
    │ 2023-05-06 00:00:00 ┆ 694591.0  ┆ 600889        │
    │ 2023-05-06 06:00:00 ┆ 730729.0  ┆ 595781        │
    │ 2023-05-06 12:00:00 ┆ 751901.0  ┆ 595288        │
    │ 2023-05-06 18:00:00 ┆ 738284.0  ┆ 596137        │
    │ 2023-05-07 00:00:00 ┆ 692074.0  ┆ 596426        │
    │ 2023-05-07 06:00:00 ┆ 709783.0  ┆ 595492        │
    │ 2023-05-07 12:00:00 ┆ 733442.0  ┆ 594015        │
    │ 2023-05-07 18:00:00 ┆ 734611.0  ┆ 596792        │
    │ 2023-05-08 00:00:00 ┆ 684412.0  ┆ 596716        │
    │ 2023-05-08 06:00:00 ┆ 529118.0  ┆ 375951        │
    │ 2023-05-08 12:00:00 ┆ 741540.0  ┆ 561570        │
    │ 2023-05-08 18:00:00 ┆ 477660.0  ┆ 310960        │
    │ 2023-05-09 00:00:00 ┆ 694639.0  ┆ 601878        │
    │ 2023-05-09 06:00:00 ┆ 527432.0  ┆ 372848        │
    │ 2023-05-09 12:00:00 ┆ 728026.0  ┆ 548366        │
    │ 2023-05-09 18:00:00 ┆ 481645.0  ┆ 311695        │
    │ 2023-05-10 00:00:00 ┆ 693462.0  ┆ 601869        │
    │ 2023-05-10 06:00:00 ┆ 535464.0  ┆ 378302        │
    │ 2023-05-10 12:00:00 ┆ 737959.0  ┆ 561987        │
    │ 2023-05-10 18:00:00 ┆ 479736.0  ┆ 313040        │
    │ 2023-05-11 00:00:00 ┆ 690939.0  ┆ 602660        │
    │ 2023-05-11 06:00:00 ┆ 530721.0  ┆ 379578        │
    │ 2023-05-11 12:00:00 ┆ 729888.0  ┆ 558757        │
    │ 2023-05-11 18:00:00 ┆ 473843.0  ┆ 312988        │
    └─────────────────────┴───────────┴───────────────┘
    
    ============================================================
      08037 — Puntual (nov/dic 2024, feb 2025)
    ============================================================
    shape: (3, 3)
    ┌─────────────────────┬────────────┬───────────────┐
    │ datetime            ┆ mwh_total  ┆ mwh_servicios │
    │ ---                 ┆ ---        ┆ ---           │
    │ datetime[μs]        ┆ f64        ┆ i64           │
    ╞═════════════════════╪════════════╪═══════════════╡
    │ 2024-11-20 00:00:00 ┆ 1.470943e6 ┆ 1461240       │
    │ 2024-12-22 00:00:00 ┆ 1.486114e6 ┆ 1472558       │
    │ 2025-02-24 00:00:00 ┆ 1.470897e6 ┆ 1466734       │
    └─────────────────────┴────────────┴───────────────┘
    
    ============================================================
      08022 — Parcial (may 2023)
    ============================================================
    shape: (8, 3)
    ┌─────────────────────┬───────────┬─────────────────┐
    │ datetime            ┆ mwh_total ┆ mwh_residencial │
    │ ---                 ┆ ---       ┆ ---             │
    │ datetime[μs]        ┆ f64       ┆ i64             │
    ╞═════════════════════╪═══════════╪═════════════════╡
    │ 2023-05-29 00:00:00 ┆ 43444.0   ┆ 16623           │
    │ 2023-05-29 06:00:00 ┆ 285671.0  ┆ 235781          │
    │ 2023-05-29 12:00:00 ┆ 950746.0  ┆ 891516          │
    │ 2023-05-29 18:00:00 ┆ 425559.0  ┆ 379977          │
    │ 2023-05-30 00:00:00 ┆ 43921.0   ┆ 16434           │
    │ 2023-05-30 06:00:00 ┆ 286177.0  ┆ 235774          │
    │ 2023-05-30 12:00:00 ┆ 951606.0  ┆ 890579          │
    │ 2023-05-30 18:00:00 ┆ 426664.0  ┆ 379715          │
    └─────────────────────┴───────────┴─────────────────┘
    
    ============================================================
      Valores > 400k
    ============================================================
    shape: (10, 4)
    ┌─────────────────────┬────────────┬─────────────────────────────┬────────────┐
    │ datetime            ┆ cod_postal ┆ nombre_postal               ┆ mwh_total  │
    │ ---                 ┆ ---        ┆ ---                         ┆ ---        │
    │ datetime[μs]        ┆ str        ┆ str                         ┆ f64        │
    ╞═════════════════════╪════════════╪═════════════════════════════╪════════════╡
    │ 2024-12-22 00:00:00 ┆ 08037      ┆ Vila de Gràcia              ┆ 1.486114e6 │
    │ 2024-11-20 00:00:00 ┆ 08037      ┆ Vila de Gràcia              ┆ 1.470943e6 │
    │ 2025-02-24 00:00:00 ┆ 08037      ┆ Vila de Gràcia              ┆ 1.470897e6 │
    │ 2023-05-30 12:00:00 ┆ 08022      ┆ Les Tres Torres / Bonanova  ┆ 951606.0   │
    │ 2023-05-29 12:00:00 ┆ 08022      ┆ Les Tres Torres / Bonanova  ┆ 950746.0   │
    │ 2023-05-06 12:00:00 ┆ 08030      ┆ El Bon Pastor / Sant Andreu ┆ 751901.0   │
    │ 2023-05-08 12:00:00 ┆ 08030      ┆ El Bon Pastor / Sant Andreu ┆ 741540.0   │
    │ 2023-05-06 18:00:00 ┆ 08030      ┆ El Bon Pastor / Sant Andreu ┆ 738284.0   │
    │ 2023-05-10 12:00:00 ┆ 08030      ┆ El Bon Pastor / Sant Andreu ┆ 737959.0   │
    │ 2023-05-07 18:00:00 ┆ 08030      ┆ El Bon Pastor / Sant Andreu ┆ 734611.0   │
    └─────────────────────┴────────────┴─────────────────────────────┴────────────┘
    
    ==================================================
    CP 08013
    ==================================================
    shape: (25, 2)
    ┌─────────────────────┬───────────┐
    │ datetime            ┆ mwh_total │
    │ ---                 ┆ ---       │
    │ datetime[μs]        ┆ f64       │
    ╞═════════════════════╪═══════════╡
    │ 2025-06-29 12:00:00 ┆ 209986.0  │
    │ 2025-06-29 18:00:00 ┆ 179926.0  │
    │ 2025-06-30 00:00:00 ┆ 143513.0  │
    │ 2025-06-30 06:00:00 ┆ 289351.0  │
    │ 2025-06-30 12:00:00 ┆ 354314.0  │
    │ 2025-06-30 18:00:00 ┆ 243793.0  │
    │ 2025-07-01 00:00:00 ┆ 163194.0  │
    │ 2025-07-01 06:00:00 ┆ 279589.0  │
    │ 2025-07-01 12:00:00 ┆ 359093.0  │
    │ 2025-07-01 18:00:00 ┆ 307379.0  │
    │ 2025-07-02 00:00:00 ┆ 259633.0  │
    │ 2025-07-02 06:00:00 ┆ 434803.0  │
    │ 2025-07-02 12:00:00 ┆ 563018.0  │
    │ 2025-07-02 18:00:00 ┆ 458091.0  │
    │ 2025-07-03 00:00:00 ┆ 201580.0  │
    │ 2025-07-03 06:00:00 ┆ 336354.0  │
    │ 2025-07-03 12:00:00 ┆ 421765.0  │
    │ 2025-07-03 18:00:00 ┆ 315639.0  │
    │ 2025-07-04 00:00:00 ┆ 147533.0  │
    │ 2025-07-04 06:00:00 ┆ 258290.0  │
    │ 2025-07-04 12:00:00 ┆ 321349.0  │
    │ 2025-07-04 18:00:00 ┆ 247963.0  │
    │ 2025-07-05 00:00:00 ┆ 43453.0   │
    │ 2025-07-05 06:00:00 ┆ 57244.0   │
    │ 2025-07-05 12:00:00 ┆ 72582.0   │
    └─────────────────────┴───────────┘
    
    ==================================================
    CP 08036
    ==================================================
    shape: (25, 2)
    ┌─────────────────────┬───────────┐
    │ datetime            ┆ mwh_total │
    │ ---                 ┆ ---       │
    │ datetime[μs]        ┆ f64       │
    ╞═════════════════════╪═══════════╡
    │ 2023-08-21 12:00:00 ┆ 503729.0  │
    │ 2023-08-21 18:00:00 ┆ 404773.0  │
    │ 2023-08-22 00:00:00 ┆ 247634.0  │
    │ 2023-08-22 06:00:00 ┆ 416642.0  │
    │ 2023-08-22 12:00:00 ┆ 507850.0  │
    │ 2023-08-22 18:00:00 ┆ 403626.0  │
    │ 2023-08-23 00:00:00 ┆ 252011.0  │
    │ 2023-08-23 06:00:00 ┆ 420037.0  │
    │ 2023-08-23 12:00:00 ┆ 523697.0  │
    │ 2023-08-23 18:00:00 ┆ 419809.0  │
    │ 2023-08-24 00:00:00 ┆ 266176.0  │
    │ 2023-08-24 06:00:00 ┆ 433164.0  │
    │ 2023-08-24 12:00:00 ┆ 535033.0  │
    │ 2023-08-24 18:00:00 ┆ 420181.0  │
    │ 2023-08-25 00:00:00 ┆ 260767.0  │
    │ 2023-08-25 06:00:00 ┆ 426685.0  │
    │ 2023-08-25 12:00:00 ┆ 510604.0  │
    │ 2023-08-25 18:00:00 ┆ 397942.0  │
    │ 2023-08-26 00:00:00 ┆ 245354.0  │
    │ 2023-08-26 06:00:00 ┆ 297161.0  │
    │ 2023-08-26 12:00:00 ┆ 358967.0  │
    │ 2023-08-26 18:00:00 ┆ 303100.0  │
    │ 2023-08-27 00:00:00 ┆ 222683.0  │
    │ 2023-08-27 06:00:00 ┆ 237011.0  │
    │ 2023-08-27 12:00:00 ┆ 280526.0  │
    └─────────────────────┴───────────┘
    
    ==================================================
    CP 08009
    ==================================================
    shape: (25, 2)
    ┌─────────────────────┬───────────┐
    │ datetime            ┆ mwh_total │
    │ ---                 ┆ ---       │
    │ datetime[μs]        ┆ f64       │
    ╞═════════════════════╪═══════════╡
    │ 2025-05-30 12:00:00 ┆ 46167.0   │
    │ 2025-05-30 18:00:00 ┆ 29997.0   │
    │ 2025-05-31 00:00:00 ┆ 19731.0   │
    │ 2025-05-31 06:00:00 ┆ 27563.0   │
    │ 2025-05-31 12:00:00 ┆ 34644.0   │
    │ 2025-05-31 18:00:00 ┆ 30558.0   │
    │ 2025-06-01 00:00:00 ┆ 20246.0   │
    │ 2025-06-01 06:00:00 ┆ 26266.0   │
    │ 2025-06-01 12:00:00 ┆ 33741.0   │
    │ 2025-06-01 18:00:00 ┆ 29463.0   │
    │ 2025-06-02 00:00:00 ┆ 131383.0  │
    │ 2025-06-02 06:00:00 ┆ 273605.0  │
    │ 2025-06-02 12:00:00 ┆ 336063.0  │
    │ 2025-06-02 18:00:00 ┆ 224451.0  │
    │ 2025-06-03 00:00:00 ┆ 17151.0   │
    │ 2025-06-03 06:00:00 ┆ 32326.0   │
    │ 2025-06-03 12:00:00 ┆ 39224.0   │
    │ 2025-06-03 18:00:00 ┆ 29513.0   │
    │ 2025-06-04 00:00:00 ┆ 19976.0   │
    │ 2025-06-04 06:00:00 ┆ 39197.0   │
    │ 2025-06-04 12:00:00 ┆ 49614.0   │
    │ 2025-06-04 18:00:00 ┆ 33703.0   │
    │ 2025-06-05 00:00:00 ┆ 20456.0   │
    │ 2025-06-05 06:00:00 ┆ 38996.0   │
    │ 2025-06-05 12:00:00 ┆ 48937.0   │
    └─────────────────────┴───────────┘
    
    ==================================================
    CP 08006
    ==================================================
    shape: (25, 2)
    ┌─────────────────────┬───────────┐
    │ datetime            ┆ mwh_total │
    │ ---                 ┆ ---       │
    │ datetime[μs]        ┆ f64       │
    ╞═════════════════════╪═══════════╡
    │ 2025-06-30 12:00:00 ┆ 276261.0  │
    │ 2025-06-30 18:00:00 ┆ 162919.0  │
    │ 2025-07-01 00:00:00 ┆ 121457.0  │
    │ 2025-07-01 06:00:00 ┆ 231411.0  │
    │ 2025-07-01 12:00:00 ┆ 288122.0  │
    │ 2025-07-01 18:00:00 ┆ 212058.0  │
    │ 2025-07-02 00:00:00 ┆ 91475.0   │
    │ 2025-07-02 06:00:00 ┆ 177770.0  │
    │ 2025-07-02 12:00:00 ┆ 224880.0  │
    │ 2025-07-02 18:00:00 ┆ 167936.0  │
    │ 2025-07-03 00:00:00 ┆ 185984.0  │
    │ 2025-07-03 06:00:00 ┆ 398974.0  │
    │ 2025-07-03 12:00:00 ┆ 493012.0  │
    │ 2025-07-03 18:00:00 ┆ 320186.0  │
    │ 2025-07-04 00:00:00 ┆ 111535.0  │
    │ 2025-07-04 06:00:00 ┆ 206710.0  │
    │ 2025-07-04 12:00:00 ┆ 245087.0  │
    │ 2025-07-04 18:00:00 ┆ 170244.0  │
    │ 2025-07-05 00:00:00 ┆ 34138.0   │
    │ 2025-07-05 06:00:00 ┆ 41018.0   │
    │ 2025-07-05 12:00:00 ┆ 51335.0   │
    │ 2025-07-05 18:00:00 ┆ 48816.0   │
    │ 2025-07-06 00:00:00 ┆ 33601.0   │
    │ 2025-07-06 06:00:00 ┆ 36960.0   │
    │ 2025-07-06 12:00:00 ┆ 48196.0   │
    └─────────────────────┴───────────┘
    
    ==================================================
    CP 08011
    ==================================================
    shape: (25, 2)
    ┌─────────────────────┬───────────┐
    │ datetime            ┆ mwh_total │
    │ ---                 ┆ ---       │
    │ datetime[μs]        ┆ f64       │
    ╞═════════════════════╪═══════════╡
    │ 2020-01-18 12:00:00 ┆ 203969.0  │
    │ 2020-01-18 18:00:00 ┆ 199003.0  │
    │ 2020-01-19 00:00:00 ┆ 114685.0  │
    │ 2020-01-19 06:00:00 ┆ 143420.0  │
    │ 2020-01-19 12:00:00 ┆ 187059.0  │
    │ 2020-01-19 18:00:00 ┆ 193235.0  │
    │ 2020-01-20 00:00:00 ┆ 114024.0  │
    │ 2020-01-20 06:00:00 ┆ 223974.0  │
    │ 2020-01-20 12:00:00 ┆ 263158.0  │
    │ 2020-01-20 18:00:00 ┆ 238920.0  │
    │ 2020-01-21 00:00:00 ┆ 118671.0  │
    │ 2020-01-21 06:00:00 ┆ 231863.0  │
    │ 2020-01-21 12:00:00 ┆ 270743.0  │
    │ 2020-01-21 18:00:00 ┆ 237106.0  │
    │ 2020-01-22 00:00:00 ┆ 117279.0  │
    │ 2020-01-22 06:00:00 ┆ 220641.0  │
    │ 2020-01-22 12:00:00 ┆ 252205.0  │
    │ 2020-01-22 18:00:00 ┆ 222542.0  │
    │ 2020-01-23 00:00:00 ┆ 114202.0  │
    │ 2020-01-23 06:00:00 ┆ 212775.0  │
    │ 2020-01-23 12:00:00 ┆ 234326.0  │
    │ 2020-01-23 18:00:00 ┆ 214006.0  │
    │ 2020-01-24 00:00:00 ┆ 113020.0  │
    │ 2020-01-24 06:00:00 ┆ 213377.0  │
    │ 2020-01-24 12:00:00 ┆ 231196.0  │
    └─────────────────────┴───────────┘
    
    ==================================================
    CP 08030
    ==================================================
    shape: (25, 2)
    ┌─────────────────────┬───────────┐
    │ datetime            ┆ mwh_total │
    │ ---                 ┆ ---       │
    │ datetime[μs]        ┆ f64       │
    ╞═════════════════════╪═══════════╡
    │ 2023-05-03 12:00:00 ┆ 217230.0  │
    │ 2023-05-03 18:00:00 ┆ 211800.0  │
    │ 2023-05-04 00:00:00 ┆ 136021.0  │
    │ 2023-05-04 06:00:00 ┆ 206698.0  │
    │ 2023-05-04 12:00:00 ┆ 232096.0  │
    │ 2023-05-04 18:00:00 ┆ 213240.0  │
    │ 2023-05-05 00:00:00 ┆ 696273.0  │
    │ 2023-05-05 06:00:00 ┆ 529464.0  │
    │ 2023-05-05 12:00:00 ┆ 733625.0  │
    │ 2023-05-05 18:00:00 ┆ 474837.0  │
    │ 2023-05-06 00:00:00 ┆ 694591.0  │
    │ 2023-05-06 06:00:00 ┆ 730729.0  │
    │ 2023-05-06 12:00:00 ┆ 751901.0  │
    │ 2023-05-06 18:00:00 ┆ 738284.0  │
    │ 2023-05-07 00:00:00 ┆ 692074.0  │
    │ 2023-05-07 06:00:00 ┆ 709783.0  │
    │ 2023-05-07 12:00:00 ┆ 733442.0  │
    │ 2023-05-07 18:00:00 ┆ 734611.0  │
    │ 2023-05-08 00:00:00 ┆ 684412.0  │
    │ 2023-05-08 06:00:00 ┆ 529118.0  │
    │ 2023-05-08 12:00:00 ┆ 741540.0  │
    │ 2023-05-08 18:00:00 ┆ 477660.0  │
    │ 2023-05-09 00:00:00 ┆ 694639.0  │
    │ 2023-05-09 06:00:00 ┆ 527432.0  │
    │ 2023-05-09 12:00:00 ┆ 728026.0  │
    └─────────────────────┴───────────┘


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

    Todos los outliers positivos imputados



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

    Valores > 400k restantes: 290
    shape: (20, 7)
    ┌──────────────┬────────────┬──────────────┬───────────┬──────────────┬──────────────┬─────────────┐
    │ datetime     ┆ cod_postal ┆ nombre_posta ┆ mwh_total ┆ mwh_industri ┆ mwh_residenc ┆ mwh_servici │
    │ ---          ┆ ---        ┆ l            ┆ ---       ┆ a            ┆ ial          ┆ os          │
    │ datetime[μs] ┆ str        ┆ ---          ┆ f64       ┆ ---          ┆ ---          ┆ ---         │
    │              ┆            ┆ str          ┆           ┆ f64          ┆ f64          ┆ f64         │
    ╞══════════════╪════════════╪══════════════╪═══════════╪══════════════╪══════════════╪═════════════╡
    │ 2025-07-02   ┆ 08013      ┆ La Sagrada   ┆ 563018.0  ┆ 9070.0       ┆ 242277.0     ┆ 311671.0    │
    │ 12:00:00     ┆            ┆ Família      ┆           ┆              ┆              ┆             │
    │ 2023-08-24   ┆ 08036      ┆ L'Antiga     ┆ 535033.0  ┆ 7951.0       ┆ 133672.0     ┆ 393410.0    │
    │ 12:00:00     ┆            ┆ Esquerra de  ┆           ┆              ┆              ┆             │
    │              ┆            ┆ l'Eixampl…   ┆           ┆              ┆              ┆             │
    │ 2022-03-27   ┆ 08040      ┆ La Zona      ┆ 534188.0  ┆ 434408.0     ┆ 8628.0       ┆ 91152.0     │
    │ 00:00:00     ┆            ┆ Franca       ┆           ┆              ┆              ┆             │
    │ 2025-07-21   ┆ 08036      ┆ L'Antiga     ┆ 532515.0  ┆ 10303.0      ┆ 139008.0     ┆ 383204.0    │
    │ 12:00:00     ┆            ┆ Esquerra de  ┆           ┆              ┆              ┆             │
    │              ┆            ┆ l'Eixampl…   ┆           ┆              ┆              ┆             │
    │ 2025-01-09   ┆ 08036      ┆ L'Antiga     ┆ 530918.0  ┆ 8552.0       ┆ 117016.0     ┆ 405350.0    │
    │ 12:00:00     ┆            ┆ Esquerra de  ┆           ┆              ┆              ┆             │
    │              ┆            ┆ l'Eixampl…   ┆           ┆              ┆              ┆             │
    │ 2025-07-22   ┆ 08036      ┆ L'Antiga     ┆ 530361.0  ┆ 10345.0      ┆ 133632.0     ┆ 386384.0    │
    │ 12:00:00     ┆            ┆ Esquerra de  ┆           ┆              ┆              ┆             │
    │              ┆            ┆ l'Eixampl…   ┆           ┆              ┆              ┆             │
    │ 2023-08-23   ┆ 08036      ┆ L'Antiga     ┆ 523697.0  ┆ 8020.0       ┆ 131469.0     ┆ 384208.0    │
    │ 12:00:00     ┆            ┆ Esquerra de  ┆           ┆              ┆              ┆             │
    │              ┆            ┆ l'Eixampl…   ┆           ┆              ┆              ┆             │
    │ 2023-08-02   ┆ 08036      ┆ L'Antiga     ┆ 514862.0  ┆ 7704.0       ┆ 120803.0     ┆ 386355.0    │
    │ 12:00:00     ┆            ┆ Esquerra de  ┆           ┆              ┆              ┆             │
    │              ┆            ┆ l'Eixampl…   ┆           ┆              ┆              ┆             │
    │ 2023-08-25   ┆ 08036      ┆ L'Antiga     ┆ 510604.0  ┆ 7236.0       ┆ 126452.0     ┆ 376916.0    │
    │ 12:00:00     ┆            ┆ Esquerra de  ┆           ┆              ┆              ┆             │
    │              ┆            ┆ l'Eixampl…   ┆           ┆              ┆              ┆             │
    │ 2022-12-05   ┆ 08028      ┆ Zona Univers ┆ 509684.0  ┆ 12660.0      ┆ 176112.0     ┆ 320216.0    │
    │ 12:00:00     ┆            ┆ itària / Les ┆           ┆              ┆              ┆             │
    │              ┆            ┆ Corts        ┆           ┆              ┆              ┆             │
    │ 2023-08-22   ┆ 08036      ┆ L'Antiga     ┆ 507850.0  ┆ 7751.0       ┆ 125365.0     ┆ 374734.0    │
    │ 12:00:00     ┆            ┆ Esquerra de  ┆           ┆              ┆              ┆             │
    │              ┆            ┆ l'Eixampl…   ┆           ┆              ┆              ┆             │
    │ 2023-08-21   ┆ 08036      ┆ L'Antiga     ┆ 503729.0  ┆ 7872.0       ┆ 123097.0     ┆ 372760.0    │
    │ 12:00:00     ┆            ┆ Esquerra de  ┆           ┆              ┆              ┆             │
    │              ┆            ┆ l'Eixampl…   ┆           ┆              ┆              ┆             │
    │ 2023-07-08   ┆ 08040      ┆ La Zona      ┆ 502958.0  ┆ 146058.0     ┆ 22360.0      ┆ 334540.0    │
    │ 00:00:00     ┆            ┆ Franca       ┆           ┆              ┆              ┆             │
    │ 2025-01-09   ┆ 08040      ┆ La Zona      ┆ 502528.0  ┆ 121898.0     ┆ 22176.0      ┆ 358454.0    │
    │ 06:00:00     ┆            ┆ Franca       ┆           ┆              ┆              ┆             │
    │ 2023-07-08   ┆ 08028      ┆ Zona Univers ┆ 497094.0  ┆ 17908.0      ┆ 178428.0     ┆ 300226.0    │
    │ 12:00:00     ┆            ┆ itària / Les ┆           ┆              ┆              ┆             │
    │              ┆            ┆ Corts        ┆           ┆              ┆              ┆             │
    │ 2023-08-01   ┆ 08036      ┆ L'Antiga     ┆ 495490.0  ┆ 7553.0       ┆ 114433.0     ┆ 373504.0    │
    │ 12:00:00     ┆            ┆ Esquerra de  ┆           ┆              ┆              ┆             │
    │              ┆            ┆ l'Eixampl…   ┆           ┆              ┆              ┆             │
    │ 2022-12-05   ┆ 08040      ┆ La Zona      ┆ 493858.0  ┆ 142068.0     ┆ 28546.0      ┆ 323244.0    │
    │ 06:00:00     ┆            ┆ Franca       ┆           ┆              ┆              ┆             │
    │ 2025-07-03   ┆ 08006      ┆ Sarrià -     ┆ 493012.0  ┆ 6489.0       ┆ 126884.0     ┆ 359639.0    │
    │ 12:00:00     ┆            ┆ Sant Gervasi ┆           ┆              ┆              ┆             │
    │ 2020-01-14   ┆ 08040      ┆ La Zona      ┆ 491401.0  ┆ 191954.0     ┆ 28893.0      ┆ 270554.0    │
    │ 06:00:00     ┆            ┆ Franca       ┆           ┆              ┆              ┆             │
    │ 2023-09-05   ┆ 08006      ┆ Sarrià -     ┆ 491368.0  ┆ 6317.0       ┆ 120546.0     ┆ 364505.0    │
    │ 12:00:00     ┆            ┆ Sant Gervasi ┆           ┆              ┆              ┆             │
    └──────────────┴────────────┴──────────────┴───────────┴──────────────┴──────────────┴─────────────┘



    
![png](01_eda_electricityBCN_files/01_eda_electricityBCN_62_1.png)
    


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


    
![png](01_eda_electricityBCN_files/01_eda_electricityBCN_66_0.png)
    



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




<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (42, 4)</small><table border="1" class="dataframe"><thead><tr><th>cod_postal</th><th>pct_industria</th><th>pct_residencial</th><th>pct_servicios</th></tr><tr><td>str</td><td>f64</td><td>f64</td><td>f64</td></tr></thead><tbody><tr><td>&quot;08039&quot;</td><td>41.5</td><td>3.3</td><td>55.3</td></tr><tr><td>&quot;08004&quot;</td><td>38.8</td><td>26.8</td><td>34.5</td></tr><tr><td>&quot;08038&quot;</td><td>36.7</td><td>21.2</td><td>42.0</td></tr><tr><td>&quot;08040&quot;</td><td>29.0</td><td>5.6</td><td>65.4</td></tr><tr><td>&quot;08030&quot;</td><td>16.0</td><td>27.0</td><td>57.1</td></tr><tr><td>&quot;08018&quot;</td><td>10.4</td><td>32.2</td><td>57.4</td></tr><tr><td>&quot;08020&quot;</td><td>7.2</td><td>31.9</td><td>60.9</td></tr><tr><td>&quot;08019&quot;</td><td>5.8</td><td>47.3</td><td>46.9</td></tr><tr><td>&quot;08021&quot;</td><td>4.0</td><td>45.3</td><td>50.6</td></tr><tr><td>&quot;08008&quot;</td><td>3.9</td><td>27.8</td><td>68.3</td></tr><tr><td>&quot;08012&quot;</td><td>3.7</td><td>51.6</td><td>44.7</td></tr><tr><td>&quot;08005&quot;</td><td>3.5</td><td>33.0</td><td>63.5</td></tr><tr><td>&quot;08006&quot;</td><td>3.3</td><td>38.3</td><td>58.4</td></tr><tr><td>&quot;08007&quot;</td><td>3.3</td><td>26.5</td><td>70.2</td></tr><tr><td>&quot;08013&quot;</td><td>3.0</td><td>48.9</td><td>48.1</td></tr><tr><td>&quot;08010&quot;</td><td>2.9</td><td>32.3</td><td>64.8</td></tr><tr><td>&quot;08022&quot;</td><td>2.9</td><td>44.1</td><td>53.0</td></tr><tr><td>&quot;08002&quot;</td><td>2.8</td><td>21.6</td><td>75.6</td></tr><tr><td>&quot;08011&quot;</td><td>2.8</td><td>37.0</td><td>60.2</td></tr><tr><td>&quot;08025&quot;</td><td>2.7</td><td>41.0</td><td>56.3</td></tr><tr><td>&quot;08036&quot;</td><td>2.6</td><td>27.3</td><td>70.1</td></tr><tr><td>&quot;08009&quot;</td><td>2.5</td><td>38.9</td><td>58.6</td></tr><tr><td>&quot;08003&quot;</td><td>2.4</td><td>29.0</td><td>68.6</td></tr><tr><td>&quot;08015&quot;</td><td>2.4</td><td>40.2</td><td>57.5</td></tr><tr><td>&quot;08028&quot;</td><td>2.4</td><td>36.9</td><td>60.6</td></tr><tr><td>&quot;08034&quot;</td><td>2.4</td><td>32.9</td><td>64.8</td></tr><tr><td>&quot;08014&quot;</td><td>2.3</td><td>39.5</td><td>58.1</td></tr><tr><td>&quot;08026&quot;</td><td>2.2</td><td>58.5</td><td>39.4</td></tr><tr><td>&quot;08029&quot;</td><td>2.2</td><td>42.8</td><td>55.0</td></tr><tr><td>&quot;08037&quot;</td><td>2.1</td><td>33.1</td><td>64.8</td></tr><tr><td>&quot;08017&quot;</td><td>2.0</td><td>39.9</td><td>58.1</td></tr><tr><td>&quot;08024&quot;</td><td>2.0</td><td>58.3</td><td>39.8</td></tr><tr><td>&quot;08041&quot;</td><td>2.0</td><td>62.4</td><td>35.6</td></tr><tr><td>&quot;08016&quot;</td><td>1.7</td><td>52.0</td><td>46.3</td></tr><tr><td>&quot;08001&quot;</td><td>1.5</td><td>36.1</td><td>62.4</td></tr><tr><td>&quot;08042&quot;</td><td>1.4</td><td>50.5</td><td>48.1</td></tr><tr><td>&quot;08033&quot;</td><td>1.3</td><td>57.3</td><td>41.3</td></tr><tr><td>&quot;08027&quot;</td><td>1.0</td><td>52.1</td><td>46.9</td></tr><tr><td>&quot;08031&quot;</td><td>1.0</td><td>63.7</td><td>35.2</td></tr><tr><td>&quot;08032&quot;</td><td>0.9</td><td>70.2</td><td>28.9</td></tr><tr><td>&quot;08023&quot;</td><td>0.8</td><td>49.5</td><td>49.7</td></tr><tr><td>&quot;08035&quot;</td><td>0.6</td><td>25.9</td><td>73.4</td></tr></tbody></table></div>



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


    
![png](01_eda_electricityBCN_files/01_eda_electricityBCN_70_0.png)
    


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




<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (84, 8)</small><table border="1" class="dataframe"><thead><tr><th>cod_postal</th><th>nombre_postal</th><th>n_registros</th><th>nulls</th><th>mwh_mean</th><th>pct_industria</th><th>pct_residencial</th><th>pct_servicios</th></tr><tr><td>str</td><td>str</td><td>u32</td><td>u32</td><td>f64</td><td>str</td><td>str</td><td>str</td></tr></thead><tbody><tr><td>&quot;08040&quot;</td><td>&quot;La Zona Franca&quot;</td><td>10100</td><td>0</td><td>233424.701881</td><td>&quot;29.0%&quot;</td><td>&quot;5.6%&quot;</td><td>&quot;65.3%&quot;</td></tr><tr><td>&quot;08030&quot;</td><td>&quot;El Bon Pastor / Sant Andreu&quot;</td><td>10100</td><td>0</td><td>211600.899257</td><td>&quot;16.0%&quot;</td><td>&quot;26.9%&quot;</td><td>&quot;57.0%&quot;</td></tr><tr><td>&quot;08028&quot;</td><td>&quot;Zona Universitària / Les Corts&quot;</td><td>10100</td><td>0</td><td>185543.360099</td><td>&quot;2.4%&quot;</td><td>&quot;36.9%&quot;</td><td>&quot;60.6%&quot;</td></tr><tr><td>&quot;08038&quot;</td><td>&quot;Montjuïc / Zona Franca&quot;</td><td>10100</td><td>0</td><td>161045.791287</td><td>&quot;36.7%&quot;</td><td>&quot;21.2%&quot;</td><td>&quot;42.0%&quot;</td></tr><tr><td>&quot;08018&quot;</td><td>&quot;Fort Pienc&quot;</td><td>10100</td><td>0</td><td>150034.722475</td><td>&quot;10.4%&quot;</td><td>&quot;32.1%&quot;</td><td>&quot;57.2%&quot;</td></tr><tr><td>&quot;08029&quot;</td><td>&quot;Nova Esquerra de l&#x27;Eixample&quot;</td><td>10100</td><td>0</td><td>139209.444554</td><td>&quot;2.2%&quot;</td><td>&quot;42.8%&quot;</td><td>&quot;55.0%&quot;</td></tr><tr><td>&quot;08003&quot;</td><td>&quot;La Barceloneta&quot;</td><td>10100</td><td>0</td><td>133936.397723</td><td>&quot;2.4%&quot;</td><td>&quot;29.0%&quot;</td><td>&quot;68.6%&quot;</td></tr><tr><td>&quot;08004&quot;</td><td>&quot;Montjuïc / Poble Sec&quot;</td><td>10100</td><td>0</td><td>133934.311584</td><td>&quot;38.8%&quot;</td><td>&quot;26.8%&quot;</td><td>&quot;34.5%&quot;</td></tr><tr><td>&quot;08014&quot;</td><td>&quot;Sants - Montjuïc&quot;</td><td>10100</td><td>0</td><td>133366.916436</td><td>&quot;2.3%&quot;</td><td>&quot;39.5%&quot;</td><td>&quot;58.1%&quot;</td></tr><tr><td>&quot;08039&quot;</td><td>&quot;El Port / La Barceloneta&quot;</td><td>10100</td><td>0</td><td>132470.119703</td><td>&quot;41.4%&quot;</td><td>&quot;3.3%&quot;</td><td>&quot;55.2%&quot;</td></tr><tr><td>&quot;08034&quot;</td><td>&quot;Pedralbes / Sarrià&quot;</td><td>10100</td><td>0</td><td>129030.457624</td><td>&quot;2.4%&quot;</td><td>&quot;32.8%&quot;</td><td>&quot;64.7%&quot;</td></tr><tr><td>&quot;08020&quot;</td><td>&quot;La Verneda&quot;</td><td>10100</td><td>0</td><td>128207.109406</td><td>&quot;7.1%&quot;</td><td>&quot;31.9%&quot;</td><td>&quot;60.9%&quot;</td></tr><tr><td>&quot;08005&quot;</td><td>&quot;El Poblenou&quot;</td><td>10100</td><td>0</td><td>125677.271386</td><td>&quot;3.5%&quot;</td><td>&quot;33.0%&quot;</td><td>&quot;63.4%&quot;</td></tr><tr><td>&quot;08013&quot;</td><td>&quot;La Sagrada Família&quot;</td><td>10100</td><td>0</td><td>125320.512871</td><td>&quot;3.0%&quot;</td><td>&quot;48.9%&quot;</td><td>&quot;48.1%&quot;</td></tr><tr><td>&quot;08002&quot;</td><td>&quot;Barri Gòtic&quot;</td><td>10100</td><td>0</td><td>124417.73495</td><td>&quot;2.8%&quot;</td><td>&quot;21.6%&quot;</td><td>&quot;75.6%&quot;</td></tr><tr><td>&quot;08025&quot;</td><td>&quot;El Guinardó&quot;</td><td>10100</td><td>0</td><td>120500.75495</td><td>&quot;2.7%&quot;</td><td>&quot;41.0%&quot;</td><td>&quot;56.3%&quot;</td></tr><tr><td>&quot;08015&quot;</td><td>&quot;Esquerra de l&#x27;Eixample&quot;</td><td>10100</td><td>0</td><td>118770.196238</td><td>&quot;2.4%&quot;</td><td>&quot;40.1%&quot;</td><td>&quot;57.4%&quot;</td></tr><tr><td>&quot;08036&quot;</td><td>&quot;L&#x27;Antiga Esquerra de l&#x27;Eixampl…</td><td>10100</td><td>0</td><td>112190.388911</td><td>&quot;2.6%&quot;</td><td>&quot;27.3%&quot;</td><td>&quot;70.1%&quot;</td></tr><tr><td>&quot;08017&quot;</td><td>&quot;Sarrià - Sant Gervasi&quot;</td><td>10100</td><td>0</td><td>102847.342574</td><td>&quot;2.0%&quot;</td><td>&quot;39.8%&quot;</td><td>&quot;58.0%&quot;</td></tr><tr><td>&quot;08006&quot;</td><td>&quot;Sarrià - Sant Gervasi&quot;</td><td>10100</td><td>0</td><td>99953.049208</td><td>&quot;3.2%&quot;</td><td>&quot;38.3%&quot;</td><td>&quot;58.3%&quot;</td></tr><tr><td>&quot;08021&quot;</td><td>&quot;Sant Gervasi - Galvany&quot;</td><td>10100</td><td>0</td><td>93059.703168</td><td>&quot;4.0%&quot;</td><td>&quot;45.3%&quot;</td><td>&quot;50.6%&quot;</td></tr><tr><td>&quot;08001&quot;</td><td>&quot;Las Ramblas / El Raval&quot;</td><td>10100</td><td>0</td><td>92141.904257</td><td>&quot;1.5%&quot;</td><td>&quot;36.1%&quot;</td><td>&quot;62.4%&quot;</td></tr><tr><td>&quot;08035&quot;</td><td>&quot;Sant Genís dels Agudells / Val…</td><td>10100</td><td>0</td><td>92096.062871</td><td>&quot;0.6%&quot;</td><td>&quot;25.9%&quot;</td><td>&quot;73.4%&quot;</td></tr><tr><td>&quot;08007&quot;</td><td>&quot;Dreta de l&#x27;Eixample / Pg. de G…</td><td>10100</td><td>0</td><td>90117.183564</td><td>&quot;3.3%&quot;</td><td>&quot;26.5%&quot;</td><td>&quot;70.0%&quot;</td></tr><tr><td>&quot;08027&quot;</td><td>&quot;La Sagrera&quot;</td><td>10100</td><td>0</td><td>87294.59802</td><td>&quot;1.0%&quot;</td><td>&quot;52.1%&quot;</td><td>&quot;46.9%&quot;</td></tr><tr><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td></tr><tr><td>&quot;08015&quot;</td><td>null</td><td>4</td><td>0</td><td>0.0</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td></tr><tr><td>&quot;08017&quot;</td><td>null</td><td>4</td><td>0</td><td>0.0</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td></tr><tr><td>&quot;08025&quot;</td><td>null</td><td>4</td><td>0</td><td>0.0</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td></tr><tr><td>&quot;08037&quot;</td><td>null</td><td>4</td><td>0</td><td>0.0</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td></tr><tr><td>&quot;08001&quot;</td><td>null</td><td>4</td><td>0</td><td>0.0</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td></tr><tr><td>&quot;08008&quot;</td><td>null</td><td>4</td><td>0</td><td>0.0</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td></tr><tr><td>&quot;08006&quot;</td><td>null</td><td>4</td><td>0</td><td>0.0</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td></tr><tr><td>&quot;08003&quot;</td><td>null</td><td>4</td><td>0</td><td>0.0</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td></tr><tr><td>&quot;08030&quot;</td><td>null</td><td>4</td><td>0</td><td>0.0</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td></tr><tr><td>&quot;08036&quot;</td><td>null</td><td>4</td><td>0</td><td>0.0</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td></tr><tr><td>&quot;08029&quot;</td><td>null</td><td>4</td><td>0</td><td>0.0</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td></tr><tr><td>&quot;08024&quot;</td><td>null</td><td>4</td><td>0</td><td>0.0</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td></tr><tr><td>&quot;08028&quot;</td><td>null</td><td>4</td><td>0</td><td>0.0</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td></tr><tr><td>&quot;08013&quot;</td><td>null</td><td>4</td><td>0</td><td>0.0</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td></tr><tr><td>&quot;08032&quot;</td><td>null</td><td>4</td><td>0</td><td>0.0</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td></tr><tr><td>&quot;08021&quot;</td><td>null</td><td>4</td><td>0</td><td>0.0</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td></tr><tr><td>&quot;08011&quot;</td><td>null</td><td>56</td><td>0</td><td>0.0</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td></tr><tr><td>&quot;08023&quot;</td><td>null</td><td>4</td><td>0</td><td>0.0</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td></tr><tr><td>&quot;08014&quot;</td><td>null</td><td>4</td><td>0</td><td>0.0</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td></tr><tr><td>&quot;08019&quot;</td><td>null</td><td>4</td><td>0</td><td>0.0</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td></tr><tr><td>&quot;08010&quot;</td><td>null</td><td>4</td><td>0</td><td>0.0</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td></tr><tr><td>&quot;08040&quot;</td><td>null</td><td>4</td><td>0</td><td>0.0</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td></tr><tr><td>&quot;08027&quot;</td><td>null</td><td>4</td><td>0</td><td>0.0</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td></tr><tr><td>&quot;08016&quot;</td><td>null</td><td>4</td><td>0</td><td>0.0</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td></tr><tr><td>&quot;08041&quot;</td><td>null</td><td>4</td><td>0</td><td>0.0</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td><td>&quot;NaN%&quot;</td></tr></tbody></table></div>



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


    
![png](01_eda_electricityBCN_files/01_eda_electricityBCN_75_0.png)
    



    
![png](01_eda_electricityBCN_files/01_eda_electricityBCN_75_1.png)
    



    
![png](01_eda_electricityBCN_files/01_eda_electricityBCN_75_2.png)
    



    
![png](01_eda_electricityBCN_files/01_eda_electricityBCN_75_3.png)
    


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


    
![png](01_eda_electricityBCN_files/01_eda_electricityBCN_77_0.png)
    


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


    
![png](01_eda_electricityBCN_files/01_eda_electricityBCN_79_0.png)
    


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

    shape: (20, 5)
    ┌─────────────────────┬───────────┬───────────────┬─────────────────┬───────────────┐
    │ datetime            ┆ mwh_total ┆ mwh_industria ┆ mwh_residencial ┆ mwh_servicios │
    │ ---                 ┆ ---       ┆ ---           ┆ ---             ┆ ---           │
    │ datetime[μs]        ┆ f64       ┆ f64           ┆ f64             ┆ f64           │
    ╞═════════════════════╪═══════════╪═══════════════╪═════════════════╪═══════════════╡
    │ 2025-08-19 00:00:00 ┆ 0.0       ┆ 0.0           ┆ 0.0             ┆ 0.0           │
    │ 2025-08-30 00:00:00 ┆ 0.0       ┆ 0.0           ┆ 0.0             ┆ 0.0           │
    │ 2025-09-07 00:00:00 ┆ 0.0       ┆ 0.0           ┆ 0.0             ┆ 0.0           │
    │ 2025-08-19 06:00:00 ┆ 0.0       ┆ 0.0           ┆ 0.0             ┆ 0.0           │
    │ 2025-08-30 06:00:00 ┆ 0.0       ┆ 0.0           ┆ 0.0             ┆ 0.0           │
    │ 2025-09-07 06:00:00 ┆ 0.0       ┆ 0.0           ┆ 0.0             ┆ 0.0           │
    │ 2025-08-19 12:00:00 ┆ 0.0       ┆ 0.0           ┆ 0.0             ┆ 0.0           │
    │ 2025-08-30 12:00:00 ┆ 0.0       ┆ 0.0           ┆ 0.0             ┆ 0.0           │
    │ 2025-09-07 12:00:00 ┆ 0.0       ┆ 0.0           ┆ 0.0             ┆ 0.0           │
    │ 2025-08-19 18:00:00 ┆ 0.0       ┆ 0.0           ┆ 0.0             ┆ 0.0           │
    │ 2025-08-30 18:00:00 ┆ 6251.0    ┆ 16.0          ┆ 5415.0          ┆ 820.0         │
    │ 2025-09-07 18:00:00 ┆ 7287.0    ┆ 21.0          ┆ 6334.0          ┆ 932.0         │
    │ 2025-08-20 00:00:00 ┆ 8681.0    ┆ 0.0           ┆ 1101.0          ┆ 7580.0        │
    │ 2025-08-10 00:00:00 ┆ 8990.0    ┆ 0.0           ┆ 1085.0          ┆ 7905.0        │
    │ 2025-08-09 00:00:00 ┆ 9103.0    ┆ 0.0           ┆ 1133.0          ┆ 7970.0        │
    │ 2025-08-08 00:00:00 ┆ 9146.0    ┆ 0.0           ┆ 1095.0          ┆ 8051.0        │
    │ 2025-08-16 00:00:00 ┆ 9215.0    ┆ 0.0           ┆ 967.0           ┆ 8248.0        │
    │ 2025-08-15 00:00:00 ┆ 9305.0    ┆ 0.0           ┆ 1258.0          ┆ 8047.0        │
    │ 2025-08-07 00:00:00 ┆ 9348.0    ┆ 0.0           ┆ 1240.0          ┆ 8108.0        │
    │ 2025-08-17 00:00:00 ┆ 9522.0    ┆ 0.0           ┆ 1197.0          ┆ 8325.0        │
    └─────────────────────┴───────────┴───────────────┴─────────────────┴───────────────┘



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


    
![png](01_eda_electricityBCN_files/01_eda_electricityBCN_86_0.png)
    


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


    
![png](01_eda_electricityBCN_files/01_eda_electricityBCN_89_0.png)
    



    
![png](01_eda_electricityBCN_files/01_eda_electricityBCN_89_1.png)
    



    
![png](01_eda_electricityBCN_files/01_eda_electricityBCN_89_2.png)
    


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

#### Análisis Vibriante

#### Análisis Correlación

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

    /tmp/ipykernel_2175/636030017.py:14: DeprecationWarning: Lag selection has changed to use a data-dependent method. To use the old method that only depends on time, set lags=-1
      kpss_ = KPSS(serie)
    /tmp/ipykernel_2175/636030017.py:14: DeprecationWarning: Lag selection has changed to use a data-dependent method. To use the old method that only depends on time, set lags=-1
      kpss_ = KPSS(serie)
    /tmp/ipykernel_2175/636030017.py:14: DeprecationWarning: Lag selection has changed to use a data-dependent method. To use the old method that only depends on time, set lags=-1
      kpss_ = KPSS(serie)
    /tmp/ipykernel_2175/636030017.py:14: DeprecationWarning: Lag selection has changed to use a data-dependent method. To use the old method that only depends on time, set lags=-1
      kpss_ = KPSS(serie)





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>CP</th>
      <th>Barrio</th>
      <th>ADF stat</th>
      <th>ADF p-valor</th>
      <th>ADF estacionaria</th>
      <th>KPSS stat</th>
      <th>KPSS p-valor</th>
      <th>KPSS estacionaria</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>08038</td>
      <td>Montjuïc / Zona Franca (Industrial)</td>
      <td>-6.387</td>
      <td>0.0000</td>
      <td>Si</td>
      <td>0.616</td>
      <td>0.0205</td>
      <td>No</td>
    </tr>
    <tr>
      <th>1</th>
      <td>08005</td>
      <td>El Poblenou (Mixto)</td>
      <td>-4.354</td>
      <td>0.0004</td>
      <td>Si</td>
      <td>4.114</td>
      <td>0.0001</td>
      <td>No</td>
    </tr>
    <tr>
      <th>2</th>
      <td>08032</td>
      <td>El Carmel / El Guinardó (Residencial)</td>
      <td>-6.604</td>
      <td>0.0000</td>
      <td>Si</td>
      <td>2.145</td>
      <td>0.0001</td>
      <td>No</td>
    </tr>
    <tr>
      <th>3</th>
      <td>08002</td>
      <td>Barri Gòtic (Servicios/Turístico)</td>
      <td>-3.948</td>
      <td>0.0017</td>
      <td>Si</td>
      <td>5.994</td>
      <td>0.0001</td>
      <td>No</td>
    </tr>
  </tbody>
</table>
</div>



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


    
![png](01_eda_electricityBCN_files/01_eda_electricityBCN_100_0.png)
    



```python
print(f"{'lag':>5} │ {'ACF':>8} │ {'PACF':>8}")
print("──────┼──────────┼──────────")
for i in range(30):
    print(f"  {i:3d} │ {acf_vals[i]:>8.4f} │ {pacf_vals[i]:>8.4f}")
```

      lag │      ACF │     PACF
    ──────┼──────────┼──────────
        0 │   1.0000 │   1.0000
        1 │   0.3743 │   0.3743
        2 │  -0.0494 │  -0.2203
        3 │   0.2755 │   0.4651
        4 │   0.8323 │   0.7529
        5 │   0.2450 │  -0.6454
        6 │  -0.1401 │   0.2917
        7 │   0.1885 │   0.0523
        8 │   0.7280 │   0.0126
        9 │   0.1728 │  -0.0807
       10 │  -0.1818 │   0.1091
       11 │   0.1611 │   0.1137
       12 │   0.7024 │   0.0901
       13 │   0.1559 │  -0.1641
       14 │  -0.1924 │   0.1186
       15 │   0.1533 │   0.0757
       16 │   0.6959 │   0.0553
       17 │   0.1516 │  -0.1056
       18 │  -0.1949 │   0.0974
       19 │   0.1529 │   0.0784
       20 │   0.7003 │   0.0754
       21 │   0.1596 │  -0.0873
       22 │  -0.1713 │   0.2027
       23 │   0.2064 │   0.2194
       24 │   0.7827 │   0.1801
       25 │   0.2180 │  -0.2875
       26 │  -0.1247 │   0.3243
       27 │   0.2682 │   0.2375
       28 │   0.8653 │   0.1477
       29 │   0.2664 │  -0.3991


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




<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (6, 12)</small><table border="1" class="dataframe"><thead><tr><th>mwh_industria</th><th>mwh_residencial</th><th>mwh_servicios</th><th>mwh_no_especificado</th><th>lst_celsius</th><th>temp_mean</th><th>temp_max</th><th>temp_min</th><th>humedad_mean</th><th>viento_mean</th><th>precipitacion_sum</th><th>irradiancia_mean</th></tr><tr><td>f64</td><td>f64</td><td>f64</td><td>i64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td></tr></thead><tbody><tr><td>727.0</td><td>28259.0</td><td>39695.0</td><td>null</td><td>12.57</td><td>9.883333</td><td>11.1</td><td>8.8</td><td>59.166667</td><td>0.466667</td><td>0.0</td><td>0.0</td></tr><tr><td>736.0</td><td>25879.0</td><td>37506.0</td><td>null</td><td>12.43</td><td>10.25</td><td>11.0</td><td>9.2</td><td>52.75</td><td>1.158333</td><td>0.0</td><td>0.0</td></tr><tr><td>771.0</td><td>27820.0</td><td>39392.0</td><td>null</td><td>11.49</td><td>8.85</td><td>9.4</td><td>8.0</td><td>53.083333</td><td>1.041667</td><td>0.0</td><td>0.0</td></tr><tr><td>816.0</td><td>29064.0</td><td>39551.0</td><td>null</td><td>11.79</td><td>7.333333</td><td>8.0</td><td>6.7</td><td>61.416667</td><td>0.358333</td><td>0.0</td><td>0.0</td></tr><tr><td>723.0</td><td>31262.0</td><td>41953.0</td><td>null</td><td>10.38</td><td>8.25</td><td>8.8</td><td>7.5</td><td>66.416667</td><td>2.666667</td><td>0.0</td><td>0.0</td></tr><tr><td>729.0</td><td>30869.0</td><td>41162.0</td><td>null</td><td>13.57</td><td>11.008333</td><td>11.5</td><td>10.4</td><td>38.916667</td><td>1.191667</td><td>0.0</td><td>0.0</td></tr></tbody></table></div>




```python
df_num.describe()
```




<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (9, 13)</small><table border="1" class="dataframe"><thead><tr><th>statistic</th><th>mwh_industria</th><th>mwh_residencial</th><th>mwh_servicios</th><th>mwh_no_especificado</th><th>lst_celsius</th><th>temp_mean</th><th>temp_max</th><th>temp_min</th><th>humedad_mean</th><th>viento_mean</th><th>precipitacion_sum</th><th>irradiancia_mean</th></tr><tr><td>str</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td></tr></thead><tbody><tr><td>&quot;count&quot;</td><td>424368.0</td><td>424368.0</td><td>424368.0</td><td>62112.0</td><td>255324.0</td><td>396343.0</td><td>287621.0</td><td>287621.0</td><td>396350.0</td><td>270947.0</td><td>287720.0</td><td>270952.0</td></tr><tr><td>&quot;null_count&quot;</td><td>0.0</td><td>0.0</td><td>0.0</td><td>362256.0</td><td>169044.0</td><td>28025.0</td><td>136747.0</td><td>136747.0</td><td>28018.0</td><td>153421.0</td><td>136648.0</td><td>153416.0</td></tr><tr><td>&quot;mean&quot;</td><td>8754.498085</td><td>35469.160324</td><td>56611.347499</td><td>495.403465</td><td>24.770183</td><td>17.873223</td><td>19.235217</td><td>16.509833</td><td>67.653776</td><td>2.540296</td><td>0.331734</td><td>186.820934</td></tr><tr><td>&quot;std&quot;</td><td>19266.60245</td><td>19483.138765</td><td>39208.184946</td><td>2653.710144</td><td>9.119631</td><td>6.281812</td><td>6.321543</td><td>6.173686</td><td>15.411539</td><td>1.638099</td><td>2.333177</td><td>227.300742</td></tr><tr><td>&quot;min&quot;</td><td>0.0</td><td>0.0</td><td>0.0</td><td>0.0</td><td>0.77</td><td>-0.258333</td><td>0.2</td><td>-5.7</td><td>4.0</td><td>0.0</td><td>0.0</td><td>-4.272727</td></tr><tr><td>&quot;25%&quot;</td><td>760.0</td><td>21467.0</td><td>27130.0</td><td>75.0</td><td>16.69</td><td>12.972727</td><td>14.2</td><td>11.7</td><td>57.0</td><td>1.408333</td><td>0.0</td><td>0.0</td></tr><tr><td>&quot;50%&quot;</td><td>1785.0</td><td>33088.0</td><td>47647.0</td><td>161.0</td><td>25.22</td><td>17.383333</td><td>18.8</td><td>15.9</td><td>68.0</td><td>2.166667</td><td>0.0</td><td>27.083333</td></tr><tr><td>&quot;75%&quot;</td><td>4372.0</td><td>46412.0</td><td>76183.0</td><td>341.0</td><td>32.61</td><td>23.025</td><td>24.3</td><td>21.6</td><td>78.75</td><td>3.225</td><td>0.0</td><td>352.333333</td></tr><tr><td>&quot;max&quot;</td><td>434408.0</td><td>260278.0</td><td>425217.0</td><td>84825.0</td><td>45.76</td><td>36.0</td><td>38.4</td><td>34.6</td><td>100.0</td><td>17.275</td><td>66.3</td><td>917.6</td></tr></tbody></table></div>



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




<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (1, 13)</small><table border="1" class="dataframe"><thead><tr><th>mwh_industria</th><th>mwh_residencial</th><th>mwh_servicios</th><th>mwh_no_especificado</th><th>lst_celsius</th><th>temp_mean</th><th>temp_max</th><th>temp_min</th><th>humedad_mean</th><th>viento_mean</th><th>precipitacion_sum</th><th>irradiancia_mean</th><th>mwh_total</th></tr><tr><td>u32</td><td>u32</td><td>u32</td><td>u32</td><td>u32</td><td>u32</td><td>u32</td><td>u32</td><td>u32</td><td>u32</td><td>u32</td><td>u32</td><td>u32</td></tr></thead><tbody><tr><td>0</td><td>0</td><td>0</td><td>362256</td><td>169044</td><td>28025</td><td>136747</td><td>136747</td><td>28018</td><td>153421</td><td>136648</td><td>153416</td><td>0</td></tr></tbody></table></div>



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
    ax2.tick_params(axis='x', rotation=45, labelsize=7)
    
    ax2 = fig.add_subplot(n_rows * 2, n_cols, (i // n_cols) * n_cols * 2 + n_cols + (i % n_cols) + 1)
    ax2.boxplot(v, vert=False, patch_artist=True,
                boxprops=dict(facecolor='#264653', alpha=0.7),
                medianprops=dict(color='#E9C46A', linewidth=2),
                flierprops=dict(marker='o', markersize=1.5, alpha=0.3))

plt.tight_layout()
plt.show()
```


    
![png](01_eda_electricityBCN_files/01_eda_electricityBCN_112_0.png)
    


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


    
![png](01_eda_electricityBCN_files/01_eda_electricityBCN_115_0.png)
    


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


    
![png](01_eda_electricityBCN_files/01_eda_electricityBCN_119_0.png)
    


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

    variable                    mediana_bajo   mediana_alto        p_value   sig
    --------------------------------------------------------------------------------
    temp_mean                         16.575         19.017     0.0000e+00     ✓
    temp_max                          17.900         20.500     0.0000e+00     ✓
    temp_min                          15.300         17.600     0.0000e+00     ✓
    humedad_mean                      70.833         65.417     0.0000e+00     ✓
    irradiancia_mean                   7.250        192.750     0.0000e+00     ✓
    viento_mean                        2.325          2.100    4.0967e-299     ✓
    lst_celsius                       24.620         27.160    8.2638e-172     ✓
    precipitacion_sum                  0.000          0.000     7.6406e-49     ✓


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



