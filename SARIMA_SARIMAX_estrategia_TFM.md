# SARIMA / SARIMAX — Estrategia de modelado (Baseline TFM)

**Fuente:** cienciadedatos.net/documentos/py51-modelos-arima-sarimax-python  
**Librería de referencia:** skforecast >= 0.19 (docs: skforecast.org)  
**Autor original:** Joaquin Amat Rodrigo  
**TFM:** Sistema de Prediccion Energetica y Alerta Temprana en Barrios de Barcelona  
**Supervisor:** Jose | **Deadline memoria:** 12/06/2026 | **Defensa:** julio 2026

> ⚠️ **Nota de versión crítica:** Desde skforecast 0.19, `ForecasterSarimax` y el módulo
> `model_selection_sarimax` están **deprecados**. La API actual usa `ForecasterStats` +
> `TimeSeriesFold` + `backtesting_stats` + `grid_search_stats`. Todo el código de este
> documento usa la API vigente.

---

## 0. Reglas de negocio (directiva Jose 03/06/2026)

- **NO usar best_model directamente** del grid search.
- **Guardar TODOS los resultados** del grid search en CSV.
- **Graficar** diferencia relativa train vs. validacion para seleccionar modelo.
- **Criterio de seleccion:** mejor R2 + menor diferencia relativa train/val.
- **Metrica primaria:** maximizar R2.
- **2026 = out-of-sample exclusivo** para backtesting final. Nunca en grid search.
- **Pruebas de estacionariedad:** documentar ADF y KPSS explicitamente en la memoria.

---

## 1. Estructura del modelo SARIMA

```
SARIMA(p, d, q)(P, D, Q)[m]
```

| Parametro | Significado |
|-----------|-------------|
| `p` | Orden AR ordinario: cuantos lags del pasado entran al modelo |
| `d` | Diferenciacion ordinaria: veces que se diferencia para estacionariedad en media |
| `q` | Orden MA ordinario: cuantos errores pasados entran al modelo |
| `P` | Orden AR estacional |
| `D` | Diferenciacion estacional |
| `Q` | Orden MA estacional |
| `m` | Periodo estacional: observaciones por ciclo |

### Valor de m para este TFM

- Frecuencia de datos: **6 horas** (4 bloques por dia)
- `m=4` captura estacionalidad **diaria**
- `m=28` captura estacionalidad **semanal** (4 bloques/dia × 7 dias) ← **principal**
- Lags confirmados en EDA: `lag_1` (PACF 0.37), `lag_4` (PACF 0.75), `lag_28` (ACF 0.87)
- Probar `m=28` como primera opcion, `m=4` como alternativa

---

## 2. Pruebas de estacionariedad

Documentar ambas en la memoria con formula y resultado.

### ADF (Augmented Dickey-Fuller)

- **H0:** la serie tiene raiz unitaria (NO es estacionaria en media)
- Si `p-value < 0.05` → rechazamos H0 → serie ES estacionaria en media
- **Resultado EDA:** ADF confirma estacionariedad en media → `d=0` probablemente suficiente

### KPSS (Kwiatkowski-Phillips-Schmidt-Shin)

- **H0:** la serie ES estacionaria
- Si `p-value < 0.05` → rechazamos H0 → serie NO es estacionaria en varianza
- **Resultado EDA:** KPSS rechaza estacionariedad en varianza (breaks: COVID, variabilidad estacional)
- → **Aplicar `log(mwh_total)` antes de SARIMA** y revertir con `exp()` al evaluar

```python
from statsmodels.tsa.stattools import adfuller, kpss

# ADF
adf_result = adfuller(y_train, autolag='AIC')
print(f"ADF p-value: {adf_result[1]:.4f}")   # < 0.05 → estacionaria en media

# KPSS
kpss_result = kpss(y_train, regression='c', nlags='auto')
print(f"KPSS p-value: {kpss_result[1]:.4f}") # < 0.05 → NO estacionaria en varianza
```

---

## 3. Librerias necesarias

```bash
pip install "skforecast>=0.19" pmdarima statsmodels scipy scikit-learn
```

```python
# ── Imports principales (API skforecast >= 0.19) ──────────────────────────────
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import itertools
import csv
import os
import warnings
warnings.filterwarnings('ignore')

# skforecast — API actual (>= 0.19)
from skforecast.stats import Sarimax            # wrapper de statsmodels SARIMAX
from skforecast.recursive import ForecasterStats # reemplaza ForecasterSarimax
from skforecast.model_selection import (
    TimeSeriesFold,      # encapsula la estrategia de CV
    backtesting_stats,   # reemplaza backtesting_sarimax
    grid_search_stats    # reemplaza grid_search_sarimax
)

# statsmodels (para ajuste directo y diagnosticos)
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.stattools import adfuller, kpss

# metricas
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error
```

> **Por qué estos imports y no los anteriores:**
> `ForecasterSarimax` y `model_selection_sarimax` fueron deprecados en skforecast 0.19.
> La nueva clase `ForecasterStats` ofrece mejor rendimiento y compatibilidad más amplia
> con modelos estadísticos. `TimeSeriesFold` ahora encapsula todos los parámetros del CV.

---

## 4. Preparacion de datos

### 4.1 Carga desde MongoDB

```python
from pymongo import MongoClient
import polars as pl

client = MongoClient("mongodb://mongo:27017/")
db = client["tfm_energy"]

CP = "08005"  # El Poblenou — CP representativo mixto
# CPs representativos: 08038 (industrial), 08005 (mixto), 08032 (residencial), 08002 (servicios)

cursor = db["dataset_clean"].find(
    {"cod_postal": CP},
    {"_id": 0, "timestamp": 1, "mwh_total": 1, "temp_mean": 1,
     "HDD": 1, "CDD": 1, "lst_celsius": 1, "lst_nublado": 1,
     "hora": 1, "es_finde": 1, "festivo": 1, "is_covid": 1}
)

df_pd = pl.from_arrow(cursor).to_pandas()
df_pd["timestamp"] = pd.to_datetime(df_pd["timestamp"])
df_pd = df_pd.sort_values("timestamp").set_index("timestamp")
df_pd = df_pd.asfreq("6H")
```

### 4.2 Split temporal

> REGLA: **2026 = out-of-sample**. Nunca entra en grid search ni seleccion de hiperparametros.

```python
# Splits
train_val = df_pd[df_pd.index.year < 2026]
test_2026  = df_pd[df_pd.index.year == 2026]

# Split interno para grid search (ultimos 3 meses de 2025 = validacion)
end_train = "2025-09-30"
end_val   = "2025-12-31"

train = train_val[train_val.index <= end_train]
val   = train_val[(train_val.index > end_train) & (train_val.index <= end_val)]

y_train = train["mwh_total"]
y_val   = val["mwh_total"]
y_test  = test_2026["mwh_total"]

print(f"Train: {y_train.index[0]} -> {y_train.index[-1]} ({len(y_train)} obs)")
print(f"Val:   {y_val.index[0]} -> {y_val.index[-1]} ({len(y_val)} obs)")
print(f"Test:  {y_test.index[0]} -> {y_test.index[-1]} ({len(y_test)} obs)")
```

### 4.3 Log-transform (KPSS lo justifica)

```python
y_train_log = np.log(y_train)
y_val_log   = np.log(y_val)
y_test_log  = np.log(y_test)

# Al evaluar metricas finales: revertir con np.exp(pred_log)
```

---

## 5. ACF y PACF — orden inicial

```python
fig, axes = plt.subplots(2, 1, figsize=(12, 8))
plot_acf(y_train_log, lags=60, ax=axes[0], title="ACF - mwh_total (log)")
plot_pacf(y_train_log, lags=60, ax=axes[1], title="PACF - mwh_total (log)")
plt.tight_layout()
plt.savefig("acf_pacf_analysis.png", dpi=150, bbox_inches="tight")
```

| Lag confirmado EDA | Interpretacion para SARIMA |
|--------------------|----------------------------|
| lag_1 (PACF 0.37) | Componente AR → `p >= 1` probable |
| lag_4 (PACF 0.75) | Estacionalidad diaria fuerte → `P >= 1` con `m=4` |
| lag_28 (ACF 0.87) | Estacionalidad semanal muy fuerte → `m=28` es el periodo principal |

---

## 6. Grid search con skforecast (API >= 0.19)

La metodologia de Amat Rodrigo usa `grid_search_stats` con un `TimeSeriesFold` para
evaluar combinaciones via backtesting walk-forward. El `param_grid` incluye `'m'` como
clave separada (no embebida en `seasonal_order`).

### 6.1 Definir el espacio de busqueda

```python
# Espacio de hiperparametros
# 'm' va como clave separada en param_grid, NO dentro de seasonal_order
param_grid = {
    "order": list(itertools.product([0, 1, 2], [0, 1], [0, 1, 2])),
    "seasonal_order": list(itertools.product([0, 1], [0, 1], [0, 1])),
    "m": [28]    # probar tambien [4] si m=28 no converge
}
```

### 6.2 Ejecutar grid search

```python
# Forecaster con placeholder (los params se sobreescriben en el grid search)
forecaster = ForecasterStats(
    estimator=Sarimax(
        order=(1, 0, 1),
        seasonal_order=(1, 1, 1),
        m=28,
        enforce_stationarity=False,
        enforce_invertibility=False
    )
)

# CV: solo sobre train (no incluir val ni test en y)
cv_grid = TimeSeriesFold(
    steps              = 28,               # horizonte: 7 dias (28 bloques de 6h)
    initial_train_size = len(y_train) - 28 * 4,  # deja 4 ventanas para CV interno
    refit              = False,
    fixed_train_size   = False
)

results_grid = grid_search_stats(
    forecaster             = forecaster,
    y                      = y_train,      # SOLO train, nunca val ni test
    cv                     = cv_grid,
    param_grid             = param_grid,
    metric                 = "mean_absolute_error",  # metrica de busqueda
    return_best            = False,        # NO usar best directamente (directiva Jose)
    n_jobs                 = "auto",
    suppress_warnings_fit  = True,
    verbose                = False,
    show_progress          = True
)

# GUARDAR TODOS LOS RESULTADOS
results_grid.to_csv(f"grid_search_results_SARIMA_{CP}.csv", index=False)
print(f"Grid search completado: {len(results_grid)} combinaciones evaluadas.")
print(results_grid.head(10))
```

> **Por qué `return_best=False`:** la directiva de Jose exige no usar el mejor modelo
> directamente. Primero hay que calcular la diferencia relativa train/val y seleccionar
> manualmente con el criterio de menor overfitting.

### 6.3 Calcular R2 en train y validacion para cada combinacion

El grid search de skforecast reporta MAE por defecto. Para calcular R2 y la diferencia
relativa train/val, ajustamos cada combinacion del CSV manualmente:

```python
df_results = pd.read_csv(f"grid_search_results_SARIMA_{CP}.csv")

r2_vals   = []
r2_trains = []

for _, row in df_results.iterrows():
    try:
        model = SARIMAX(
            y_train,
            order=eval(row["order"]) if isinstance(row["order"], str) else row["order"],
            seasonal_order=(*eval(row["seasonal_order"]), int(row["m"])),
            enforce_stationarity=False,
            enforce_invertibility=False
        ).fit(disp=False)

        pred_val   = model.forecast(steps=len(y_val))
        pred_train = model.fittedvalues[-len(y_val):]

        r2_val   = r2_score(y_val, pred_val)
        r2_train = r2_score(y_train[-len(y_val):], pred_train)

        r2_vals.append(round(r2_val, 4))
        r2_trains.append(round(r2_train, 4))
    except Exception:
        r2_vals.append(None)
        r2_trains.append(None)

df_results["r2_val"]   = r2_vals
df_results["r2_train"] = r2_trains
df_results["rel_diff"] = (
    (df_results["r2_train"] - df_results["r2_val"]).abs()
    / df_results["r2_train"].abs().clip(lower=1e-10)
).round(4)

df_results.to_csv(f"grid_search_results_SARIMA_{CP}.csv", index=False)
```

### 6.4 Fallback: grid search manual con statsmodels

Si skforecast da problemas de convergencia o version, implementar directo:

```python
results = []
combinations = list(itertools.product(
    range(0, 3),  # p
    range(0, 2),  # d
    range(0, 3),  # q
    range(0, 2),  # P
    range(0, 2),  # D
    range(0, 2),  # Q
))
m = 28

for (p, d, q, P, D, Q) in combinations:
    try:
        model = SARIMAX(
            y_train,
            order=(p, d, q),
            seasonal_order=(P, D, Q, m),
            enforce_stationarity=False,
            enforce_invertibility=False
        ).fit(disp=False)

        pred_val   = model.forecast(steps=len(y_val))
        pred_train = model.fittedvalues[-len(y_val):]

        r2_val   = r2_score(y_val, pred_val)
        r2_train = r2_score(y_train[-len(y_val):], pred_train)
        rel_diff = abs(r2_train - r2_val) / max(abs(r2_train), 1e-10)

        results.append({
            "p": p, "d": d, "q": q,
            "P": P, "D": D, "Q": Q, "m": m,
            "r2_val":   round(r2_val, 4),
            "r2_train": round(r2_train, 4),
            "rel_diff": round(rel_diff, 4),
            "mse_val":  round(mean_squared_error(y_val, pred_val), 2),
            "aic":      round(model.aic, 2)
        })
        print(f"SARIMA({p},{d},{q})({P},{D},{Q})[{m}] R2_val={r2_val:.4f} rel_diff={rel_diff:.4f}")
    except Exception as e:
        print(f"Error ({p},{d},{q})({P},{D},{Q})[{m}]: {e}")
        continue

keys = ["p","d","q","P","D","Q","m","r2_val","r2_train","rel_diff","mse_val","aic"]
with open(f"grid_search_results_SARIMA_{CP}.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=keys)
    writer.writeheader()
    writer.writerows(results)
```

---

## 7. Seleccion del modelo (criterio Jose)

### 7.1 Grafico diferencia relativa train vs. validacion

```python
df_results = pd.read_csv(f"grid_search_results_SARIMA_{CP}.csv")
df_results = df_results.dropna(subset=["r2_val", "rel_diff"])
df_results = df_results.sort_values("r2_val", ascending=False).reset_index(drop=True)

fig, ax1 = plt.subplots(figsize=(14, 6))
ax2 = ax1.twinx()

x = range(len(df_results))
ax1.bar(x, df_results["r2_val"], alpha=0.7, color="#2471A3", label="R2 Validacion")
ax2.plot(x, df_results["rel_diff"], color="#C0392B", marker="o", markersize=3, label="Dif. relativa train/val")

ax1.set_xlabel("Combinacion (ordenada por R2_val)")
ax1.set_ylabel("R2 Validacion", color="#2471A3")
ax2.set_ylabel("Diferencia relativa", color="#C0392B")
ax1.set_title(f"Grid Search SARIMA — Seleccion de modelo — CP {CP}")

lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right")
plt.tight_layout()
plt.savefig(f"grid_search_plot_SARIMA_{CP}.png", dpi=150, bbox_inches="tight")
plt.show()
```

### 7.2 Seleccion programatica

```python
UMBRAL_REL_DIFF = 0.10  # max 10% de diferencia tolerable entre train y val

df_candidates = df_results[df_results["rel_diff"] <= UMBRAL_REL_DIFF]

if df_candidates.empty:
    print("Sin candidatos bajo umbral. Usando top-5 con menor rel_diff.")
    df_candidates = df_results.nsmallest(5, "rel_diff")

best = df_candidates.sort_values("r2_val", ascending=False).iloc[0]

print(f"Modelo seleccionado: SARIMA({int(best.p)},{int(best.d)},{int(best.q)})({int(best.P)},{int(best.D)},{int(best.Q)})[{int(best.m)}]")
print(f"  R2_val    = {best.r2_val:.4f}")
print(f"  R2_train  = {best.r2_train:.4f}")
print(f"  Rel. diff = {best.rel_diff:.4f}")
```

---

## 8. SARIMAX — variables exogenas

### 8.1 Variables permitidas

| Variable | Descripcion |
|----------|-------------|
| `temp_mean` | Temperatura media del bloque 6h |
| `HDD` | `max(0, 15 - temp_mean)` — proxy calefaccion |
| `CDD` | `max(0, temp_mean - 22)` — proxy aire acondicionado |
| `lst_celsius` | MODIS LST en Celsius — contribucion original TFM |
| `lst_nublado` | Flag binario: 1 si LST no disponible por nubes |
| `hora` | Hora del dia (0, 6, 12, 18) |
| `es_finde` | 1 si sabado o domingo |
| `festivo` | 1 si dia festivo BCN |
| `is_covid` | 1 para periodo 2020 |

> **LEAKAGE — NO usar:**
> `pct_industria = mwh_industria / mwh_total` usa el target → excluido.
> `mwh_no_especificado` → excluido por cambio de schema septiembre 2025.

### 8.2 Ajuste SARIMAX

```python
exog_cols = ["temp_mean", "HDD", "CDD", "lst_celsius", "lst_nublado",
             "hora", "es_finde", "festivo", "is_covid"]

exog_train = train[exog_cols]
exog_val   = val[exog_cols]
exog_test  = test_2026[exog_cols]

model_sarimax = SARIMAX(
    y_train,
    exog=exog_train,
    order=(int(best.p), int(best.d), int(best.q)),
    seasonal_order=(int(best.P), int(best.D), int(best.Q), int(best.m)),
    enforce_stationarity=False,
    enforce_invertibility=False
).fit(disp=False)

print(model_sarimax.summary())
```

### 8.3 Prediccion y metricas en validacion

```python
pred_val = model_sarimax.forecast(steps=len(y_val), exog=exog_val)

r2_val   = r2_score(y_val, pred_val)
mae_val  = mean_absolute_error(y_val, pred_val)
rmse_val = np.sqrt(mean_squared_error(y_val, pred_val))
mape_val = np.mean(np.abs((y_val - pred_val) / y_val)) * 100

print(f"R2={r2_val:.4f} | MAE={mae_val:.2f} | RMSE={rmse_val:.2f} | MAPE={mape_val:.2f}%")
```

---

## 9. Backtesting out-of-sample (2026) con skforecast >= 0.19

```python
# Forecaster final con los parametros seleccionados
forecaster_final = ForecasterStats(
    estimator=Sarimax(
        order=(int(best.p), int(best.d), int(best.q)),
        seasonal_order=(int(best.P), int(best.D), int(best.Q)),
        m=int(best.m),
        enforce_stationarity=False,
        enforce_invertibility=False
    )
)

# Serie completa hasta fin de 2025 + test 2026
y_all    = pd.concat([y_train, y_val])
exog_all = pd.concat([exog_train, exog_val])

# TimeSeriesFold: el modelo se entrena con todo hasta 2025, predice en 2026
cv_test = TimeSeriesFold(
    steps              = 28,         # horizonte: 7 dias
    initial_train_size = len(y_all), # todo 2019-2025 como train inicial
    refit              = True,       # re-entrenar en cada ventana
    fixed_train_size   = False
)

metric_backtest, pred_backtest = backtesting_stats(
    forecaster            = forecaster_final,
    y                     = pd.concat([y_all, y_test]),
    exog                  = pd.concat([exog_all, exog_test]),
    cv                    = cv_test,
    metric                = ["r2", "mean_absolute_error", "mean_squared_error"],
    n_jobs                = "auto",
    suppress_warnings_fit = True,
    verbose               = True,
    show_progress         = True
)

print("Backtesting 2026 (out-of-sample):")
print(metric_backtest)
```

---

## 10. Guardar metricas para comparacion de modelos

```python
metrics_report = {
    "modelo":         "SARIMAX",
    "cp":             CP,
    "order":          f"({int(best.p)},{int(best.d)},{int(best.q)})",
    "seasonal_order": f"({int(best.P)},{int(best.D)},{int(best.Q)})[{int(best.m)}]",
    "r2_val":         round(r2_val, 4),
    "mae_val":        round(mae_val, 2),
    "rmse_val":       round(rmse_val, 2),
    "mape_val":       round(mape_val, 2),
    "r2_train":       round(r2_score(y_train, model_sarimax.fittedvalues), 4),
    "aic":            round(model_sarimax.aic, 2)
}

csv_path = "model_comparison_results.csv"
file_exists = os.path.isfile(csv_path)
with open(csv_path, "a", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=metrics_report.keys())
    if not file_exists:
        writer.writeheader()
    writer.writerow(metrics_report)

print(f"Metricas guardadas en {csv_path}")
```

---

## 11. Diagnostico de residuos

```python
fig = model_sarimax.plot_diagnostics(figsize=(16, 10))
plt.suptitle(f"Diagnostico residuos SARIMAX — CP {CP}", y=1.02)
plt.tight_layout()
plt.savefig(f"residuals_SARIMAX_{CP}.png", dpi=150, bbox_inches="tight")
```

---

## 12. Formulas LaTeX para la memoria

```latex
% R2
R^2 = 1 - \frac{\sum_{t=1}^{n}(y_t - \hat{y}_t)^2}{\sum_{t=1}^{n}(y_t - \bar{y})^2}

% MAE
\text{MAE} = \frac{1}{n} \sum_{t=1}^{n} |y_t - \hat{y}_t|

% RMSE
\text{RMSE} = \sqrt{\frac{1}{n} \sum_{t=1}^{n} (y_t - \hat{y}_t)^2}

% MAPE
\text{MAPE} = \frac{1}{n} \sum_{t=1}^{n} \left|\frac{y_t - \hat{y}_t}{y_t}\right| \times 100

% Diferencia relativa train/val
\text{Dif. relativa} = \frac{|R^2_{\text{train}} - R^2_{\text{val}}|}{\max(|R^2_{\text{train}}|, \varepsilon)}
```

---

## 13. Checklist antes de reportar

- [ ] ADF ejecutado y resultado documentado en la memoria
- [ ] KPSS ejecutado y resultado documentado en la memoria
- [ ] Decision sobre log-transform documentada y justificada
- [ ] Grid search completo guardado en CSV (`grid_search_results_SARIMA_<CP>.csv`)
- [ ] R2 train/val y diferencia relativa calculados para cada combinacion
- [ ] Grafico diferencia relativa generado y guardado
- [ ] Modelo seleccionado con criterio: mejor R2 + menor diferencia relativa
- [ ] SARIMA ajustado y evaluado en validacion
- [ ] SARIMAX ajustado con variables exogenas y evaluado en validacion
- [ ] Backtesting walk-forward ejecutado con datos 2026 via `backtesting_stats`
- [ ] Metricas guardadas en `model_comparison_results.csv`
- [ ] Diagnostico de residuos generado (`plot_diagnostics`)
- [ ] Formulas LaTeX de cada metrica incluidas en la memoria
- [ ] Codigo con black + isort antes de commit
- [ ] Notebook con outputs limpios antes de commit a develop

---

*TFM — La Salle (Universitat Ramon Llull) — 2026*
