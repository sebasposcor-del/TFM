# Directivas de José — Modelado (tutoría 03/06/2026)

> Estas son las instrucciones exactas de José para la fase de modelado del TFM.
> Seguirlas al pie de la letra es obligatorio antes de la defensa.

---

## 1. Modelos baseline

- Usar **SARIMA y SARIMAX** como modelos baseline. Nada más.
- **Prophet está descartado** del scope de modelado. No incluirlo.

---

## 2. Datos para backtesting

- Usar **datos de 2025 (periodo más reciente disponible)** para el backtesting out-of-sample.
- El periodo de test **nunca se toca** durante el entrenamiento ni la selección de hiperparámetros.

---

## 3. Métrica primaria

- La métrica principal es **R²**. El objetivo es **maximizarlo**.
- Reportar también MAE, RMSE y MAPE, pero la decisión de qué modelo es mejor se basa en R².

---

## 4. Grid search — regla crítica

> Esta es la regla más importante. No saltársela.

- **NO usar `best_model` directamente** después del grid search.
- El proceso correcto es:
  1. Guardar **todos** los resultados del grid search en un CSV.
  2. Calcular la **diferencia relativa entre R² train y R² validación** para cada combinación.
  3. Graficar esa diferencia relativa para ver qué modelos tienen overfitting.
  4. Seleccionar el modelo con **mejor R² de validación + menor diferencia relativa train/val**.

```python
# Criterio de selección programático
UMBRAL_REL_DIFF = 0.10  # max 10% de diferencia tolerable

df_candidates = df_results[df_results["rel_diff"] <= UMBRAL_REL_DIFF]
best = df_candidates.sort_values("r2_val", ascending=False).iloc[0]
```

**Por qué:** un modelo con R² val=0.92 pero R² train=0.99 tiene overfitting claro.
Uno con R² val=0.89 y R² train=0.91 es más fiable aunque su métrica sea menor.

---

## 5. Pruebas de estacionariedad — documentar en la memoria

Definir explícitamente en el documento cuáles se usan y por qué:

- **ADF (Augmented Dickey-Fuller):** confirmar estacionariedad en media.
- **KPSS (Kwiatkowski-Phillips-Schmidt-Shin):** confirmar estacionariedad en varianza.

Ambas deben aparecer con su fórmula matemática y el resultado obtenido en los datos.

---

## 6. Documento / memoria

- Incluir **todos los related works** con referencias completas (IEEE).
- Añadir **fórmulas matemáticas completas** de cada métrica (R², MAE, RMSE, MAPE).
- Añadir **descripción técnica** de cada modelo (SARIMA, SARIMAX, XGBoost, LSTM/GRU).
- **Migrar todo a LaTeX** y subir a Overleaf.

---

## Resumen del flujo de modelado según José

```
1. ADF + KPSS sobre la serie
        ↓
2. ACF / PACF para orden inicial (p, q, P, Q)
        ↓
3. Grid search → guardar TODO en CSV
        ↓
4. Calcular diferencia relativa train/val para cada combinación
        ↓
5. Graficar → identificar candidatos sin overfitting
        ↓
6. Seleccionar: mejor R²_val + menor diferencia relativa
        ↓
7. Ajustar SARIMAX con variables exógenas
        ↓
8. Backtesting final con datos de test (oct-dic 2025)
        ↓
9. Reportar métricas en la memoria
```

---

*Tutoría José — 03/06/2026*
