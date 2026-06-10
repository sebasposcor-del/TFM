import sys, time, warnings
import polars as pl, pandas as pd, numpy as np
from pymongo import MongoClient
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
from joblib import Parallel, delayed
warnings.filterwarnings('ignore')

N_CPS = int(sys.argv[1]) if len(sys.argv) > 1 else None   # limita CPs (test). None = los 42

client = MongoClient('mongodb://mongo:27017/'); db = client['tfm_energy']
df = pl.DataFrame(list(db['dataset_features'].find({}, {'_id': 0})), infer_schema_length=None)
print('Shape:', df.shape, flush=True)

FIN_VAL = '2025-09-30 18:00:00'; S = 28; INI = '2019-01-01'   # historico completo (oficial)
ORDER_SEL = (2, 0, 1); SORDER_SEL = (1, 1, 1, S)
HORIZONTES = {'24h': 4, '48h': 8, '72h': 12}
EXOG_COLS = ['HDD', 'CDD', 'humedad_mean', 'es_festivo', 'is_covid']
CPS_TODOS = sorted(df['cod_postal'].unique().to_list())
if N_CPS:
    CPS_TODOS = CPS_TODOS[:N_CPS]

def get_serie(cp):
    s = (df.filter(pl.col('cod_postal') == cp).sort('datetime')
           .select(['datetime', 'mwh_total']).to_pandas().set_index('datetime')['mwh_total'])
    return s.asfreq('6h')

def get_exog(cp):
    e = (df.filter(pl.col('cod_postal') == cp).sort('datetime')
           .select(['datetime', 'hora'] + EXOG_COLS).to_pandas().set_index('datetime').asfreq('6h'))
    e = e.ffill().bfill()
    ang = 2 * np.pi * e['hora'] / 24
    e['sin_dia'] = np.sin(ang); e['cos_dia'] = np.cos(ang)
    return e.drop(columns=['hora'])

def metricas(y_true, y_pred):
    y_true = np.asarray(y_true, float); y_pred = np.asarray(y_pred, float)
    mask = ~np.isnan(y_true) & ~np.isnan(y_pred); y_true, y_pred = y_true[mask], y_pred[mask]
    denom = np.where(y_true == 0, np.nan, y_true)
    return {'r2': r2_score(y_true, y_pred), 'mae': mean_absolute_error(y_true, y_pred),
            'rmse': float(np.sqrt(mean_squared_error(y_true, y_pred))),
            'mape': float(np.nanmean(np.abs((y_true - y_pred) / denom)) * 100)}

def ajustar_sarimax(serie, order, so, exog=None):
    return SARIMAX(serie, exog=exog, order=order, seasonal_order=so,
                   enforce_stationarity=False, enforce_invertibility=False).fit(disp=False)

def backtest(serie_full, params, order, sorder, n_test, steps, exog_full=None):
    # RAPIDO: filtra la serie completa UNA vez con los params del train y hace
    # forecast a 'steps' desde cada origen via predict(dynamic=True). dynamic=True
    # condiciona en los reales ANTES del origen (walk-forward, sin leakage) y NO
    # reconstruye el espacio de estados por ventana. Robusto (no usa res.extend).
    res = SARIMAX(serie_full, exog=exog_full, order=order, seasonal_order=sorder,
                  enforce_stationarity=False, enforce_invertibility=False).filter(params)
    ev = np.asarray(serie_full, float)
    n_total = len(ev); n_train = n_total - n_test
    yt, yp = [], []; i = n_train
    while i < n_total:
        h = min(steps, n_total - i)
        pred = res.predict(start=i, end=i + h - 1, dynamic=True)
        yp.extend(np.asarray(pred)[:h]); yt.extend(ev[i:i+h])
        i += h
    return np.array(yt), np.array(yp)

datos = {cp: (get_serie(cp), get_exog(cp)) for cp in CPS_TODOS}
print('datos extraidos:', len(datos), 'CPs', flush=True)

t0 = time.time()
def _eval(cp, serie, exog):
    s_full = serie.loc[INI:]                 # train + test (desde INI)
    s_trv  = serie.loc[INI:FIN_VAL]          # train (para estimar params)
    n_test = len(serie.loc[FIN_VAL:].iloc[1:])
    e_full = exog.loc[INI:]; e_trv = exog.loc[INI:FIN_VAL]
    ev = np.asarray(s_full, float); n_total = len(ev); n_train = n_total - n_test
    out = []
    for nombre, exf, ext in [('SARIMA', None, None), ('SARIMAX', e_full, e_trv)]:
        try:
            params = ajustar_sarimax(s_trv, ORDER_SEL, SORDER_SEL, exog=ext).params   # 1 fit
            res = SARIMAX(s_full, exog=exf, order=ORDER_SEL, seasonal_order=SORDER_SEL,
                          enforce_stationarity=False, enforce_invertibility=False).filter(params)  # 1 filter
            for hn, st in HORIZONTES.items():                                # reusa res en los 3 horizontes
                yt, yp = [], []; i = n_train
                while i < n_total:
                    h = min(st, n_total - i)
                    yp.extend(np.asarray(res.predict(start=i, end=i + h - 1, dynamic=True))[:h])
                    yt.extend(ev[i:i+h])
                    i += h
                m = metricas(yt, yp); m.update(cp=cp, modelo=nombre, horizonte=hn); out.append(m)
        except Exception as e:
            out.append({'cp': cp, 'modelo': nombre, 'error': str(e)[:80]})
    print(f'  {cp} listo ({(time.time()-t0)/60:.1f} min)', flush=True)
    return out

_res = Parallel(n_jobs=-1, backend='threading')(delayed(_eval)(cp, s, e) for cp, (s, e) in datos.items())
multi = pd.DataFrame([f for sub in _res for f in sub])
multi.to_csv('/home/app/notebooks/resultados_multihorizonte.csv', index=False)
db['resultados_multihorizonte'].drop(); db['resultados_multihorizonte'].insert_many(multi.to_dict('records'))
print(f'LISTO en {(time.time()-t0)/60:.1f} min | {len(multi)} filas', flush=True)

if 'r2' in multi.columns:
    m = multi[multi['cp'] != '08037'].dropna(subset=['r2'])
    tabla = m.groupby(['modelo', 'horizonte'])['r2'].median().unstack()[['24h', '48h', '72h']].round(3)
    print('=== R2 MEDIANA por horizonte (sin 08037) ===', flush=True)
    print(tabla.to_string(), flush=True)
else:
    print('TODO ERROR. Ejemplos:', multi.get('error', pd.Series(dtype=str)).head().tolist(), flush=True)
