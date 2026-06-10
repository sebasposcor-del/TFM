import time, warnings
import polars as pl, pandas as pd, numpy as np
from pymongo import MongoClient
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import r2_score, mean_absolute_error
from joblib import Parallel, delayed
warnings.filterwarnings('ignore')

client = MongoClient('mongodb://mongo:27017/'); db = client['tfm_energy']
df = pl.DataFrame(list(db['dataset_features'].find({}, {'_id': 0})), infer_schema_length=None)
print('Shape:', df.shape, flush=True)

# Config OFICIAL (historico completo, validacion completa ene-sep)
INI = '2019-01-01'; FIN_TRAIN = '2024-12-31 18:00:00'; FIN_VAL = '2025-09-30 18:00:00'
S = 28; STEPS = 12
ORDERS = [(2, 0, 1), (3, 0, 1)]
SORDERS = [(1, 1, 1, S), (0, 1, 1, S)]
CPS_REPR = ['08038', '08005', '08032', '08002']

def get_serie(cp):
    s = (df.filter(pl.col('cod_postal') == cp).sort('datetime')
           .select(['datetime', 'mwh_total']).to_pandas().set_index('datetime')['mwh_total'])
    return s.asfreq('6h')

def fit_params(serie, order, so):
    return SARIMAX(serie, order=order, seasonal_order=so,
                   enforce_stationarity=False, enforce_invertibility=False).fit(disp=False).params

def r2_train_72h(serie_tr, params, order, so):
    # in-sample a 72h via predict(dynamic=True) en bloques
    res = SARIMAX(serie_tr, order=order, seasonal_order=so,
                  enforce_stationarity=False, enforce_invertibility=False).filter(params)
    ev = np.asarray(serie_tr, float); n = len(ev); yt, yp = [], []
    for st in range(S, n - STEPS, STEPS):
        yp.extend(np.asarray(res.predict(start=st, end=st + STEPS - 1, dynamic=True))[:STEPS])
        yt.extend(ev[st:st+STEPS])
    return r2_score(yt, yp)

def r2_val_72h(serie_full, params, order, so, n_val):
    # walk-forward 72h sobre validacion (filtra una vez, predict dynamic)
    res = SARIMAX(serie_full, order=order, seasonal_order=so,
                  enforce_stationarity=False, enforce_invertibility=False).filter(params)
    ev = np.asarray(serie_full, float); n_total = len(ev); n_train = n_total - n_val
    yt, yp = [], []; i = n_train
    while i < n_total:
        h = min(STEPS, n_total - i)
        yp.extend(np.asarray(res.predict(start=i, end=i + h - 1, dynamic=True))[:h]); yt.extend(ev[i:i+h])
        i += h
    return np.array(yt), np.array(yp)

datos = {cp: get_serie(cp) for cp in CPS_REPR}
t0 = time.time()

def _eval(cp, serie):
    s_tr = serie.loc[INI:FIN_TRAIN]
    s_full = serie.loc[INI:FIN_VAL]
    n_val = len(serie.loc[FIN_TRAIN:FIN_VAL].iloc[1:])
    out = []
    for order in ORDERS:
        for so in SORDERS:
            try:
                params = fit_params(s_tr, order, so)
                r2_tr = r2_train_72h(s_tr, params, order, so)
                yt, yp = r2_val_72h(s_full, params, order, so, n_val)
                r2_va = r2_score(yt, yp)
                rel = (r2_tr - r2_va) / abs(r2_tr) if r2_tr != 0 else np.nan
                out.append({'cp': cp, 'order': str(order), 'seasonal': str(so),
                            'r2_train': round(r2_tr, 4), 'r2_val': round(r2_va, 4),
                            'rel_diff': round(rel, 4), 'mae_val': round(mean_absolute_error(yt, yp), 1)})
                print(f"{cp} {order}x{so}: R2_tr={r2_tr:.3f} R2_val={r2_va:.3f} rel={rel:.3f} ({(time.time()-t0)/60:.1f}m)", flush=True)
            except Exception as e:
                out.append({'cp': cp, 'order': str(order), 'seasonal': str(so), 'error': str(e)[:60]})
                print(f"{cp} {order}x{so}: ERROR {str(e)[:50]}", flush=True)
    return out

_res = Parallel(n_jobs=-1, backend='threading')(delayed(_eval)(cp, s) for cp, s in datos.items())
res_grid = pd.DataFrame([f for sub in _res for f in sub])
res_grid.to_csv('/home/app/notebooks/grid_sarima.csv', index=False)
print(f'GRID listo en {(time.time()-t0)/60:.1f} min | {len(res_grid)} filas', flush=True)

ok = res_grid.dropna(subset=['r2_val'])
agg = (ok.groupby(['order', 'seasonal'])
         .agg(r2_val=('r2_val', 'mean'), rel_diff=('rel_diff', 'mean'), mae_val=('mae_val', 'mean'))
         .reset_index().sort_values('r2_val', ascending=False))
print('=== AGREGADO (media 4 CPs) ===', flush=True)
print(agg.to_string(index=False), flush=True)
best = agg.iloc[0]
print(f"GANADOR -> order={best['order']} seasonal={best['seasonal']} (R2_val={best['r2_val']:.3f}, rel={best['rel_diff']:.3f})", flush=True)
