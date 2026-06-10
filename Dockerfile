# TFM - Energy Prediction & Early Warning BCN

FROM python:3.11-slim

# libgomp1: runtime OpenMP requerido por LightGBM/XGBoost (la imagen slim no lo trae)
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /home/app