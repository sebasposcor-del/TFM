# TFM - Energy Prediction & Early Warning BCN

# Base image: PySpark + Jupyter + Python 3.11
FROM jupyter/pyspark-notebook:spark-3.5.0

# Install project dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Set working directory
WORKDIR /home/jovyan/work