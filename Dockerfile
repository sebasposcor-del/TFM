# TFM - Energy Prediction & Early Warning BCN

# Base image: Python 3.11 ligera
FROM python:3.11-slim

# Install project dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Set working directory
WORKDIR /home/app