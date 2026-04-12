import requests

# Cuántos registros tiene X2 en 2024 en la API
url_2024 = (
    "https://analisi.transparenciacatalunya.cat/resource/"
    "nzvn-apee.json"
    "?$where=codi_estacio='X2'"
    " AND codi_variable in('32')"
    " AND data_lectura between '2024-01-01T00:00:00' and '2025-01-01T00:00:00'"
    "&$limit=1&$select=count(*)"
)

# Cuántos registros tiene X2 en 2025 en la API
url_2025 = (
    "https://analisi.transparenciacatalunya.cat/resource/"
    "nzvn-apee.json"
    "?$where=codi_estacio='X2'"
    " AND codi_variable in('32')"
    " AND data_lectura between '2025-01-01T00:00:00' and '2026-01-01T00:00:00'"
    "&$limit=1&$select=count(*)"
)

r2024 = requests.get(url_2024, timeout=60).json()
r2025 = requests.get(url_2025, timeout=60).json()

print(f"X2 2024 en API: {r2024}")
print(f"X2 2025 en API: {r2025}")