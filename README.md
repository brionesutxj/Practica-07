
# Proceso ETL con Diferentes Orígenes de Datos

Este proyecto muestra el uso práctico del proceso ETL (Extracción, Transformación y Carga de datos) utilizando Python, Jupyter Notebook y diversas bibliotecas para conectarse a diferentes fuentes de datos: una base de datos relacional (MySQL) y una API pública (Open-Meteo).

---

## Objetivo

Aplicar los conceptos del proceso ETL, así como los comandos básicos usados en el análisis de datos con Python. Se trabajan dos fuentes de datos: una base de datos relacional y una API pública. Este proyecto demuestra:

- Extracción de datos desde diferentes fuentes
- Limpieza de datos para eliminar duplicados y valores nulos
- Transformación para generar nuevos conocimientos
- Exportación a nuevas tablas o formatos de salida

---

## Herramientas Utilizadas

- **Lenguaje de programación:** Python
- **Entorno:** Jupyter Notebook
- **Librerías:** pandas, requests, SQLAlchemy, PyMySQL
- **Base de datos:** MySQL (Chinook)
- **API pública:** Open-Meteo

---

## Actividades Realizadas

### 1. Base de Datos Relacional (MySQL - Chinook)

- ✅ Conexión a la base de datos `chinook`
- ✅ Extracción de las tablas `Customer` e `Invoice`
- ✅ Limpieza de datos: eliminación de duplicados y verificación de nulos
- ✅ Transformación: cálculo de ventas totales por país
- ✅ Exportación: escritura del DataFrame resultante a la tabla `ventas_pais_temp`

### 2. API Pública (Open-Meteo)

- ✅ Conexión a la API Open-Meteo para obtener temperaturas horarias en Veracruz
- ✅ Limpieza de datos: eliminación de duplicados y nulos
- ✅ Transformación: categorización de temperaturas (`Frío`, `Templado`, `Caluroso`)
- ✅ Exportación: escritura del DataFrame resultante a la tabla `weather_temp`

---

## Fragmentos de Código Relevantes

### Extracción desde MySQL

```python
from sqlalchemy import create_engine
import pandas as pd

engine = create_engine("mysql+pymysql://root:1234@localhost:3307/chinook")

df_customers = pd.read_sql("SELECT CustomerId, FirstName, LastName, Country FROM Customer", engine)
df_invoices = pd.read_sql("SELECT InvoiceId, CustomerId, Total, BillingCountry, InvoiceDate FROM Invoice", engine)
```

### Limpieza y transformación

```python
df_customers = df_customers.drop_duplicates()
df_invoices = df_invoices.drop_duplicates()

df_country_sales = df_invoices.groupby("BillingCountry")["Total"].sum().reset_index()
df_country_sales.columns = ["País", "TotalFacturado"]
df_country_sales = df_country_sales.sort_values("TotalFacturado", ascending=False)
```

### Exportación

```python
df_country_sales.to_sql("ventas_pais_temp", con=engine, if_exists="replace", index=False)
```

### Extracción desde API pública

```python
import requests

url = "https://api.open-meteo.com/v1/forecast?latitude=19.5&longitude=-96.9&current_weather=true&hourly=temperature_2m"
resp = requests.get(url).json()

df_weather = pd.DataFrame({
    "time": resp["hourly"]["time"],
    "temp_C": resp["hourly"]["temperature_2m"]
})
```

### Clasificación de temperatura

```python
def clasificar_temp(temp):
    if temp < 15:
        return "Frío"
    elif temp < 25:
        return "Templado"
    else:
        return "Caluroso"

df_weather["clima_categoria"] = df_weather["temp_C"].apply(clasificar_temp)
```


