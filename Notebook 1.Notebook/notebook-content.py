# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ee3a5cef-859c-4f37-965e-cba92833f8b7",
# META       "default_lakehouse_name": "Bronze_Landing",
# META       "default_lakehouse_workspace_id": "0937df67-f304-4d29-bfd4-c5a2bc260840",
# META       "known_lakehouses": [
# META         {
# META           "id": "ee3a5cef-859c-4f37-965e-cba92833f8b7"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Un poquito de SPARK en Notebooks

DataframeSpark = spark.read.table("Bronze_Landing.minciencia")

DataframeSpark = DataframeSpark.select(
    "time",
    "ff_Valor",
    "CodigoNacional"
)
display(DataframeSpark)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

# Ruta del Lakehouse
path = "abfss://0937df67-f304-4d29-bfd4-c5a2bc260840@onelake.dfs.fabric.microsoft.com/ee3a5cef-859c-4f37-965e-cba92833f8b7/Tables/minciencia"

# Leer parquet
df_pandas = pd.read_parquet(path)

# Seleccionar solo las columnas que quieres
df_pandas = df_pandas[["time", "ff_Valor", "CodigoNacional"]]

df_pandas.head()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# Si no está instalado en el entorno:
# %pip install statsmodels

from pyspark.sql import functions as F
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA

# 1) Leer tabla base desde el Lakehouse
df = spark.read.table("Bronze_Landing.minciencia")

# 2) Dejar solo columnas necesarias y tipar fecha/hora
df = (
    df.select("time", "ff_Valor", "CodigoNacional")
      .withColumn("time", F.to_timestamp("time"))
)

# 3) Promedio diario por estación
df_diario = (
    df
    .withColumn("fecha", F.to_date("time"))
    .groupBy("CodigoNacional", "fecha")
    .agg(F.avg("ff_Valor").alias("ff_Valor_dia"))
)

df_diario = df_diario.orderBy("CodigoNacional", "fecha")
display(df_diario)

# 4) Obtener listado de estaciones (CodigosNacionales)
codigos = (
    df_diario
    .select("CodigoNacional")
    .distinct()
    .toPandas()["CodigoNacional"]
    .tolist()
)

# 5) Para cada estación, entrenar ARIMA y proyectar 2 años diarios
resultados = []
pasos_forecast = 365 * 2  # 2 años aprox

for codigo in codigos:
    # Filtrar una estación
    df_est = (
        df_diario
        .filter(F.col("CodigoNacional") == codigo)
        .orderBy("fecha")
    )

    pdf = df_est.toPandas()

    # Si hay pocos datos, saltamos la estación (evita errores de ARIMA)
    if len(pdf) < 30:
        print(f"Saltando CodigoNacional {codigo}: muy pocos datos ({len(pdf)} filas)")
        continue

    pdf = pdf.sort_values("fecha").set_index("fecha")

    serie = pdf["ff_Valor_dia"]
    serie.index = pd.DatetimeIndex(serie.index)

    # Asegurar frecuencia diaria con resample, rellenando huecos
    serie = (
        serie
        .resample("D")
        .mean()
        .ffill()
    )

    # ARIMA simple (ajusta (p,d,q) si después quieres afinar)
    try:
        modelo = ARIMA(serie, order=(1, 1, 1))
        modelo_ajustado = modelo.fit()
    except Exception as e:
        print(f"Error entrenando ARIMA para CodigoNacional {codigo}: {e}")
        continue

    # Forecast 2 años diarios
    forecast_vals = modelo_ajustado.forecast(steps=pasos_forecast)

    # Fechas futuras
    fecha_inicio = serie.index[-1] + pd.Timedelta(days=1)
    fechas_futuras = pd.date_range(
        start=fecha_inicio,
        periods=pasos_forecast,
        freq="D"
    )

    forecast_df = pd.DataFrame({
        "fecha": fechas_futuras,
        "ff_Valor_predicho": forecast_vals.values,
        "CodigoNacional": codigo
    })

    resultados.append(forecast_df)

# 6) Unir todos los resultados de todas las estaciones
if resultados:
    forecast_all_pd = pd.concat(resultados, ignore_index=True)
    display(forecast_all_pd)

    # 7) Pasar a Spark y guardar si quieres
    forecast_all_spark = spark.createDataFrame(forecast_all_pd)

    display(forecast_all_spark)

    # Ejemplo de guardado en Lakehouse (ajusta nombre de DB/tabla)
    # forecast_all_spark.write.mode("overwrite") \
    #     .saveAsTable("Gold_ModeloARIMA.ff_valor_forecast_diario_todas_estaciones")
else:
    print("No se generaron pronósticos (revisa datos de entrada).")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
