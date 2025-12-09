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
# META         },
# META         {
# META           "id": "7199137e-ea3a-4d45-87ba-9fafcc3f4154"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## eliminamos algunos datos para que funcione el ejemplo

# CELL ********************

#para preparar vamos a eliminar algunos datos de octubre y noviembre de la tabla destino
df_wm = spark.sql(
"""
DELETE FROM Silver_Refined.mciencia_incremental
WHERE year(time) in (2023,2022) and month(time) IN (09,10,11,12); 
"""
)
display(df_wm)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# ## Aca parte la carga incremental

# CELL ********************

#Notebook para Carga Incremental

#a buscar datos de origen
df_origen = spark.sql("select * from Bronze_Landing.minciencia")
## display(df_origen)

#marca de agua o maximo existente
df_wm = spark.sql("select max(time) as wm from Silver_Refined.mciencia_incremental")
display(df_wm)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# transformar el dataframe en coleccion
from datetime import timedelta

wm_value = df_wm.agg({"wm": "max"}).collect()[0][0]
wm_value_end = wm_value + timedelta(days=60)

print("Marca de agua actual:", wm_value)
print("extraer hasta :",wm_value_end)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filtrar incremental desde el origen
df_incremental = df_origen.filter(
    (df_origen["time"] > wm_value) &
    (df_origen["time"]< wm_value_end)
)

#quitamos columnas innecesarias
df_incremental = df_incremental.drop("latitud","longitud","nombreEstacion")
display(df_incremental)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#escribimos en la tabla de destino

df_incremental.write.mode("append").saveAsTable("Silver_Refined.mciencia_incremental")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

verificacion = spark.sql(
"""
SELECT MIN(time) as inicio , 
Max(time) as fin
from Silver_Refined.mciencia_incremental
""")
display(verificacion)
# 2020-02-01 00:00:00,2022-10-30 22:00:00
# 2020-02-01 00:00:00,2022-12-29 21:00:00
# 2020-02-01 00:00:00,2023-01-01 00:00:00

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
