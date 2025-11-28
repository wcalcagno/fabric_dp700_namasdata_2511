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

#Notebook de Ingesta

#Invocar librerias
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
import os 
import json 
import pyspark.sql.functions as F
import time 

#configuracion
ruta_origen = "Files/ficheros_json"
nombre_tabla = "temperatura_simulada"
ruta_check = "Files/ficheros_checkpoint"

#Esquema del fichero de origen (data JSON de origen)
file_schema = StructType() \
    .add("id", StringType()) \
    .add("temp", DoubleType()) \
    .add("timestamp", TimestampType())

spark.sql(f"CREATE TABLE IF NOT EXISTS {nombre_tabla}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Leer el fichero de origen con Spark
fichero_raiz_df = spark.readStream\
    .schema(file_schema)\
    .option("maxFilesPerTrigger",1)\
    .json(ruta_origen)

#agregar el timestamp de procesamiento
fichero_mod_df = fichero_raiz_df.withColumn("ts_proces",F.current_timestamp())

#escribimos la data en la tabla delta
deltastream = fichero_mod_df\
    .writeStream\
    .format("delta")\
    .outputMode("append")\
    .option("mergeSchema",True)\
    .option("checkpointLocation",ruta_check)\
    .start(f"Tables/{nombre_tabla}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fichero_raiz_df.isStreaming

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltastream.isActive

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltastream.status

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltastream.lastProgress

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

while deltastream.isActive:
    print("✅ Stream is running...")
    print("📊 Last progress:", deltastream.lastProgress)
    time.sleep(5)

print("❌ Stream has stopped.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltastream.stop()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfst = spark.sql("SELECT count(*) FROM Bronze_Landing.temperatura_simulada")
display(dfst)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
