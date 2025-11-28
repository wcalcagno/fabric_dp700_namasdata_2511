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

#Crear ficheros JSON con data aleatoria

import json
import os 
import uuid
import random
import time 
from datetime import datetime

#Configuracion ficheros
carpeta_salida = "/lakehouse/default/Files/ficheros_json"
num_ficheros = 10
seg_espera = 3

#verificacion de existencia de carpetas
os.makedirs(carpeta_salida, exist_ok=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for i in range(num_ficheros):
    #variables generales
    now = datetime.utcnow()
    timestamp_str = now.strftime("%Y%m%dT%H%M%S")

    #simulacion de mensaje del sensor
    registro = {
        "id":str(uuid.uuid4()),
        "temp":round(random.uniform(21.0, 35.0),2),
        "timestamp": now.isoformat()
    }

    #construir el fichero json
    filename = f"temp_{timestamp_str}.json"
    filepath = os.path.join(carpeta_salida, filename)

    #materializar el fichero JSON
    with open(filepath, "w") as f:
        json.dump(registro, f)

    print(f"OK-[{i+1}/{num_ficheros}] Escribio: {filename}")
    time.sleep(seg_espera)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
