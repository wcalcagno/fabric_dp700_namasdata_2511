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

# ultimo dato en mi destino

# CELL ********************

df = spark.sql("select max(time) as fecha from Silver_Refined.mciencia_incremental_pipeline")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.mode("overwrite").option("mergeSchema","true").saveAsTable("Bronze_Landing.marca_de_agua")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
