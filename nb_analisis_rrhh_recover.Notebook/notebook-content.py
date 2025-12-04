# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "18486496-0682-4358-97e0-361abe52b0d4",
# META       "default_lakehouse_name": "Bronze_Landing",
# META       "default_lakehouse_workspace_id": "08828f76-ed07-410e-b93a-b73fc55caadc",
# META       "known_lakehouses": [
# META         {
# META           "id": "18486496-0682-4358-97e0-361abe52b0d4"
# META         },
# META         {
# META           "id": "28d79845-228c-43cf-8292-8c7f6a545f3d"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Trabajamos las tablas sin tiempo complejo (materializar parquets en Tablas)

# CELL ********************

#Leer los ficheros parquet que son el resultado de la importacion anterior

df_department = spark.read.parquet("Files/humanresources/department")
df_employeedh = spark.read.parquet("Files/humanresources/employeedepartmenthistory")
df_empoyeeph = spark.read.parquet("Files/humanresources/employeepayhistory")
df_jobcandidate = spark.read.parquet("Files/humanresources/jobcandidate")
# df_shift 
display(df_department)
display(df_employeedh)
display(df_empoyeeph)
display(df_jobcandidate)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Guardar los dataframes como tablas
df_department.write.format("delta").mode("overwrite").saveAsTable("Bronze_Landing.hr_department")
df_employeedh.write.format("delta").mode("overwrite").saveAsTable("Bronze_Landing.hr_employeedepartmenthistory")
df_empoyeeph.write.format("delta").mode("overwrite").saveAsTable("Bronze_Landing.hr_employeepayhistory")
df_jobcandidate.write.format("delta").mode("overwrite").saveAsTable("Bronze_Landing.hr_jobcandidate")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Trabajamos la tabla mas compleja

# CELL ********************

import pandas as pd
from pyspark.sql import functions as F

#1) lee y limpia
df_shift_pandas = pd.read_parquet("/lakehouse/default/Files/humanresources/shift")
df_shift_pandas = df_shift_pandas.drop(columns=["modifieddate"])

#2) Normaliza tipos : time -> string
for c in ["starttime", "endtime"]:
    df_shift_pandas[c] = df_shift_pandas[c].astype(str)   # "HH:MM:SS"

#3) de pandas a Spark
df_shift = spark.createDataFrame(df_shift_pandas)

#4) Casts finales
df_shift = (df_shift
    .withColumn("shiftid", F.col("shiftid").cast("int"))
    .withColumn("name",    F.col("name").cast("string"))
    .withColumn("starttime", F.col("starttime").cast("string"))
    .withColumn("endtime",   F.col("endtime").cast("string"))
)

#5) Guarda la tabla Delta en Bronze
df_shift.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("Bronze_Landing.hr_shift")

# Verificación
spark.sql("SELECT * FROM bronze_landing.hr_shift").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
