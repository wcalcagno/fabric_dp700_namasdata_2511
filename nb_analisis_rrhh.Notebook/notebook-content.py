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

# ## Trabajamos las tablas sin tiempo complejo

# CELL ********************

# leemos los ficheros spark que estan en la carpeta humanresuorces y los materializamos en dataframes
df_department = spark.read.parquet("Files/humanresources/department")
df_employeedpthis = spark.read.parquet("Files/humanresources/employeedepartmenthistory")
df_employeepayhistory = spark.read.parquet("Files/humanresources/employeepayhistory")
df_jobcandidate = spark.read.parquet("Files/humanresources/jobcandidate")
# df_shift = spark.read.parquet("Files/humanresources/shift")

#/lakehouse/default/Files/humanresources/shift

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#guardar los dataframes como tablas en bronze_landing
df_department.write.format("delta").mode("overwrite").saveAsTable("Bronze_landing.hr_department")
df_employeedpthis.write.format("delta").mode("overwrite").saveAsTable("Bronze_landing.hr_employeedepartmenthistory")
df_employeepayhistory.write.format("delta").mode("overwrite").saveAsTable("Bronze_landing.hr_employeepayhistory")
df_jobcandidate.write.format("delta").mode("overwrite").saveAsTable("Bronze_landing.hr_jobcandidate")

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

# 1) Lee y limpia
df_shift_pandas = pd.read_parquet("/lakehouse/default/Files/humanresources/shift")
df_shift_pandas = df_shift_pandas.drop(columns=["modifieddate"])

# 2) Normaliza tipos: time -> string
for c in ["starttime", "endtime"]:
    df_shift_pandas[c] = df_shift_pandas[c].astype(str)   # "HH:MM:SS"

# 3) A Spark
df_shift = spark.createDataFrame(df_shift_pandas)

# 4) Casts finales
df_shift = (df_shift
    .withColumn("shiftid", F.col("shiftid").cast("int"))
    .withColumn("name",    F.col("name").cast("string"))
    .withColumn("starttime", F.col("starttime").cast("string"))
    .withColumn("endtime",   F.col("endtime").cast("string"))
)

# 5) Guarda como tabla Delta en Bronze
df_shift.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_landing.hr_shift")

# Verificación
spark.sql("SELECT * FROM bronze_landing.hr_shift").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Creacion de tablas de hecho y Dimensiones para guardarlas en Silver

# CELL ********************

# Crear tabla de Hechos
df_ft_costos_rrhh = spark.sql(
"""
SELECT
    edh.businessentityid,
    edh.departmentid,
    edh.shiftid,
    eph.ratechangedate,
    eph.rate,
    eph.payfrequency,
    CASE
        WHEN eph.payfrequency = 1 THEN eph.rate / 30       -- Mensual
        WHEN eph.payfrequency = 2 THEN eph.rate / 15       -- Quincenal
        ELSE eph.rate
    END AS costo_dia
FROM Bronze_Landing.hr_employeedepartmenthistory edh
LEFT JOIN Bronze_Landing.hr_employeepayhistory eph
    ON edh.businessentityid = eph.businessentityid
ORDER BY
    edh.businessentityid,
    eph.ratechangedate;

"""
)
display(df_ft_costos_rrhh)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Guardar la tabla de hechos
df_ft_costos_rrhh.write.format("delta").mode("overwrite").saveAsTable("Silver_Refined.ft_costos_rrhh_dia_turno_depto_business_entity")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#/* Dimension Trabajadores */
df_dim_trabajador = spark.sql(
"""
SELECT
    businessentityid,
    MIN(startdate)  AS fechainicio,
    MAX(enddate)  AS fechatermino
FROM Bronze_Landing.hr_employeedepartmenthistory
GROUP BY businessentityid
"""
)
display(df_dim_trabajador)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Guardar la tabla de dimensiones
df_dim_trabajador.write.mode("overwrite").saveAsTable("Silver_Refined.dim_trabajador")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#creamos la tabla de dimension departamento
df_dim_departamento = spark.sql(
"""
SELECT
    departmentid,
    name AS departmentname,
    groupname
FROM Bronze_Landing.hr_department;
"""
)
display(df_dim_departamento)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Guardar la tabla de dimensiones
df_dim_departamento.write.mode("overwrite").saveAsTable("Silver_Refined.dim_departamento")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Creamos Dimension Turno
df_dim_turno =  spark.sql(
"""
SELECT
    shiftid,
    name AS shiftname,
    starttime,
    endtime
FROM Bronze_Landing.hr_shift;
"""
)
display(df_dim_turno)

df_dim_turno.write.mode("overwrite").saveAsTable("Silver_Refined.dim_turno")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_revision = spark.read.table('Bronze_Landing.hr_shift')
display(df_revision)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
