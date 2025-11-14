# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "28d79845-228c-43cf-8292-8c7f6a545f3d",
# META       "default_lakehouse_name": "Silver_Refined",
# META       "default_lakehouse_workspace_id": "08828f76-ed07-410e-b93a-b73fc55caadc",
# META       "known_lakehouses": [
# META         {
# META           "id": "28d79845-228c-43cf-8292-8c7f6a545f3d"
# META         },
# META         {
# META           "id": "520a8cb9-a177-499d-b186-a7012ab0a47b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Preparar data para ML analitica predictiva

df = spark.read.table("Silver_Refined.ft_costos_rrhh_dia_turno_depto_business_entity")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT 
# MAGIC   A.businessentityid as idtrabajador
# MAGIC , A.shiftid        as idturno
# MAGIC , Cast(A.ratechangedate as date) as Fechahora
# MAGIC , A.rate            as tasa 
# MAGIC , A.payfrequency   as freqpago
# MAGIC , A.costo_dia     
# MAGIC , B.departmentname as departamento
# MAGIC , B.groupname as grupo
# MAGIC FROM Silver_Refined.ft_costos_rrhh_dia_turno_depto_business_entity A 
# MAGIC INNER join dim_departamento B
# MAGIC ON B.departmentid = A.departmentid 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Integrar SPARK con SQL para crear un DataFrame
df_entrenamiento = spark.sql(
"""
SELECT 
  A.businessentityid as idtrabajador
, A.shiftid        as idturno
, Cast(A.ratechangedate as date) as Fechahora
, A.rate            as tasa 
, A.payfrequency   as freqpago
, A.costo_dia     
, B.departmentname as departamento
, B.groupname as grupo
FROM Silver_Refined.ft_costos_rrhh_dia_turno_depto_business_entity A 
INNER join dim_departamento B
ON B.departmentid = A.departmentid 
"""
)
display(df_entrenamiento)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Guardar DF entrenamiento en capa gold

df_entrenamiento.write.format("delta").mode("overwrite").option("overwriteschema","true").saveAsTable("Gold_Trusted.train_tasatrabajo")
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM Gold_Trusted.train_tasatrabajo


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
