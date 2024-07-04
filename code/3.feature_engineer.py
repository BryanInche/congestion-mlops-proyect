#Librerias que se usaran
import pandas as pd  # Libreria para administrar tablas, y realizar trabajos con distintas formas de tablas o dataframes
import numpy as np   # Libreria para poder hacer operaciones matematicas y matriciales 
import pandas as pd
from pyspark.sql.functions import col

#1. Leemos los datos de PROCEESED la tabla Delta 
df_delta = spark.read.format("delta").load("/mnt/datalakemlopsd4m/processed/proyectocongestion_processed/datapreprocessed_congestion_tabladelta")
datos = df_delta.toPandas()


# 2. Limpieza de variables No prioritarias
columnas_a_eliminar = ['LT_F_BRK_TEMP', 'RT_R_WHEEL_SPD', 'LT_R_WHEEL_SPD','Tire_Press_N5','start_time_alert', 'end_time_alert', 'Event_Date']

# 2.1 Filtrar las columnas que existen en el DataFrame
columnas_existentes = [col for col in columnas_a_eliminar if col in datos.columns]

# 2.2 Verificar si hay columnas para eliminar
if columnas_existentes:
    datos.drop(columnas_existentes, axis=1, inplace=True)

# 3. Seleecion de variables en especifico luego de seleccion de caracteristicas (Correlacion, variables significaticas)
#datos = datos.loc[:, ['x', 'y']]



# n. Pasos Adicionales
# Ajustar la configuración para mostrar todas las columnas
#spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
# n.1 Listar todas las bases de datos
#spark.sql("SHOW DATABASES").show(truncate=False)

# # n.2 Crear la base de datos si no existe en el almacenamiento de processed (en la ruta donde se almacenaran TablaDelta preprocesados)
#spark.sql("CREATE DATABASE IF NOT EXISTS dbproyectocongestion_presentation LOCATION '/mnt/datalakemlopsd4m/presentation/proyectocongestion_presentation/'")

# 3. Convertir el DataFrame de Pandas a un DataFrame de Spark
spark_datos = spark.createDataFrame(datos)

# 4. Guardar los datos preprocesados en una tabla Delta en el Azure Storage 
# Asegurar que las columnas de fecha y hora mantengan sus tipos de datos
#spark_datos = spark_datos.withColumn("Event_Date", col("Event_Date").cast("timestamp"))
#spark_datos = spark_datos.withColumn("instant_date_t", col("instant_date_t").cast("timestamp"))

# Nombre de la tabla Delta a guardar
nombre_tabla_delta = "dbproyectocongestion_presentation.tablacaracteristicas_congestion_tabladelta"

# 4.1 Verificar si ya existe la tabla Delta
if spark.catalog.tableExists(nombre_tabla_delta):
    # Eliminar la tabla Delta existente
    spark.sql("DROP TABLE IF EXISTS " + nombre_tabla_delta)

# 4.2 Guardar los datos preprocesados en una tabla Delta
spark_datos.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(nombre_tabla_delta)

