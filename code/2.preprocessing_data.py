#Librerias que se usaran
import pandas as pd  # Libreria para administrar tablas, y realizar trabajos con distintas formas de tablas o dataframes
import numpy as np   # Libreria para poder hacer operaciones matematicas y matriciales
import matplotlib.pyplot as plt # Libreria para realizar graficos 
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import io
from pyspark.sql.functions import col  # para trabajar con tablas delta y spark

# 1. Obtener conection Azure DataLake,interfaz de Azure(Claves de acceso: Key1) (Debes comentar esta variable Si NO deja hacer COMMIT DEL 
# CODIGO EN GIT,GITHUB)
# connection_string = 'DefaultEndpointsProtocol=https;AccountName=datalakemlopsd4m;AccountKey=iWT8t74/#XlqcqoR03keDVtFZPzr0PB9zDffMPaLWMUBIAjUww8uYAVkc9xRkcBtvTmUHKBvd1sB3+ASt6mGgcQ==;EndpointSuffix=core.windows.net'

# 2. Conectar al Blob Storage de Azure
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# 2.1 Identificamos el nombre del contenedor(container_name) y nombre del archivo(definir blob_name) en el Blob Storage
container_name = "raw/proyectocongestion_raw/fuentedatos_consolidado/"

#2.2 nombre del csv
#blob_name = "datos_raw_shougang_c4m_h4m.csv"
#2.2 nombre del parquet
blob_name = "datos_raw_shougang_c4m_h4m.parquet"

# 2.3 Obtener el blob_client
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

# 2.4 Leer el contenido del blob como texto
#blob_data = blob_client.download_blob().content_as_text()
# 2.4 Leer el contenido del blob como un objeto de bytes
blob_data = blob_client.download_blob().readall()

# 2.5 Especificar tipos de datos al leer el archivo CSV
column_types = {
    'instant_date_t': 'datetime64[ns]' ,
    'Event Date' : 'datetime64[ns]'
}

# 2.6 Leer el archivo CSV en un DataFrame de Pandas desde el texto
#datos = pd.read_csv(io.StringIO(blob_data), parse_dates=['instant_date_t', 'Event Date'])
# 2.6. Leer el archivo PARQUET en un DataFrame de Pandas desde el texto
datos = pd.read_parquet(io.BytesIO(blob_data), columns=column_types.keys())


# 3. Extraer componentes de fecha y hora
datos['year'] = datos['instant_date_t'].dt.year
datos['mes'] = datos['instant_date_t'].dt.month
datos['dia'] = datos['instant_date_t'].dt.day
datos['hora'] = datos['instant_date_t'].dt.hour
datos['minuto'] = datos['instant_date_t'].dt.minute


# 4. Limpieza de Valores Nulos
valores_nulos = datos.isnull().sum()
valores_nulos_ordenados = valores_nulos.sort_values(ascending=False)
porcentaje_nulos = (valores_nulos_ordenados / len(datos)) * 100
#Extraemos los indices(nombres) de las columnas o variables que tienen mas de 70% de nulos
columnas_a_eliminar = porcentaje_nulos[porcentaje_nulos > 70].index
#Eliminamos las variables con mayor porcentaje de valores nulos
datos = datos.drop(columnas_a_eliminar, axis=1)


# 5. Tratamiento de valores duplicados
#datos = datos.drop_duplicates()


# 6. Limpieza de variables No prioritarias
columnas_a_eliminar = ['columnatestingt', 'marchacod', 'marchacdo','marchadod','marchaddo','id_category_path',
                       'adelantardo','adelantarod','isunderground', 'tiem_creac','tiem_update','Event_Date', 'arreglolimites']

# 6.1 Filtrar las columnas que existen en el DataFrame
columnas_existentes = [col for col in columnas_a_eliminar if col in datos.columns]

# 6.2 Verificar si hay columnas para eliminar
if columnas_existentes:
    datos.drop(columnas_existentes, axis=1, inplace=True)


# 7. Manejo de variables Categoricas
# Define el diccionario de mapeo
map_dict = {True: 1, False: 0}

# 7.1 Aplica el mapeo a la columna "isload_t"
datos["isload_t"] = datos["isload_t"].map(map_dict)


# 8. Renombrar Variables con los nuevos nombres de las columnas
nuevos_nombres = {'Equipment' : 'nombre_equipo'}  # 
# Renombra las columnas del DataFrame
datos = datos.rename(columns=nuevos_nombres)

# Eliminar "null" de los nombres de las variables
# nuevos_nombres2 = {col: col.replace("_null", "") for col in datos.columns}
# datos = datos.rename(columns=nuevos_nombres2)

# Pasos opcionales: Nota (Estos procesos se deben hacer en un notebook por separado para no interunpir el flujo)
# n.1 Listar todas las bases de datos
# Ajustar la configuración para mostrar todas las columnas
#spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
# n.1 Listar todas las bases de datos
#spark.sql("SHOW DATABASES").show(truncate=False)

# # n.2 Crear la base de datos si no existe en el almacenamiento de processed (en la ruta donde se almacenaran TablaDelta preprocesados)
#spark.sql("CREATE DATABASE IF NOT EXISTS dbproyectocongestion_processed LOCATION '/mnt/datalakemlopsd4m/processed/proyectocongestion_processed/'")

# 9. Limpieza de los nombres de las columnas para eliminar caracteres especiales
datos.columns = [col.replace(" ", "_").replace("-", "_").replace("(", "").replace(")", "").replace("_null", "") for col in datos.columns]

# 10. Ajuste de los datos a Series Temporales
# 10.1 Se establece la columna 'Time' como el índice del DataFrame "datos"
# datos = datos.set_index('instant_date_t')

# # 10.2 Ordenamos el dataset de forma ascendente segun el datetime
# datos.sort_index(inplace=True)

# # 10.3 Identificamos la periocidad de la serie temporal
# df_time_diffs = datos.index.to_series().diff().dt.total_seconds()

# # 10.4 Eliminamos o Filtramos las filas donde la diferencia es distinta de cero (Con ello eliminamos las filas o registros de fechas duplicadas)
# datos = datos[df_time_diffs != 0]

# # 10.5. # Reinterpolar el dataset con una periosidad en especifico
# # 'S' o 'seg' : Segundo, 'T' o 'min': Minuto,'H' o 'Hr': Hora,'D': Día,'W': Semana,'M': Mes,'Q': Trimestre,'Y': Año
# datos = datos.asfreq(freq='T', method='bfill')

# 11. Convertir el DataFrame de Pandas a un DataFrame de Spark
spark_datos = spark.createDataFrame(datos)

# 12. Guardar los datos preprocesados en una tabla Delta en el Azure Storage 
# Asegurar que las columnas de fecha y hora mantengan sus tipos de datos
from pyspark.sql.functions import col
#spark_datos = spark_datos.withColumn("Event_Date", col("Event_Date").cast("timestamp"))
spark_datos = spark_datos.withColumn("instant_date_t", col("instant_date_t").cast("timestamp"))

# Nombre de la tabla Delta a guardar
nombre_tabla_delta = "dbproyectocongestion_processed.datapreprocessed_congestion_tabladelta"

# Verificar si ya existe la tabla Delta
if spark.catalog.tableExists(nombre_tabla_delta):
    # Eliminar la tabla Delta existente
    spark.sql("DROP TABLE IF EXISTS " + nombre_tabla_delta)

# 13. Guardar los datos preprocesados en una tabla Delta
spark_datos.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(nombre_tabla_delta)
