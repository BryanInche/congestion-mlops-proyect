#Librerias que se usaran
import pandas as pd  # Libreria para administrar tablas, y realizar trabajos con distintas formas de tablas o dataframes
import numpy as np   # Libreria para poder hacer operaciones matematicas y matriciales
import matplotlib.pyplot as plt # Libreria para realizar graficos 
from tabulate import tabulate   # Permite formatear y mostrar de mejor manera los datos tabulares
import seaborn as sns  # Libreria para realizar graficos y vizualizaciones
import psycopg2        # Libreria que permite la conexion con PostgresSQL
from matplotlib.backends.backend_pdf import PdfPages # Libreria que permite exportar graficos en pdf
from obtenciondatos_postgresql_equipo import consultar_postgres_y_obtener_df_c4m 
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient #Librerias para obtener los accesos del Contanier de Azure Data Lake
from io import BytesIO  # Permite trabajar con datos binarios en memoria como si fueran archivos

#SECCION 1: EXTRACCION DE DATOS POR EQUIPO

# 1. Extraccion de datos Fuente C4M (Datos por equipo)
# Llamar a la función para ejecutar la consulta y obtener el DataFrame 

####### !IMPORTANTE!: INGRESAR ID_EQUIPO A EXTRAER  ##########################################################
datos_equipo = consultar_postgres_y_obtener_df_c4m(61)
# datos_equipo_199 = consultar_postgres_y_obtener_df_c4m(199)
# datos_equipo_201 = consultar_postgres_y_obtener_df_c4m(201)

# 1.Guardamos el df convertido en csv en la variable csv_data
csv_data = datos_equipo.to_csv(index=False)
# csv_data_199 = datos_equipo_199.to_csv(index=False)
# csv_data_201 = datos_equipo_201.to_csv(index=False)

# 1.2 Obtener conection Azure DataLake,interfaz de Azure(Claves de acceso: Key1) (Debes comentar esta variable Si NO deja hacer COMMIT DEL 
# CODIGO EN GIT,GITHUB)
# connection_string = 'DefaultEndpointsProtocol=https;AccountName=datalakemlopsd4m;AccountKey=iWT8t74/#XlqcqoR03keDVtFZPzr0PB9zDffMPaLWMUBIAjUww8uYAVkc9xRkcBtvTmUHKBvd1sB3+ASt6mGgcQ==;EndpointSuffix=core.windows.net'

# 1.3 Conectar al Blob Storage de Azure
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# 1.4 Identificamos el nombre del contenedor(container_name) y nombre del archivo(definir blob_name) en el Blob Storage
container_name = "raw/proyectocongestion_raw/fuentedatos_c4m/operacion_shougang/datos_equipos_individual/"

####### !IMPORTANTE!: CAMBIAR EL NOMBRE DEL CSV (PARA EL EQUIPO EN PARTICULAR)  ##########################################################
blob_name = "datos_raw_shougang_equipo_61.csv"
# blob_name_199 = "datos_raw_shougang_equipo_199.csv"
# blob_name_201 = "datos_raw_shougang_equipo_201.csv"

# 1.5 Guardamos el archivo CSV al Blob Storage
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
# Si existe un archivo con el el mismo nombre, dara error (en ese caso deberias eliminar primero el archivo en el Azure Storage)

# Verificar si existe el archivo
if blob_client.exists():
    # Eliminar el archivo existente
    blob_client.delete_blob()

# Cargar el nuevo archivo CSV al Blob Storage
blob_client.upload_blob(csv_data)