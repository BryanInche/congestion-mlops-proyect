#Librerias que se usaran
import pandas as pd  # Libreria para administrar tablas, y realizar trabajos con distintas formas de tablas o dataframes
import numpy as np   # Libreria para poder hacer operaciones matematicas y matriciales
import matplotlib.pyplot as plt # Libreria para realizar graficos 
from tabulate import tabulate   # Permite formatear y mostrar de mejor manera los datos tabulares
import seaborn as sns  # Libreria para realizar graficos y vizualizaciones
import psycopg2        # Libreria que permite la conexion con PostgresSQL
from matplotlib.backends.backend_pdf import PdfPages # Libreria que permite exportar graficos en pdf
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient #Librerias para obtener los accesos del Contanier de Azure Data Lake
from io import BytesIO  # Permite trabajar con datos binarios en memoria como si fueran archivos


# 1.2 Obtener conection Azure DataLake,interfaz de Azure(Claves de acceso: Key1) (Debes comentar esta variable Si NO deja hacer COMMIT DEL 
# CODIGO EN GIT,GITHUB)
# connection_string = 'DefaultEndpointsProtocol=https;AccountName=datalakemlopsd4m;AccountKey=iWT8t74/#XlqcqoR03keDVtFZPzr0PB9zDffMPaLWMUBIAjUww8uYAVkc9xRkcBtvTmUHKBvd1sB3+ASt6mGgcQ==;EndpointSuffix=core.windows.net'

# 1.3 Conectar al Blob Storage de Azure
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# SECCION 2: JOIN o UNIR TODOS LOS CSV DEL BLOBSTORAGE EN UNO SOLO

# 2.1 Se crea una lista para almacenar los DataFrames de cada archivo CSV
dfs = []

container_name_join = "raw"
folder_path = "proyectocongestion_raw/fuentedatos_c4m/operacion_shougang/datos_equipos_individual/"

# Obtener la lista de blobs en la carpeta específica del contenedor
#blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name_join)
blobs = container_client.list_blobs(name_starts_with=folder_path)

# Filtrar los nombres solo de los archivos CSV
csv_files = [blob.name for blob in blobs if blob.name.endswith('.csv')]

# 6. Leemos cada archivo CSV y guardarlos en DataFrames individuales
for csv_file in csv_files:
    # Obtener el blob del Blob Storage
    blob_client = blob_service_client.get_blob_client(container=container_name_join, blob=csv_file)
    
    # Descargar el contenido del blob
    blob_data = blob_client.download_blob()
    
    # Leer el contenido del blob en un objeto BytesIO
    bytes_io = BytesIO()
    blob_data.download_to_stream(bytes_io)
    bytes_io.seek(0)  # Asegurar que la posición del cursor esté al inicio del archivo
    
    # Leer el archivo CSV en un DataFrame de Pandas desde el objeto BytesIO
    df = pd.read_csv(bytes_io)
    
    # Agregar el DataFrame a la lista
    dfs.append(df)

# 7. Se Concatena todos los DataFrames en uno solo df
result_df = pd.concat(dfs, ignore_index=True)

# Convertimos el df_consolidado en csv en la variable datos_total para enviarlo al Blob Storage Azure
datos_total = result_df.to_csv(index=False)

# 4. Identificamos el nombre del contenedor(container_name) y nombre del archivo(definir blob_name) en el Blob Storage
container_name_consolidado = "raw/proyectocongestion_raw/fuentedatos_c4m/operacion_shougang/datos_equipos_consolidado/"
blob_name_consolidado = "datos_raw_shougang_c4m.csv"

# 5. Guardar el archivo CSV al Blob Storage
blob_client = blob_service_client.get_blob_client(container=container_name_consolidado, blob=blob_name_consolidado)

# Verificar si existe el archivo 
if blob_client.exists():
    # Eliminar el archivo existente(para luego guardar la actualizacion)
    blob_client.delete_blob()

blob_client.upload_blob(datos_total)