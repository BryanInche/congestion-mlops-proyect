#Librerias que se usaran
import pandas as pd  # Libreria para administrar tablas, y realizar trabajos con distintas formas de tablas o dataframes
import numpy as np   # Libreria para poder hacer operaciones matematicas y matriciales
import matplotlib.pyplot as plt # Libreria para realizar graficos 
from tabulate import tabulate   # Permite formatear y mostrar de mejor manera los datos tabulares
import seaborn as sns  # Libreria para realizar graficos y vizualizaciones
import psycopg2        # Libreria que permite la conexion con PostgresSQL
from matplotlib.backends.backend_pdf import PdfPages # Libreria que permite exportar graficos en pdf

#Libreria creada para extraer datos desde SQL
from obtenciondatos_postgresql import consultar_postgres_y_obtener_df

#Librerias para obtener los accesos del Contanier de Azure Data Lake
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# 1. Extraccion de datos Fuente C4M
# Llamar a la función para ejecutar la consulta y obtener el DataFrame (ingresar a la funcion que id_equipo y fechas se desea extraer)
# equipos acarreo disponibles en telemetria h4m(2dias) : 61,65,68,126,199,201,*204*,205
datos = consultar_postgres_y_obtener_df()

# 2.Guardar los datos extraidos desde PostgreSQL
# 2.1 Definir un nombre para el archivo csv que se guardara en el Blob Storage de Azure DataLake
datos.to_csv("datos_raw_shougang_equipo63.csv", index=False)

# 3. Se Obtiene las credenciales y accesos al contanier de Azure Data Lake conection,de Azure DataLake,interfaz de Azure (Claves de acceso: Key1) (Debes la #variable "connection_string" Si NO deja hacer COMMIT DEL CODIGO EN GIT,GITHUB)
# connection_string = 'DefaultEndpointsProtocol=https;AccountName=datalakemlopsd4m;AccountKey=iWT8t74/#XlqcqoR03keDVtFZPzr0PB9zDffMPaLWMUBIAjUww8uYAVkc9xRkcBtvTmUHKBvd1sB3+ASt6mGgcQ==;EndpointSuffix=core.windows.net'

# 3.1 Conectar al Blob Storage de Azure
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# 3.2 Nombre del contenedor y archivo en el Blob Storage
container_name = "raw/proyectocongestion_raw/fuentedatos_c4m/operacion_shougang/"
blob_name = "datos_raw_shougang_equipo63.csv"

# 3.3 Subir el archivo CSV al Blob Storage
with open("datos_raw_shougang_equipo63.csv", "rb") as data:
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(data)

OPCIONAL: Si deseamos consolidar o unir todos los csv(equipos) en uno solo
# 1.Lista para almacenar los DataFrames de cada archivo CSV
dfs = []

# 2.Nombres de los archivos CSV
csv_files = ["datos_raw_shougang_equipo61.csv","datos_raw_shougang_equipo65.csv","datos_raw_shougang_equipo68.csv","datos_raw_shougang_equipo126.csv",             
             "datos_raw_shougang_equipo199.csv","datos_raw_shougang_equipo199.csv",
             "datos_raw_shougang_equipo201.csv","datos_raw_shougang_equipo205.csv"]

# 3.Leer cada archivo CSV y guardarlos en DataFrames individuales
for csv_file in csv_files:
    # Descargar el archivo CSV del Blob Storage
    with open(csv_file, "wb") as my_blob:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=csv_file)
        blob_data = blob_client.download_blob()
        blob_data.readinto(my_blob)
    
    # Leer el archivo CSV en un DataFrame de Pandas
    df = pd.read_csv(csv_file)
    
    # Agregar el DataFrame a la lista
    dfs.append(df)

# 4.Concatenar todos los DataFrames en uno solo al final
result_df = pd.concat(dfs, ignore_index=True)

# 5.Guardar los datos extraidos desde PostgreSQL
# 5.1 Definir un nombre para el archivo csv que se guardara en el Blob Storage de Azure DataLake
result_df.to_csv("datos_raw_shougang_consolidado.csv", index=False)

# 5.2 Conectar al Blob Storage de Azure
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# 5.3 Nombre del contenedor y archivo en el Blob Storage
container_name = "raw/proyectocongestion_raw/fuentedatos_c4m/operacion_shougang/"
blob_name = "datos_raw_shougang_consolidado.csv"

# 5.4 Subir el archivo CSV al Blob Storage
with open("datos_raw_shougang_consolidado.csv", "rb") as data:
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(data)