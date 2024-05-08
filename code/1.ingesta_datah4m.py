import pandas as pd
from io import BytesIO
from azure.storage.blob import BlobServiceClient

# Establecer la conexión al servicio de almacenamiento en la nube
# connection_string = 'DefaultEndpointsProtocol=https;AccountName=datalakemlopsd4m;AccountKey=iWT8t74/#XlqcqoR03keDVtFZPzr0PB9zDffMPaLWMUBIAjUww8uYAVkc9xRkcBtvTmUHKBvd1sB3+ASt6mGgcQ==;EndpointSuffix=core.windows.net'
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Listas para almacenar los DataFrames de cada equipo
dfs_equipo_fc89 = []
dfs_equipo_fc95 = []
dfs_equipo_fc98 = []
### Agregar mas equipos si es necesario .............................................

container_name_join = "raw"

# Rutas de las carpetas para cada equipo
folder_path_fc89 = "proyectocongestion_raw/fuentedatos_h4m/operacion_shougang/equipo_fc89/"
folder_path_fc95 = "proyectocongestion_raw/fuentedatos_h4m/operacion_shougang/equipo_fc95/"
folder_path_fc98 = "proyectocongestion_raw/fuentedatos_h4m/operacion_shougang/equipo_fc98/"
### Agregar mas equipos si es necesario .............................................


# Función para leer los archivos CSV y almacenarlos en DataFrames
def read_csv_files(container_client, folder_path, dfs_equipo):
    blobs = container_client.list_blobs(name_starts_with=folder_path)
    csv_files = [blob.name for blob in blobs if blob.name.endswith('.csv')]
    for csv_file in csv_files:
        blob_client = blob_service_client.get_blob_client(container=container_name_join, blob=csv_file)
        blob_data = blob_client.download_blob()
        bytes_io = BytesIO()
        blob_data.download_to_stream(bytes_io)
        bytes_io.seek(0)
        df = pd.read_csv(bytes_io)
        dfs_equipo.append(df)

# Conectar al contenedor
container_client = blob_service_client.get_container_client("raw")

# Leer archivos CSV para equipo fc89
read_csv_files(container_client, folder_path_fc89, dfs_equipo_fc89)

# Leer archivos CSV para equipo fc95
read_csv_files(container_client, folder_path_fc95, dfs_equipo_fc95)

# Leer archivos CSV para equipo fc98
read_csv_files(container_client, folder_path_fc98, dfs_equipo_fc98)

### Agregar mas equipos si es necesario .............................................

# Inicializar todos los DataFrames para evitar errores
result_df_fc89 = pd.DataFrame()
result_df_fc95 = pd.DataFrame()
result_df_fc98 = pd.DataFrame()
### Agregar mas equipos si es necesario .............................................


# Concatenar los DataFrames si hay datos
if dfs_equipo_fc89:
    result_df_fc89 = pd.concat(dfs_equipo_fc89, ignore_index=True)
else:
    result_df_fc89 = pd.DataFrame()  # Inicializar como DataFrame vacío

if dfs_equipo_fc95:
    result_df_fc95 = pd.concat(dfs_equipo_fc95, ignore_index=True)
else:
    result_df_fc95 = pd.DataFrame()  # Inicializar como DataFrame vacío

if dfs_equipo_fc98:
    result_df_fc98 = pd.concat(dfs_equipo_fc98, ignore_index=True)
else:
    result_df_fc98 = pd.DataFrame()  # Inicializar como DataFrame vacío
### Agregar mas equipos si es necesario .............................................

# Concatenar los DataFrames de todos los equipos en uno solo si hay datos
if any([dfs_equipo_fc89, dfs_equipo_fc95, dfs_equipo_fc98]):
    resultado_h4m_allequipos = pd.concat([result_df_fc89, result_df_fc95, result_df_fc98], ignore_index=True)

    # Convertir el DataFrame consolidado en un archivo CSV
    #datos_total = resultado_h4m_allequipos.to_csv(index=False)
    # Convertir el DataFrame consolidado en un archivo PARQUET
    datos_total = resultado_h4m_allequipos.to_parquet(engine='pyarrow')

    # Subir el archivo CSV al Blob Storage
    container_name_consolidado = "raw/proyectocongestion_raw/fuentedatos_h4m/operacion_shougang/equipos_consolidado/"

    #blob_name_consolidado = "datos_raw_shougang_h4m.csv"
    blob_name_consolidado = "datos_raw_shougang_h4m.parquet"

    blob_client = blob_service_client.get_blob_client(container=container_name_consolidado, blob=blob_name_consolidado)

    # Verificar si existe el archivo 
    if blob_client.exists():
        # Eliminar el archivo existente(para luego guardar la actualizacion)
        blob_client.delete_blob()

    blob_client.upload_blob(datos_total)