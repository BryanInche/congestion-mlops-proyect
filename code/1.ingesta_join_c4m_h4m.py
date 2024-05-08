#Librerias necesarias para usar el Azure DataLake y DataBrinks
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import io

#LEE C4M
# 2. Obtener conection Azure DataLake,interfaz de Azure(Claves de acceso: Key1) (Debes comentar esta variable Si NO deja hacer COMMIT DEL 
# CODIGO EN GIT,GITHUB)
# connection_string = 'DefaultEndpointsProtocol=https;AccountName=datalakemlopsd4m;AccountKey=iWT8t74/#XlqcqoR03keDVtFZPzr0PB9zDffMPaLWMUBIAjUww8uYAVkc9xRkcBtvTmUHKBvd1sB3+ASt6mGgcQ==;EndpointSuffix=core.windows.net'

# 3. Conectar al Blob Storage de Azure
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# 4. Identificamos el nombre del contenedor(container_name) y nombre del archivo(definir blob_name) en el Blob Storage
container_name_c4m = "raw/proyectocongestion_raw/fuentedatos_c4m/operacion_shougang/datos_equipos_consolidado/"

#5. Nombre en csv
#blob_name_c4m = "datos_raw_shougang_c4m.csv"
#5. Nombre en parquet
blob_name_c4m = "datos_raw_shougang_c4m.parquet"

# 5. Obtener el blob_client
blob_client_c4m = blob_service_client.get_blob_client(container=container_name_c4m, blob=blob_name_c4m)

# 6. Leer el contenido del blob como texto
#blob_data_c4m = blob_client_c4m.download_blob().content_as_text()
# 6. Leer el contenido del blob como un objeto de bytes
blob_data_c4m = blob_client_c4m.download_blob().readall()


# 7. Leer el archivo CSV en un DataFrame de Pandas desde el texto
#datos_c4m = pd.read_csv(io.StringIO(blob_data_c4m))
# 7. Leer el archivo PARQUET en un DataFrame de Pandas desde el texto
datos_c4m = pd.read_parquet(io.BytesIO(blob_data_c4m))
#-----------------------------------------------------------------------------------------------------------------------------------------

# LEE H4M
# 4. Identificamos el nombre del contenedor(container_name) y nombre del archivo(definir blob_name) en el Blob Storage
container_name_h4m = "raw/proyectocongestion_raw/fuentedatos_h4m/operacion_shougang/equipos_consolidado/"

#blob_name_h4m = "datos_raw_shougang_h4m.csv"
blob_name_h4m = "datos_raw_shougang_h4m.parquet"

# 5. Obtener el blob_client
blob_client_h4m = blob_service_client.get_blob_client(container=container_name_h4m, blob=blob_name_h4m)

# 6. Leer el contenido del blob como texto (en caso sea CSV)
#blob_data_h4m = blob_client_h4m.download_blob().content_as_text()
# 6. Leer el contenido del blob como un objeto de bytes (en caso de PARQUET)
blob_data_h4m = blob_client_h4m.download_blob().readall()

# 7. Leer el archivo CSV en un DataFrame de Pandas desde el texto
#datos_h4m = pd.read_csv(io.StringIO(blob_data_h4m))
# 7. Leer el archivo PARQUET en un DataFrame de Pandas desde el texto
datos_h4m = pd.read_parquet(io.BytesIO(blob_data_h4m))


# FORMATO DE FECHAS
# 8. Convertir a formato datetime, pero quitando la Zona horaria, y solo quedandonos hasta la parte de Segundos
datos_c4m['instant_date_t'] = pd.to_datetime(datos_c4m['instant_date_t'], errors='coerce', utc=False).dt.tz_localize(None)

# 9. Convertir a formato datetime
datos_h4m['Event Date'] = pd.to_datetime(datos_h4m['Event Date'])

# 10. Realizar el join especificando las columnas clave en cada DataFrame
resultado_c4m_h4m = pd.merge(datos_c4m, datos_h4m, left_on=['nombre', 'instant_date_t'], right_on=['Equipment','Event Date', ], how='left')
#'eq_id', 'id_equipo'

# 11. Contiene las filas que hicieron Match adecuadamente
resultado_c4m_h4m_filtrado = resultado_c4m_h4m[resultado_c4m_h4m['Event Date'].notnull()]

# 12. Guardarmos el df en el Azure Storage

# 12.1 Convertimos el df_consolidado en csv en la variable datos_total para enviarlo al Blob Storage Azure
#datos_total = resultado_c4m_h4m_filtrado.to_csv(index=False)
# 12.1 Convertimos el df_consolidado en parquet en la variable datos_total para enviarlo al Blob Storage Azure
datos_total = resultado_c4m_h4m_filtrado.to_parquet(engine='pyarrow')

# 12.2 Identificamos el nombre del contenedor(container_name) y nombre del archivo(definir blob_name) en el Blob Storage
container_name_consolidado = "raw/proyectocongestion_raw/fuentedatos_consolidado/"

#blob_name_consolidado = "datos_raw_shougang_c4m_h4m.csv"
blob_name_consolidado = "datos_raw_shougang_c4m_h4m.parquet"

# 12.3 Guardar el archivo CSV al Blob Storage
blob_client = blob_service_client.get_blob_client(container=container_name_consolidado, blob=blob_name_consolidado)

# 12.4 Verificar si existe el archivo 
if blob_client.exists():
    # Eliminar el archivo existente(para luego guardar la actualizacion)
    blob_client.delete_blob()

# 13. Guardamos los datos en el Blob Storage
blob_client.upload_blob(datos_total)
