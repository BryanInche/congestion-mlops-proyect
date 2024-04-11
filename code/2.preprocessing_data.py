import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import io

# Obtener la conexión al Blob Storage de Azure
#connection_string = 'DefaultEndpointsProtocol=https;AccountName=datalakemlopsd4m;AccountKey=iWT8t74/#XlqcqoR03keDVtFZPzr0PB9zDffMPaLWMUBIAjUww8uYAVkc9xRkcBtvTmUHKBvd1sB3+ASt6mGgcQ==;EndpointSuffix=core.windows.net'

# Crear el cliente del servicio Blob
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Nombre del contenedor y archivo en el Blob Storage
container_name = "raw/proyectocongestion_raw/fuentedatos_c4m/operacion_shougang/"
blob_name = "datos_raw_shougang_c4m.csv"

# Obtener el blob
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

# Leer el contenido del blob como texto
blob_data = blob_client.download_blob().content_as_text()

# Leer el archivo CSV en un DataFrame de Pandas desde el texto
datos = pd.read_csv(io.StringIO(blob_data))

# Visualizar el DataFrame
datos.head()