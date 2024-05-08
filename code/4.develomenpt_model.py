#Librerias que se usaran
import pandas as pd  # Libreria para administrar tablas, y realizar trabajos con distintas formas de tablas o dataframes
import numpy as np   # Libreria para poder hacer operaciones matematicas y matriciales 
import pandas as pd
from pyspark.sql.functions import col

#1. Leemos los datos de TABLA CARACTERISTICAS la tabla Delta de storage Presentation 
df_delta = spark.read.format("delta").load("/mnt/datalakemlopsd4m/presentation/proyectocongestion_presentation/tablacaracteristicas_congestion_tabladelta")
datos = df_delta.toPandas()

# 2. Ajuste de los datos a Series Temporales para ingresar datos al modelo de Machine learning
# 2.1 Se establece la columna 'Time' como el índice del DataFrame "datos"
datos = datos.set_index('instant_date_t')

# 2.2 Ordenamos el dataset de forma ascendente segun el datetime
datos.sort_index(inplace=True)

# 2.3 Identificamos la periocidad de la serie temporal
df_time_diffs = datos.index.to_series().diff().dt.total_seconds()

# 2.4 Eliminamos o Filtramos las filas donde la diferencia es distinta de cero (Con ello eliminamos las filas o registros de fechas duplicadas)
datos = datos[df_time_diffs != 0]

# 2.5. # Reinterpolar el dataset con una periosidad en especifico
# # 'S' o 'seg' : Segundo, 'T' o 'min': Minuto,'H' o 'Hr': Hora,'D': Día,'W': Semana,'M': Mes,'Q': Trimestre,'Y': Año
datos = datos.asfreq(freq='T', method='bfill')


# 3. Datos Train, Validation, Test




