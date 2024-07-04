import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
import numpy as np
import pandas as pd
from imblearn.over_sampling import SMOTE

from EDA.ValidationData import Validation_data, DataTypeAnalysis
from EDA.StatisticalAnalysis import StatisticalAnalysis
from EDA.PlotGeometryAnalysis import PlotGeometryAnalysis

#1. Leemos los datos de PROCEESED la tabla Delta
print('-------------------------- 1. Importando informacion --------------------------')
df_delta = spark.read.format("delta").load("/mnt/datalakemlopsd4m/presentation/proyectocongestion_presentation/data_congestion_serietemp_balanceada")
datos_sin_balanceo = df_delta.toPandas()

# 2. Limpieza de variables No prioritarias
print('-------------------------- 2. Limpieza de variables: Eliminar variables --------------------------')
columnas_a_eliminar = ['nombre_equipo','nombre', 'nombre_equipo','nombre','start_time_alert','end_time_alert','Event_Date']

# 2.1 Filtrar las columnas que existen en el DataFrame
columnas_existentes = [col for col in columnas_a_eliminar if col in datos_sin_balanceo.columns]

# 2.2 Verificar si hay columnas para eliminar
if columnas_existentes:
    datos_sin_balanceo.drop(columnas_existentes, axis=1, inplace=True)

# 3. Filtramos datos atipicos
print('-------------------------- 3. Limpieza de variables: Filtramos --------------------------')
# 3.1. Coordenadas (0,0,0)
mask = (datos_sin_balanceo['x'] == 0) & (datos_sin_balanceo['y'] == 0) & (datos_sin_balanceo['z'] == 0)
datos_sin_balanceo = datos_sin_balanceo[~mask]

# # 3.2. Filtramos equipos con mayor informacion
# mask = (datos_sin_balanceo['id_equipo'] == 61) | ((datos_sin_balanceo['id_equipo'] == 199))
# datos_sin_balanceo = datos_sin_balanceo[mask]

# # 3.3. Filtramos datos con error de satelites
# mask = (datos_sin_balanceo['n_sat'] > 0)
# datos_sin_balanceo = datos_sin_balanceo[mask]

# # 3.4. Filtramos rango de combustible
# mask = (datos_sin_balanceo['combustibleint_t'] <= 100) & (datos_sin_balanceo['combustibleint_t'] >= 0)
# datos_sin_balanceo = datos_sin_balanceo[mask]

# # 3.5. Filtramos el porcentaje de combustible
# mask = (datos_sin_balanceo['Hourmeter_MSPU'] <= 360)
# datos_sin_balanceo = datos_sin_balanceo[mask]

# # 3.6. Filtramos la temperatura diferencial del vehiculo
# mask = (datos_sin_balanceo['DIFF_TEMP'] >= -100)
# datos_sin_balanceo = datos_sin_balanceo[mask]

# # 3.7. Filtramos porcentaje de lubricante
# mask = (datos_sin_balanceo['AUTO_LUBE'] <= 1) & (datos_sin_balanceo['AUTO_LUBE'] >= 0)
# datos_sin_balanceo = datos_sin_balanceo[mask]

# # 3.8. Filtramos la temperatura del aire filtrado
# mask = (datos_sin_balanceo['RT_LT_EXH_TEMP'] >= -100)
# datos_sin_balanceo = datos_sin_balanceo[mask]

# # 3.9. Filtramos la presion del lubricante del diferencial del vehiculo
# mask = (datos_sin_balanceo['DIFF_LUBE_PRES'] >= -100)
# datos_sin_balanceo = datos_sin_balanceo[mask]

# # 3.10. Filtramos la temperatura del lubricante de la transmision
# mask = (datos_sin_balanceo['TRN_LUBE_TEMP'] <= 101)
# datos_sin_balanceo = datos_sin_balanceo[mask]

print('-------------------------- 4. Preprocesamiento: Balanceo de variable objetivo (congestion) --------------------------')
datos = datos_sin_balanceo.copy()

# Convertimos a index la fecha
datos = datos.set_index('instant_date_t')
datos = datos.sort_index()

#2.4 Identificamos la periocidad de la serie temporal
df_time_diffs = datos.index.to_series().diff().dt.total_seconds()
 
# # 2.4.1 Contar cuántas diferencias de tiempo tienen cada valor específico
# # 2.4.2 Mostrar los recuentos
 
#3. Eliminamos o Filtramos las filas donde la diferencia es distinta de cero (Con ello eliminamos las filas o registros de fechas duplicadas)
datos = datos[df_time_diffs != 0]

#4. # Reinterpolar el dataset con una periosidad en especifico
# 'S' o 'seg' : Segundo, 'T' o 'min': Minuto,'H' o 'Hr': Hora,'D': Día,'W': Semana,'M': Mes,'Q': Trimestre,'Y': Año
datos = datos.asfreq(freq='6S', method='bfill')

# Filtrar las fechas donde congestion == 1
fechas_congestion_1 = datos[datos['congestion'] == 1].index

# Generar un rango de fechas completo dentro del rango del DataFrame
fecha_min = datos.index.min()
fecha_max = datos.index.max()
rango_completo = pd.date_range(start=fecha_min, end=fecha_max, freq='S')

# Excluir las fechas de congestion == 1 del rango completo
fechas_disponibles = rango_completo.difference(fechas_congestion_1)

# Aplicar SMOTE para generar nuevas muestras de la clase minoritaria (congestion == 0)
smote = SMOTE(sampling_strategy='auto', random_state=42)

# Usar todas las columnas como características para SMOTE excepto 'congestion'
X = datos.drop(columns=['congestion'])
y = datos['congestion']

# Convertir las fechas del índice a números para usarlas con SMOTE
X['instant_date_num'] = X.index.astype(int) / 10**9  # Convertir a segundos

# Aplicar SMOTE
X_res, y_res = smote.fit_resample(X, y)

# Convertir de nuevo a DataFrame
X_res = pd.DataFrame(X_res, columns=X.columns)
y_res = pd.Series(y_res, name='congestion')

# Filtrar las nuevas muestras generadas por SMOTE (la diferencia entre las originales y las nuevas)
nuevas_muestras = X_res[len(datos):]
nuevas_congestion = y_res[len(datos):]

# Asignar nuevas fechas secuenciales de fechas_disponibles a las nuevas muestras
nuevas_fechas_asignadas = fechas_disponibles[:len(nuevas_muestras)]
nuevas_muestras['instant_date'] = nuevas_fechas_asignadas

# Crear el DataFrame final combinando los datos originales y las nuevas muestras
nuevas_muestras = nuevas_muestras.set_index('instant_date')
nuevas_muestras = nuevas_muestras.drop(columns=['instant_date_num'])
nuevas_muestras['congestion'] = nuevas_congestion.values

# Combinar los datos originales y las nuevas muestras
datos_balanceados = pd.concat([datos, nuevas_muestras]).sort_index()
datos_balanceados = datos_balanceados.reset_index(names='instant_date_t')

# 3. Convertir el DataFrame de Pandas a un DataFrame de Spark
print('-------------------------- 5. Guardar datos: Spark --------------------------')
spark_datos = spark.createDataFrame(datos_balanceados)
 
# 4. Guardar los datos preprocesados en una tabla Delta en el Azure Storage
# Asegurar que las columnas de fecha y hora mantengan sus tipos de datos
 
# Nombre de la tabla Delta a guardar
nombre_tabla_delta = "dbproyectocongestion_presentation.tablacaracteristicas_congestion_tabladelta_v3"
 
# 4.1 Verificar si ya existe la tabla Delta
if spark.catalog.tableExists(nombre_tabla_delta):
    # Eliminar la tabla Delta existente
    spark.sql("DROP TABLE IF EXISTS " + nombre_tabla_delta)
 
# 4.2 Guardar los datos preprocesados en una tabla Delta
spark_datos.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(nombre_tabla_delta)
print('-------------------------- 5. Finalizacion del codigo --------------------------')
