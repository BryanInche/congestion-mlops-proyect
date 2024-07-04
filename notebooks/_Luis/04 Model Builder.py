# Databricks notebook source
#Tratamiento de datos
import pandas as pd
import numpy as np

#Graficos
import matplotlib.pyplot as plt
import seaborn as sns
from statsmodels.graphics.tsaplots import plot_acf
from statsmodels.graphics.tsaplots import plot_pacf
import plotly.express as px

# COMMAND ----------

datos_cong_balanceado.index.min(),datos_cong_balanceado.index.max()

# COMMAND ----------

#Separacion de los datos en Train-Validation-Test
datos_cong_balanceado = datos_cong_balanceado.loc['2024-03-27 01:15:28' : '2024-04-23 23:59:30']
fin_train = '2024-04-15 23:59:30'
fin_validacion = '2024-04-19 23:59:30'
datos_train = datos_cong_balanceado.loc[:fin_train, :] #20 dias
datos_val = datos_cong_balanceado.loc[fin_train:fin_validacion, :] #5 dias
datos_test = datos_cong_balanceado.loc[fin_validacion: , :] # 5 dias

print(f"Fechas train: {datos_train.index.min()} ... {datos_train.index.max()} (n={len(datos_train)})")
print(f"Fechas validacion: {datos_val.index.min()} ... {datos_val.index.max()} (n={len(datos_val)})")
print(f"Fechas test: {datos_test.index.min()} ... {datos_test.index.max()} (n={len(datos_test)})")

# COMMAND ----------

#Grafico de la serie temporal

#crea una nueva figura y un par de ejes (axes) dentro de la figura
fig, ax = plt.subplots(figsize=(15,4)) # figsize=(15,4) establece el tamaño de la figura en 15 unidades de ancho y 4 unidades de alto.

#Las series se colorean automáticamente según una paleta de colores predefinida por matplotlib
datos_train.congestion.plot(ax=ax, label="entrenamiento", linewidth=1)
datos_val.congestion.plot(ax=ax, label="validacion", linewidth=1)
datos_test.congestion.plot(ax=ax, label="test", linewidth=1)
ax.set_title('Congestion ')
ax.legend();

# COMMAND ----------

#Grafico autocorrelación

#Crea una figura y un conjunto de ejes (ax)
fig, ax = plt.subplots(figsize=(10,4))

#Aquí utilizamos la función plot_acf para trazar la función de autocorrelación
#ax=ax indica que el gráfico se dibujará en los ejes que has creado anteriormente. 
# lags=72 define el número máximo de intervalos de tiempo pasados que se considerarán al calcular la función de autocorrelación. 
plot_acf(datos_cong_balanceado.congestion, ax=ax, lags=800)
plt.show()

#Podemos ver que la correlacion de la prediccion esta altamente correlaciona con el la demanda en el dia anterior a la misma hora.

#Grafico de Autocorrelacion parcial
fig, ax = plt.subplots(figsize=(10,4))

#plot_pacf para trazar la función de autocorrelación parcial
plot_pacf(datos_cong_balanceado.congestion, ax=ax, lags=60)

# PACF muestra la correlación directa entre la serie y sus 
# valores anteriores hasta 60 intervalos de tiempo atrás, teniendo en cuenta las correlaciones indirecta
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.1.4 Algoritmo de LGBM(Ligth Bosting)

# COMMAND ----------

# Modelado y Forescasting
from lightgbm import LGBMRegressor, LGBMClassifier    #Regressor mas potente de gradient bossting
from skforecast.ForecasterAutoreg import ForecasterAutoreg  # Modelo autoregressivo
from skforecast.model_selection import grid_search_forecaster # Encontrar los mejores hiperparametros(optimizar el modelo)
from skforecast.model_selection import backtesting_forecaster # Evaluar el modelo si hubiese estado en produccion

# COMMAND ----------

#Crear  y entrenar forescaster
forescaster = ForecasterAutoreg(
    regressor=LGBMClassifier(max_depth=3, learning_rate=0.1, n_estimators=70),
    lags= 5 #[1, 2, 3, 24]
)
forescaster #forescaster.summary()

# COMMAND ----------

forescaster.fit(y=datos_cong_balanceado.loc[:fin_validacion, 'congestion']) #Entrenamiento con conjuntos de train + validacion
forescaster

# COMMAND ----------

predicciones = forescaster.predict(steps=1000)
predicciones

# COMMAND ----------

forescaster.last_window

# COMMAND ----------

# Obtener las probabilidades para las predicciones
probabilidades = forescaster.regressor.predict_proba(forescaster.last_window.values.reshape(1, -1))

# COMMAND ----------

# Inicializar una lista para almacenar las probabilidades para cada predicción
probabilidades_predicciones = []

# Iterar sobre las predicciones y obtener las probabilidades para cada una
for pred in predicciones:
    # Obtener las probabilidades para la predicción actual
    proba = forescaster.regressor.predict_proba(forescaster.last_window.values.reshape(1, -1))
    probabilidades_predicciones.append(proba)

# COMMAND ----------

probabilidades_predicciones
