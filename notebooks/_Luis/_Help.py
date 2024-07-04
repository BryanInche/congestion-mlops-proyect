# Databricks notebook source
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# COMMAND ----------

#1. Leemos los datos de PROCEESED la tabla Delta
df_delta = spark.read.csv("/mnt/datalakemlopsd4m/demo/01 ProductividadTurnoDiaSemana/Ciclos2024-01-01-Ambos.csv", header=True)
datos = df_delta.toPandas()
datos.head()

# COMMAND ----------

datos
