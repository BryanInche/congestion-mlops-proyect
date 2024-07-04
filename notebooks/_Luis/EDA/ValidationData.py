# Tratamiento de datos
# ==============================================================================
import pandas as pd
import numpy as np

# Gráficos
# ==============================================================================
import matplotlib.pyplot as plt
from matplotlib import style
import matplotlib.pyplot as plt
from matplotlib import cm
from matplotlib.colors import ListedColormap, LinearSegmentedColormap
import matplotlib.colors
import matplotlib.gridspec as gridspec

import pylab
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
import plotly
import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as ff

import seaborn as sns
sns.set_style('darkgrid')

# Preprocesado y análisis
# ==============================================================================
import statsmodels.api as sm
# import pingouin as pg
from scipy import stats
from scipy.stats import pearsonr
import math
import statsmodels.stats as smst

# Configuración matplotlib
# ==============================================================================
plt.style.use('ggplot')

# Configuración warnings
# ==============================================================================
import warnings
warnings.filterwarnings('ignore')


# Validacion de la data (vacios, nulos)
class Validation_data():
    def __init__(self, data, ruta_report = '../02_Resultados/') -> None:
        self.data = data
        self.ruta_report = ruta_report
        pass
    def Missing_data(self, save_graf=False):
        total_datos_perdidos = self.data.isnull().sum().sort_values(ascending=False)
        porc_perdidos = (self.data.isnull().sum()/self.data.isnull().count())*100
        round_porc_perdidos = (round(porc_perdidos, 2)).sort_values(ascending=False)
        datos_perdidos = pd.concat([total_datos_perdidos, porc_perdidos], axis=1, keys=['Datos Perdidos','Porcentaje de Datos Perdidos'])
        if save_graf:
            datos_perdidos.to_excel(f"{self.ruta_report}Reporte_Validacion de dato faltantes.xlsx")
        return datos_perdidos
    def Duplicated_data(self, save_graf=False):
        size = self.data.shape[0]
        data_help = self.data.duplicated().to_frame()
        data_help.reset_index(inplace=True,drop=False)
        data_help.rename(columns ={"index":"valour",0:"type"}, inplace=True)
        data_help = data_help.groupby(["type"], as_index = False).count()
        mask = data_help["type"] == False
        data_help.loc[mask,"type"] = "No Duplicado"
        data_help.loc[~mask,"type"] = "Duplicado"
        if data_help["valour"][0] == size:
            dic_help = {'type':"Duplicado", 'valour':0}
            df_help = pd.DataFrame(dic_help,index=[1])
            data_help = pd.concat([data_help, df_help])
        if save_graf:
            data_help.to_excel(f"{self.ruta_report}Reporte_Validacion de dato duplicado.xlsx")
        return data_help
    def Dealing_data(self):
        missing_data = self.Missing_data()
        data_help = self.data.drop((self.data[self.data['Datos Perdidos']>0.1]).index,1)
        return data_help
    def Report_Analisys(self, n_top, dealing_data=False, save_graf=False):
        data = self.Missing_data()
        data.head(n_top).plot.bar(color={"red","green"})
        plt.title(f"Top {n_top} con datos faltantes")
        plt.ylabel("Cantidad de datos faltantes")
        plt.xlabel("Elementos")
        if save_graf:
            # data.to_excel(f"{self.ruta_report}Report_MissingData.xlsx", index=False)
            plt.savefig(f"{self.ruta_report}Grafico del top {n_top} con valores faltantes.png")
        plt.show()

        data = self.Duplicated_data()
        data.plot.bar(color={"red","green"})
        plt.legend(["No Duplicado","Duplicado"])
        plt.title(f"Datos duplicados")
        plt.ylabel("Cantidad de datos duplicados")
        plt.xlabel("Elementos")
        if save_graf:
            # data.to_excel(f"{self.ruta_report}Report_DuplicateData.xlsx", index=False)
            plt.savefig(f"{self.ruta_report}Grafico del top {n_top} con valores duplicados.png")
        plt.show()

        if dealing_data:
            new_data = self.Dealing_data()
            return new_data


class DataTypeAnalysis():
    def __init__(self, data, ruta_report = '../02_Resultados/') -> None:
        self.data = data
        self.ruta_report = ruta_report
        pass
    def DataExploration(self, save_graf=False):
        lst_types = [str(self.data.dtypes[i]) for i in range(len(self.data.dtypes))]
        class_types = list(set(lst_types))
        data_help = self.data.dtypes.to_frame()
        data_help.reset_index(inplace=True,drop=False)
        data_help.rename(columns ={"index":"tipo dato",0:"type"}, inplace=True)
        if save_graf:
            data_help.to_excel(f"{self.ruta_report}Type_Variable.xlsx", index=False)
        return data_help
    def Report_Analisys(self, save_graf=False):
        data = self.DataExploration()
        data.groupby(["type"]).count().sort_values('tipo dato', ascending=False).plot.bar()
        plt.title("Tipos de variable")
        plt.xlabel("Cantidad")
        plt.ylabel("Elementos")
        plt.xticks(rotation = 0)
        if save_graf:
            # data.to_excel(f"{self.ruta_report}Type_Variable.xlsx", index=False)
            plt.savefig(f"{self.ruta_report}Tipo de variable de los datos.png")
        plt.show()