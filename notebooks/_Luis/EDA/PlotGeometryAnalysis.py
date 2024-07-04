# Tratamiento de datos
# ==============================================================================
import pandas as pd
import numpy as np
import random
import os

# Gráficos
# ==============================================================================
import matplotlib as mpl
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


class PlotGeometryAnalysis():
    def __init__(self, data, lst_coordenada, size_block, ruta_report = '../02_Resultados/') -> None:
        self.data = data
        self.x = lst_coordenada[0]
        self.y = lst_coordenada[1]
        self.z = lst_coordenada[2]
        self.xs = size_block[0]
        self.ys = size_block[1]
        self.zs = size_block[2]
        self.ruta_report = ruta_report
        pass
    def Section2D(self, elemento, limit_inf, limit_sup, option_viewer = "Z", range_color=False, save_graf=False):
        # dividir de acuerdo a la dimension del bloque, en este caso seria 20x20x15
        X = (self.data[self.x].max() - self.data[self.x].min())/self.xs
        Y = (self.data[self.y].max() - self.data[self.y].min())/self.ys
        Z = (self.data[self.z].max() - self.data[self.z].min())/self.zs

        print("La cantidad de perfiles en X son: " + str(X))
        print("La cantidad de perfiles en Y son: " + str(Y))
        print("La cantidad de perfiles en Z son: " + str(Z))

        if option_viewer == "X":
            viewe_orientation = X
            coor_axi_1 = self.y
            coor_axi_2 = self.z
            coor_orientacion = self.x
            block_size = self.xs
            etiq_1 = "Norte (y)"
            etiq_2 = "Elev (z)"
        elif option_viewer == "Y":
            viewe_orientation = Y
            coor_axi_1 = self.z
            coor_axi_2 = self.x
            coor_orientacion = self.y
            block_size = self.ys
            etiq_1 = "Elev (z)"
            etiq_2 = "Este (x)"
        else:
            viewe_orientation = Z
            coor_axi_1 = self.x
            coor_axi_2 = self.y
            x_min = self.data[self.x].min()
            x_max = self.data[self.x].max()
            y_min = self.data[self.y].min()
            y_max = self.data[self.y].max()
            coor_orientacion = self.z
            block_size = self.zs
            etiq_1 = "Este (x)"
            etiq_2 = "Norte (y)"
        
        # Creando los colores, etiquetas, elemento  
        lst_element = list(set(self.data[elemento].dropna()))
            
        lst_label = [f'{elemento}_{int(label)}' for label in lst_element]

        lst_color = [list(matplotlib.colors.cnames.keys())[random.randint(30,50)] for color in range(0,146)]
        lst_color = ['gray', 'red', 'orange', 'gold','green', 'lime', 'aqua', 'navy', 'blue', 'indigo', 'darkorchid', 'fuchsia', 'crimson']
        
        plt.rcParams.update({'figure.max_open_warning' : 0})
        if range_color:
            for i in range(int(viewe_orientation)):
                data2 = self.data[(self.data[elemento] > limit_inf) & (self.data[elemento]<limit_sup) & (self.data[coor_orientacion] == self.data[coor_orientacion].max()-block_size*i)]
                x_element_min = self.data[elemento].min()
                x_element_max = self.data[elemento].mean() + 2*self.data[elemento].std()
                fig, ax = plt.subplots(figsize = (15,7))
                # cmap = cm.get_cmap('hsv', 200)
                cmap = cm.get_cmap('viridis', 200)
                norm = mpl.colors.Normalize(vmin=x_element_min, vmax=x_element_max)

                p = ax.scatter(x = data2[coor_axi_1],
                                y = data2[coor_axi_2],
                                c = data2[elemento],
                                marker = ".",
                                s=30,
                                alpha=1,
                                cmap = cmap,
                                norm=norm)

                fig.colorbar(mpl.cm.ScalarMappable(norm=norm, cmap=cmap),
                                    ax=ax,
                                    pad=0.01,
                                    location='right',
                                    label=f"{elemento}")
                ax.set_xlabel(etiq_1, fontdict={"size":15}, fontweight = "bold")
                ax.set_ylabel(etiq_2, fontdict={"size":15}, fontweight = "bold")
                ax.spines['bottom'].set_color('black')
                ax.spines['top'].set_color('black')
                ax.spines['left'].set_color('black')
                ax.spines['right'].set_color('black')
                ax.xaxis.label.set_color('black')
                ax.tick_params(axis='x', colors='black')
                ax.grid(color='black', alpha = .1)

                plt.xticks(size=13)
                plt.yticks(size=13)
                plt.title(f"Perfil {coor_orientacion} = " + str(self.data[coor_orientacion].max() - block_size*i), fontdict={"size":15}, fontweight = "bold")
                plt.xlim(x_min,x_max)
                plt.ylim(y_min,y_max)
                if save_graf:
                    plt.savefig(f"{self.ruta}MB_{coor_orientacion}_{elemento}_"+str(self.data[coor_orientacion].max() - block_size*i)+".png")
                plt.show()
        else:
            for i in range(int(viewe_orientation)):
                data2 = self.data[(self.data[elemento] > limit_inf) & (self.data[elemento]<limit_sup) & (self.data[coor_orientacion] == self.data[coor_orientacion].max()-block_size*i)]
                x_element_min = self.data[elemento].min()
                x_element_max = self.data[elemento].max()
                lst_phase_internal = list(set(data2[elemento]))
                lst_label_internal = []
                lst_color_internal = []
                for bucle in lst_phase_internal:
                    index = lst_element.index(bucle)
                    lst_label_internal.append(lst_label[index])
                    lst_color_internal.append(lst_color[index])
                fig, ax = plt.subplots(figsize = (15,7))
                for element_b, label_b, color_b  in zip(lst_phase_internal, lst_label_internal, lst_color_internal):
                    mask = (data2[elemento] == element_b)
                    data3 = data2[mask]

                    p = ax.scatter(x = data3[coor_axi_1],
                                y = data3[coor_axi_2],
                                c = color_b,
                                label = label_b,
                                marker = ".",
                                s=30,
                                alpha=1,
                                cmap = None)
                ax.legend().set_title('Legenda')
                ax.set_xlabel(etiq_1, fontdict={"size":15}, fontweight = "bold")
                ax.set_ylabel(etiq_2, fontdict={"size":15}, fontweight = "bold")
                ax.spines['bottom'].set_color('black')
                ax.spines['top'].set_color('black')
                ax.spines['left'].set_color('black')
                ax.spines['right'].set_color('black')
                ax.xaxis.label.set_color('black')
                ax.tick_params(axis='x', colors='black')
                ax.grid(color='black', alpha = .1)

                plt.xticks(size=13)
                plt.yticks(size=13)
                plt.title(f"Perfil {coor_orientacion} = " + str(self.data[coor_orientacion].max() - block_size*i), fontdict={"size":15}, fontweight = "bold")
                plt.xlim(x_min,x_max)
                plt.ylim(y_min,y_max)
                if save_graf:
                    plt.savefig(f"{self.ruta}MB_{coor_orientacion}_{elemento}_"+str(self.data[coor_orientacion].max() - block_size*i)+".png")
                plt.show()
    
    def Plot3D(self, var, azim=330, dip=25, save_graf=False):
        x = self.data[self.x]
        y = self.data[self.y]
        z = self.data[self.z]
        u = self.data[var]

        fig = plt.figure(figsize=(10,10),dpi=100, facecolor='white')
        ax = fig.add_subplot(111, projection = "3d")

        p = ax.scatter3D(x,y,zs=z,c=u, marker=".", s=5, cmap=plt.cm.rainbow)
        ax.view_init(azim=azim, elev=dip)
        ax.set_xlabel("Eje X")
        ax.set_ylabel("Eje Y")
        ax.set_zlabel("Eje Z")
        ax.set_title(f"Modelo de bloques - {var}")
        fig.colorbar(p, ax=ax, label=var)
        if save_graf:
            plt.savefig(f"{self.ruta_report}Modelo de bloques con legenda {var}.png")
        plt.show()
    
    def Plot3DInteractive(self, var):
        fix = go.Figure(data = [
                        go.Scatter3d(x = self.data[self.x],
                                    y = self.data[self.y],
                                    z = self.data[self.z],
                                    mode = "markers",
                                    showlegend=True,
                                    marker = dict(size=3,
                                    color = self.data[var],
                                    colorscale= "Viridis",
                                    showscale = True,
                                    opacity = 0.8)
                        )])
        fix.update_layout(margin=dict(l=0, r=0, b=0, t=0))
        fix.show()