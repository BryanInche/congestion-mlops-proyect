# Tratamiento de datos
# ==============================================================================
import pandas as pd
import numpy as np
from itertools import combinations

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


class StatisticalAnalysis():
    def __init__(self, data, columns, category, ruta_report = '../02_Resultados/') -> None:
        self.data = data
        self.columns = columns
        self.category = category
        self.size = 15
        self.ruta_report = ruta_report
        pass
    def DescriptiveStatistics(self, alpha = 0.05, tratar=False, category=False, save_report=False):
        columnss = ["nobs","missing","mean","median","std","min","max"]
        if category:
            for cate in self.category:
                clasification = list(set(self.data[cate]))
                for clasi in clasification:
                    if tratar:
                        #["nobs","missing","mean","std_err","std","iqr", "mad","coef_var","range","max","min","skew","kurtosis", "mode","median"]
                        df_help = pd.DataFrame(columns=columnss,index=["CORTE"])
                        df_help = df_help.T
                        data_cate = self.data[self.data[cate] == clasi]
                        for i in range(len(self.columns)):
                            data_new = data_cate[data_cate[self.columns[i]]>0]
                            #print(data_new.shape)
                            if data_new.shape[0] != 0:
                                data_help = smst.descriptivestats.describe(data_new[self.columns[i]],
                                                                stats = columnss,
                                                                alpha = alpha,
                                                                )
                            else:
                                continue

                            df_help = pd.concat([df_help,data_help], axis=1)
                        

                        
                        df_help = df_help.drop(["CORTE"], axis=1).copy()
                        data_help = df_help.copy()
                        

                    else:
                        df_help = pd.DataFrame(columns=columnss,index=["CORTE"])
                        df_help = df_help.T
                        data_cate = self.data[self.data[cate] == clasi]
                        for i in range(len(self.columns)):
                            data_help = smst.descriptivestats.describe(self.data[(self.data[self.columns[i]]>0)&(self.data[cate] == clasi)],
                                                            stats = columnss,
                                                            alpha = alpha,
                                                            )
                    if save_report:
                        data_help.to_excel(f"{self.ruta_report}00_Estadistica_Descriptiva_{cate}_{clasi}.xlsx")
        else:
            if tratar:
                df_help = pd.DataFrame(columns=columnss,index=["CORTE"])
                df_help = df_help.T
                for i in range(len(self.columns)):
                    data_new = self.data[self.data[self.columns[i]]>0]
                    data_help = smst.descriptivestats.describe(data_new[self.columns[i]],
                                                    stats = columnss,
                                                    alpha = alpha,
                                                    )
                    df_help = pd.concat([df_help,data_help], axis=1)
                df_help = df_help.drop(["CORTE"], axis=1).copy()
                data_help = df_help.copy()
                

            else:
                data_help = smst.descriptivestats.describe(self.data[self.columns],
                                                stats = columnss,
                                                alpha = alpha,
                                                )
            if save_report:
                str_columns = str()
                for col in self.columns:
                    str_columns = str_columns + ', ' + col
                data_help = data_help.T[columnss].T
                data_help.to_excel(f"{self.ruta_report}00_Estadistica_Descriptiva.xlsx")


        return data_help

    def NormalityTest(self, test_shapiro = False, test_DAgostino = False):
        for elemento in self.columns:
            print(f"-------- Reporte del test de  Normalidad {elemento} --------")
            if test_shapiro:
                # Normalidad de los residuos Shapiro-Wilk test
                # ==============================================================================
                shapiro_test = stats.shapiro(self.data[elemento])
                print(f"Variable {elemento}: {shapiro_test}")
            if test_DAgostino:
                # Normalidad de los residuos D'Agostino's K-squared test
                # ==============================================================================
                k2, p_value = stats.normaltest(self.data[elemento])
                print(f"Variable {elemento}: Estadítico = {k2}, p-value = {p_value}")
    def HomoHetero_scedasticityTest(self):
        for elemento in range(len(self.columns)): 
            if elemento != (len(self.columns)-1):
                print(f"-------- Reporte del test de  Homo-Hetero scedasticidad {self.columns[elemento]} y {self.columns[elemento+1]} --------")
                # Levene test - No tiene la seguridad de distribucion normal
                # ==============================================================================
                levene_test = stats.levene(self.data[self.columns[elemento]], self.data[self.columns[elemento+1]], center='median')
                print(f" Levene test: {levene_test}")

                # Bartlett test - Si tienen distribucion normal
                # ==============================================================================
                bartlett_test = stats.bartlett(self.data[self.columns[elemento]], self.data[self.columns[elemento+1]])
                print(f" Bartlett test: {bartlett_test}")

                # Fligner-Killeen test - No tiene la seguridad de distribucion normal (mas robusto)
                # ==============================================================================
                fligner_test = stats.fligner(self.data[self.columns[elemento]], self.data[self.columns[elemento+1]], center='median')
                print(f" Fligner-Killeen test: {fligner_test}")
    
    def AutocorrelationPlot(self, save_graf=False):
        for elemento in self.columns:
            print(f"-------- Autocorrelacion de {elemento} --------")
            fig, axs = plt.subplots(nrows=1, ncols=1, figsize=(15, 7))
            pd.plotting.autocorrelation_plot(series = self.data[self.data[elemento]>0][elemento],
                        ax= axs)
            axs.tick_params(labelsize = self.size)
            plt.xlabel(f"Lag - {elemento}", fontdict={"size":self.size})
            plt.ylabel("Autocorrelacion", fontdict={"size":self.size})
            plt.title("Histograma", fontdict={"size":self.size}, fontweight = "bold")
            if save_graf:
                plt.savefig(f"{self.ruta_report}00_Grafico de Autocorrelacion de {elemento}.png")
    
    def Histogram(self, bins=50, hue=None, log_scale=False, tratar=False, save_graf=False):
        for elemento in self.columns:
            if tratar:
                data = self.data[self.data[elemento] > 0]
            else:
                data = self.data
            max_lim = data[elemento].mean() + 3*data[elemento].std()
            print(f"-------- Histograma de {elemento} --------")
            fig, axs = plt.subplots(nrows=1, ncols=1, figsize=(7, 7))
            sns.histplot(x=elemento,
                        data=data,
                        hue = hue,
                        log_scale = log_scale,
                        palette="gist_rainbow",
                        element="step",
                        color='dodgerblue',
                        ax= axs,kde=True
                        )
            axs.tick_params(labelsize = self.size)
            plt.xlabel(f"{elemento}", fontdict={"size":self.size})
            plt.ylabel("Cantidad", fontdict={"size":self.size})
            plt.xlim(0,max_lim)
            plt.title("Histograma", fontdict={"size":self.size}, fontweight = "bold")
            if save_graf:
                if hue==None:
                    plt.savefig(f"{self.ruta_report}01_Histograma_{elemento}.png")
                else:
                    plt.savefig(f"{self.ruta_report}01_Histograma_{elemento}_{hue}.png")
    
    def BoxPlot(self, scala_x = "linear", save_graf=False):
        for elemento in self.columns:
            print(f"-------- Diagrama de caja y bigotes de {elemento} --------")
            fig, axs = plt.subplots(nrows=1, ncols=1, figsize=(15, 7))
            sns.boxplot(x=elemento,
                        data=self.data[self.data[elemento] > 0],
                        color='dodgerblue',
                        ax= axs)
            sns.stripplot(x = elemento,
                        data = self.data,
                        jitter = 0.2,
                        size = 1.5,
                        color = 'dodgerblue')
            axs.tick_params(labelsize = self.size)
            plt.xscale(scala_x)
            plt.xlabel(f"{elemento}", fontdict={"size":self.size})
            plt.title("Grafico de caja", fontdict={"size":self.size}, fontweight = "bold")
            if save_graf:
                plt.savefig(f"{self.ruta_report}02_Grafico de caja de {elemento}.png")
    
    def ScatterPlot(self, tratar = False, use_category = False, save_graf = False):
        # for i in range(len(self.columns)):
        #     print(f"-------- Diagrama de dispersion --------")
        #     lst_total = self.columns.copy()
        #     lst_eval = self.columns[i]
        #     lst_total.remove(lst_eval)
        #     lst_conj = lst_total
        #     for j in range(len(lst_conj)):
        for (i,j) in combinations(self.columns.copy(), 2):
            if tratar:
                data = self.data[(self.data[i]>0) & (self.data[j]>0)]
            else:
                data = self.data
            if use_category == False:
                fig, ax = plt.subplots(1, 1, figsize=(15,7))
                sns.scatterplot(
                            data=data,
                            x=i,
                            y=j,
                            alpha= 0.6,
                            color='dodgerblue',
                            palette='hls',
                            ax=ax)
                ax.set_xlabel(f"{i}")
                ax.set_ylabel(f"{j}")
                plt.xlabel(f"{i}", fontdict={"size" : self.size})
                plt.ylabel(f"{j}", fontdict={"size" : self.size})
                plt.tick_params(labelsize = self.size)
                plt.title(f"Diagrama de dispersión entre {i} y {j}", fontdict={"size" : self.size}, fontweight = "bold")
                if save_graf:
                    plt.savefig(f"{self.ruta_report}03_Diagrama de dispersión entre {i} y {j}.png")
                plt.show()
            else:
                fig, ax = plt.subplots(1, 1, figsize=(15,7))
                sns.scatterplot(
                            data=data,
                            x=i,
                            y=j,
                            hue=self.category[0],
                            alpha= 0.6,
                            color='dodgerblue',
                            palette='hls',
                            ax=ax)
                ax.set_xlabel(f"{i}")
                ax.set_ylabel(f"{j}")
                plt.xlabel(f"{i}", fontdict={"size" : self.size})
                plt.ylabel(f"{j}", fontdict={"size" : self.size})
                plt.tick_params(labelsize = self.size)
                plt.title(f"Diagrama de dispersión entre {i} y {j}", fontdict={"size" : self.size}, fontweight = "bold")
                if save_graf:
                    plt.savefig(f"{self.ruta_report}03_Diagrama de dispersión entre {i} y {j}.png")
                plt.show()

    
    def QQPlot(self, save_graf = False):
        for elemento in self.columns:
            print(f"-------- Grafico de QQPlot de {elemento} --------")
            # Gráfico Q-Q
            # ==============================================================================
            fig, axs = plt.subplots(nrows=1, ncols=1, figsize=(15, 7))

            sm.qqplot(
                data = self.data[self.data[elemento] > 0][elemento],
                fit   = True,
                line  = 'q',
                alpha = 0.4,
                lw    = 2,
                ax    = axs
            )
            axs.set_title(f'Gráfico Q-Q de {elemento}', fontsize = self.size, fontweight = "bold")
            axs.tick_params(labelsize = self.size)
            plt.xlabel("Cuartiles teóticos", fontdict={"size":self.size})
            plt.ylabel("Cuantiles de muestra", fontdict={"size":self.size})
            if save_graf:
                plt.savefig(f"{self.ruta_report}04_QQPlot de {elemento}.png")
            plt.show()
    
    def PPPlot(self, save_graf = False):
        for elemento in self.columns:
            print(f"-------- Grafico de PPPlot de {elemento} --------")
            # Gráfico P-P
            # ==============================================================================
            fig, axs = plt.subplots(nrows=1, ncols=1, figsize=(15, 7))

            stats.probplot(
                self.data[self.data[elemento] > 0][elemento],
                dist="norm",
                plot=axs)
            axs.set_title(f'Gráfico P-P de {elemento}', fontsize = self.size, fontweight = "bold")
            axs.tick_params(labelsize = self.size)
            plt.xlabel("Cuartiles teóticos", fontdict={"size":self.size})
            plt.ylabel("Valores ordenados", fontdict={"size":self.size})
            if save_graf:
                plt.savefig(f"{self.ruta_report}05_Ploteo estadistico de {elemento}.png")
            plt.show()
    
    def PlotGroup(self, filtro_1, filtro_2 = None, kind = "box", scala_y = "linear", points_figure = False, save_graf = False):
        for elemento in self.columns:
            print(f"-------- Grafico acumulativo de {elemento} filtado por {filtro_1} --------")
            ax = sns.catplot(data = self.data[self.data[elemento] > 0],
                            x = filtro_1,
                            y = elemento,
                            hue = filtro_2,
                            kind = kind,
                            height = 10,
                            palette = "gist_rainbow")
            if points_figure:
                ax = sns.stripplot(data = self.data[self.data[elemento] > 0],
                                x = filtro_1,
                                y = elemento,
                                hue = filtro_2,
                                jitter = 0.2,
                                size = 1.5)
            plt.yscale(scala_y)
            if save_graf:
                plt.savefig(f"{self.ruta_report}06_Diagrama de caja y bigotes de {elemento} por tipo de material"+".png")
            plt.show()
    
    def HeadMatrix(self, method_analysis='pearson', category=False, save_graf = False):
        if category == False:
            '''
            method_analysis = 'pearson', 'kendall', 'spearman'
            '''
            # Heatmap matriz de correlaciones
            # ==============================================================================
            fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(10, 10))

            sns.heatmap(
                self.data[self.columns].corr(method = method_analysis),
                annot     = True,
                fmt       =".2f",
                cbar      = False,
                annot_kws = {"size": self.size},
                vmin      = -1,
                vmax      = 1,
                center    = 0,
                cmap      = sns.diverging_palette(20, 220, n=200),
                square    = True,
                ax        = ax,
                linewidths = 5,
            )

            ax.set_xticklabels(
                ax.get_xticklabels(),
                rotation = 0,
                horizontalalignment = 'center',
            )
            ax.set_title('Matriz de correlación', fontsize = self.size, fontweight = "bold")
            ax.tick_params(labelsize = self.size)
            plt.tight_layout()
            if save_graf:
                plt.savefig(f"{self.ruta_report}07_Mapa de correlación.png")
            plt.show()
        else:
            '''
            method_analysis = 'pearson', 'kendall', 'spearman'
            '''
            for i in list(set(self.data[self.category[0]])):
                # Heatmap matriz de correlaciones
                # ==============================================================================
                data_head = self.data[self.data[self.category[0]] == i]
                fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(10, 10))

                sns.heatmap(
                    data_head[self.columns].corr(method = method_analysis),
                    annot     = True,
                    cbar      = False,
                    annot_kws = {"size": self.size},
                    vmin      = -1,
                    vmax      = 1,
                    center    = 0,
                    cmap      = sns.diverging_palette(20, 220, n=200),
                    square    = True,
                    ax        = ax,
                    linewidths = self.size,
                )

                ax.set_xticklabels(
                    ax.get_xticklabels(),
                    rotation = 45,
                    horizontalalignment = 'center',
                )
                ax.set_title('Matriz de correlación', fontsize = self.size, fontweight = "bold")
                ax.tick_params(labelsize = self.size)
                if save_graf:
                    plt.savefig(f"{self.ruta_report}07_Mapa de correlación_{self.category[0]}_{i}.png")
                plt.show()

    def Plot_Tempo_Var_Total(self, Column_Time, category = False, xlabel='Tiempo', ylabel='Value', dpi=100, save_graf=False):
        database = self.data.copy()
        database.set_index(Column_Time, inplace=True)
        for col in self.columns:
            fig, axs = plt.subplots(1, 1, figsize=(15,6), dpi=dpi)
            if category:
                list_cat = set(database[self.category[0]])
                for cat in list_cat:
                    mask = database[self.category[0]] == cat
                    database[mask][col].plot(label = cat, alpha = 0.7, ax = axs)
                # sns.lineplot(
                #     data = database,
                #     x = Column_Time,
                #     y = col,
                #     hue = self.category,
                #     ax = axs
                # )
            else:
                # sns.lineplot(
                #     data = database,
                #     x = Column_Time,
                #     y = col,
                #     ax = axs
                # )
                database[col].plot(color='tab:red', label = col, ax = axs)
            # database[col].plot()
            # for p in range(len(x)):
            #     axs.annotate(text = round(y[p],2),
            #                     xy = (x[p], y[p]),
            #                     ha='center',
            #                     va='center',
            #                     xytext=(0, 10),
            #                     textcoords='offset points',
            #                     fontsize=7,
            #                     )
            plt.legend(ncol=1, prop=dict(family='monospace', size='x-small'), fontsize='small', facecolor='whitesmoke', edgecolor='grey', framealpha=0.9, title='Leyenda', title_fontproperties=dict(style='oblique'))
            plt.tight_layout()
            plt.gca().set(title=col, xlabel=xlabel, ylabel=ylabel)
            if save_graf:
                plt.savefig(f"{self.ruta_report}{col}_Plot_Temporal.png")
            plt.show()

    def Plot_Tempo_Var(self, Column_Time, Column_Var, title="", xlabel='Date', ylabel='Value', dpi=100, save_graf=False):
        x = self.data[Column_Time].reset_index(drop=True).copy()
        y = self.data[Column_Var].reset_index(drop=True).copy()
        fig, axs = plt.subplots(1, 1, figsize=(15,6), dpi=dpi)
        axs.plot(x, y, color='tab:red')
        # sns.pointplot(x = x, y = y, color='tab:red', ax=axs)
        # for p in range(len(x)):
        #     axs.annotate(text = round(y[p],2),
        #                     xy = (x[p], y[p]),
        #                     ha='center',
        #                     va='center',
        #                     xytext=(0, 10),
        #                     textcoords='offset points',
        #                     fontsize=7,
        #                     )

        plt.gca().set(title=title, xlabel=xlabel, ylabel=ylabel)
        if save_graf:
            plt.savefig(f"{self.ruta_report}{Column_Var}_Semana_{i}.png")
        plt.show()


    def Plot_Weekly_Var(self, Column_Time, Column_Var, save_graf=False):
        string = 'a b c d e f g h i j k l m n o p q r s t u v w x y z'.replace(" ", "|")
        string += "|" + string.upper()

        nn = self.data[[Column_Time, Column_Var]]
        print("Info de la Data Original")
        print("")
        print("")
        print(nn.info())
        print("")
        print("")
        try:
            nn["Cond"] = nn[Column_Var].str.contains(string, regex=True)
            print("Info de cuntas filas no tiene dato")
            print("")
            print("")
            print(nn.info())
            print(nn[nn["Cond"] == True].groupby(by=Column_Var).count())
            print("")
            print("")
            pd_var_nan = nn[nn["Cond"] == True].copy()
            pd_var = nn[nn["Cond"] != True].copy()
            pd_var[Column_Var] = pd.to_numeric(pd_var[Column_Var])
            pd_var["WEEK_"] = pd_var[Column_Time].dt.strftime("%W")
            pd_var_nan["WEEK_"] = pd_var_nan[Column_Time].dt.strftime("%W")
            pd_var["WEEK"] = pd.to_numeric(pd_var["WEEK_"])
            pd_var_nan["WEEK"] = pd.to_numeric(pd_var_nan["WEEK_"])
            pd_var = pd_var.set_index(Column_Time, drop = False)
            pd_var.rename(columns={Column_Time: "time"}, inplace= True)
            pd_var_nan = pd_var_nan.set_index(Column_Time, drop = False)
            pd_var_nan.rename(columns={Column_Time: "time"}, inplace= True)
        except:
            pd_var = nn.copy()
            pd_var[Column_Var] = pd.to_numeric(pd_var[Column_Var])
            pd_var["WEEK_"] = pd_var[Column_Time].dt.strftime("%W")
            pd_var["WEEK"] = pd.to_numeric(pd_var["WEEK_"])
            pd_var = pd_var.set_index(Column_Time, drop = False)
            pd_var.rename(columns={Column_Time: "time"}, inplace= True)
            pd_var_nan = pd.DataFrame(columns=pd_var.columns)

        print("Descripcion de la variable")
        print("")
        print("")
        print(pd_var[[Column_Var]].describe())
        print("")
        print("")
        for i in list(set(pd_var["WEEK"])):
            pd_var_ = pd_var[pd_var["WEEK"] == i].copy()
            print(f"SEMANA  {i}")
            pd_var_[Column_Var].plot(figsize=(10, 8), ylim=(0,200))
            if save_graf:
                plt.savefig(f"{self.ruta_report}{Column_Var}_Semana_{i}.png")
            plt.show()
            if pd_var_nan[pd_var_nan["WEEK"] == i].shape[0] != 0:
                print(pd_var_nan[pd_var_nan["WEEK"] == i].sort_values(by=["time"])[[Column_Var]])
            print("--------------------------------------------------------")

    def BoxPlot_Weekly_Season_Var(self, Column_Time, dic_filter = None, save_graf = False):
        database = self.data.copy()
        database.set_index(Column_Time, inplace = True)
        database['Mes'] = database.index.month
        database['Semana'] = database.index.weekday

        for col in self.columns:
            # Draw Plot
            fig, axes = plt.subplots(1, 2, figsize=(20,7), dpi= 80)

            if (dic_filter != None):
                mask = (database[col] > 0) & (database[col] < dic_filter[col])
                sns.boxplot(x='Mes', y = col, data=database[mask], ax=axes[0])
                sns.boxplot(x='Semana', y = col, data=database[mask])
            else:
                sns.boxplot(x='Mes', y = col, data=database, ax=axes[0])
                sns.boxplot(x='Semana', y = col, data=database)

            # Set Title
            axes[0].tick_params(axis='both',width=5, labelsize=self.size)
            axes[0].set_ylabel(col, fontsize = self.size)
            axes[0].set_xlabel('Mes', fontsize = self.size)
            axes[0].set_title('Diagrama de caja mensual\n(Tendencia)', fontsize=(self.size + 3))
            axes[1].tick_params(axis='both',width=5, labelsize=self.size)
            axes[1].set_ylabel(col, fontsize = self.size)
            axes[1].set_xlabel('Semana', fontsize = self.size)
            axes[1].set_title('Diagrama de caja semanal\n(Estacionalidad)', fontsize=(self.size + 3))

            if save_graf:
                plt.savefig(f'{self.ruta_report}BoxPlot_{col}.svg')
            plt.show()


    def PlotAnalisys(self, var, bins=30, max_lim=None, save_graf=False):
        #plt.figure(figsize=(15,7))

        fig = plt.figure(tight_layout=True, figsize=(15,7))
        gs = gridspec.GridSpec(2, 2)

        axs = fig.add_subplot(gs[0,0])

        sns.histplot(x=var,
                    data=self.data,
                    kde = True,
                    bins=bins,
                    color='dodgerblue',
                    ax = axs)
        plt.tick_params(labelsize = self.size)
        plt.xlabel(f"{var}", fontdict={"size":self.size})
        plt.ylabel("Cantidad", fontdict={"size":self.size})
        plt.xlim(0,max_lim)
        plt.title("Histograma", fontdict={"size":self.size}, fontweight = "bold")

        axs = fig.add_subplot(gs[0,1])
        # GRAFICO DE CAJAS
        sns.boxplot(x=var,
                    data=self.data,
                    color='dodgerblue',
                    ax = axs)
        plt.title("Grafico de caja", fontdict={"size":self.size}, fontweight = "bold")
        plt.tick_params(labelsize = self.size)
        plt.xlabel(f"{var}", fontdict={"size":self.size})

        # plt.subplot(1, 3, 2)
        # sns.boxplot(x=var, data=data, kind="box" , palette="gist_rainbow", ax=plt)

        axs = fig.add_subplot(gs[1, :])

        # GRAFICO DE QQPlot
        sm.qqplot(
            self.data[var],
            fit   = True,
            line  = 'q',
            alpha = 0.4,
            lw    = 2,
            ax = axs
        )
        plt.title(f'Gráfico Q-Q de {var}', fontsize = self.size, fontweight = "bold")
        plt.tick_params(labelsize = self.size)
        plt.xlabel("Cuartiles teóticos", fontdict={"size":self.size})
        plt.ylabel("Cuantiles de muestra", fontdict={"size":self.size})
        if save_graf:
            plt.savefig(f"{self.ruta_report}QQPlot de {var}.png")
        plt.show()
