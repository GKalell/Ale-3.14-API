import pandas as pd
import numpy as np
import re

from fastapi import FastAPI

app = FastAPI()
@app.get('/peliculas_mes')

def peliculas_mes(mes):
    DF = pd.read_csv("movies_dataset.csv")
    meses = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre']
    #Verifico que el valor sea correcto.
    if mes not in meses:
        return {'mes':"valor incorrecto. Ingrese un mes", 'cantidad':"Error"}
    #Creo columna extrayendo el mes, cuento los valores y creo una lista con ellos
    DF["release_month"] = DF["release_date"].str[5:7]
    
    month_count = list(DF.release_month.value_counts().sort_index())
    #Matcheo el mes con el indice de la lista y genero la respuesta.
    indice = meses.index(mes) + 1


    respuesta = month_count[indice]
    
    return {'mes':mes, 'cantidad':respuesta}
@app.get('/peliculas_dia')
def peliculas_dia(dia):
    dias_semana = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]
    dias_semana.sort()
    DF = pd.read_csv("movies_dataset.csv")
    
    if dia not in dias_semana:
        return {'dia':"valor incorrecto. Ingrese un dia", 'cantidad':"Error"}
    
    DF["release_date"] = pd.to_datetime(DF["release_date"], errors='coerce')
    DF["dia_semana"] = DF["release_date"].dt.day_name(locale='es_ES')

    # Ordenar los días de la semana
    DF["dia_semana"] = DF["dia_semana"].astype(pd.CategoricalDtype(categories=dias_semana, ordered=True))

    day_count = DF["dia_semana"].value_counts().sort_index().tolist()
    
    indice = dias_semana.index(dia)
    respuesta = day_count[indice]
    
    return {'dia': dia, 'cantidad': respuesta}
@app.get('/franquicia')

def franquicia(franquicia):
    ## Abro el archivo y aplico groupby para tener los df de donde tomare la info que me solicitan
    DF = pd.read_csv("movies_dataset.csv")
    DF_SUM = DF.groupby(["belongs_to_collection"]).sum()
    DF_PROM = DF.groupby(["belongs_to_collection"]).mean()
    DF_COUNT = DF.groupby(["belongs_to_collection"]).count()
    lista_collection = list(DF.belongs_to_collection)
    
    ## Verifico que el valor se valido
    
    if franquicia not in lista_collection:
        return {'franquicia':"Valor incorrecto", 'cantidad':"Error", 'ganancia_total':"Error", 'ganancia_promedio':"Error"}
    
    ## Ubico los valores de la franquicia a traves de la funcion loc

    resp_cant = DF_COUNT.loc[franquicia, 'revenue']

    resp_gan_tot = DF_SUM.loc[franquicia, 'revenue']

    resp_gan_prom = DF_PROM.loc[franquicia, 'revenue']


    return {'franquicia':franquicia, 'cantidad':resp_cant, 'ganancia_total':resp_gan_tot, 'ganancia_promedio':resp_gan_prom}
@app.get('/peliculas_pais')
def peliculas_pais(pais):
    DF = pd.read_csv("movies_dataset.csv")
    list_countries = list(DF.production_countries.values)
    

    cantidad = sum(1 for item in list_countries if re.search(pais, str(item)))
    return {'pais':pais, 'cantidad':cantidad}
@app.get('/productoras')
def productoras(productora):
    # Lectura del dataset
    DF = pd.read_csv("movies_dataset.csv")

    # Limpieza de la columna production_companies
    DF['production_companies'] = DF['production_companies'].apply(lambda x: ','.join(x).strip('[]') if isinstance(x, list) else x)

    # Friltro por productora y aplicamos .sum()para obtener la ganancia total
    DF['production_companies'] = DF['production_companies'].fillna('')
    filtered = DF[DF['production_companies'].str.contains(productora)]

    resp_gan_tot = filtered['revenue'].sum()

    # Calcular la cantidad de películas producidas por la productora
    filtered_groupby = filtered.groupby(['production_companies'])['revenue'].count().reset_index()
    resp_cant = len(filtered_groupby)

    # Retornar un diccionario con los resultados
    return {'productora':productora, 'ganancia_total':resp_gan_tot, 'cantidad':resp_cant}

@app.get('/retorno')
def retorno(pelicula): 
    DF = pd.read_csv("movies_dataset.csv")
    DF.at[19714, 'budget'] = 0
    DF.at[29472, 'budget'] = 0
    DF.at[35543, 'budget'] = 0
    DF.set_index('title', inplace=True)
    list_peli =  list(DF.index.values)
    if pelicula not in list_peli:
        return {'pelicula':"Valor incorrecto", 'inversion':"Error", 'ganacia':"Error",'retorno':"Error", 'anio':"Error"}
    

    DF["budget"] = DF["budget"].astype(float)
    DF["revenue"] = DF["revenue"].fillna(0)
    DF["revenue"] = DF["revenue"].astype(float)
    resp_inver = DF.loc[pelicula , "budget"]
    resp_gan = DF.loc[pelicula , "revenue"]
    resp_ret = resp_gan - resp_inver
    resp_anio = DF.loc[pelicula , "release_year"]
    return {'pelicula':pelicula, 'inversion':resp_inver, 'ganacia':resp_gan,'retorno':resp_ret, 'anio':resp_anio}

if __name__ == '__main__':
    app.run()