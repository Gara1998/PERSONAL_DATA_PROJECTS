#Librerias
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import math
from datetime import datetime as dt, timedelta
import warnings
from IPython.display import Image, display

#Ignoramos todos los warnings
warnings.filterwarnings('ignore')

#Función general para guardar los DataFrames en archivos CSV
def guardar_tabla(tabla, ruta):
    for nombre, df in tabla:
        df.to_csv(os.path.join(ruta, f"{nombre}.csv"), index=False)

#Carga de datos
path_base='/Users/Garazi/Desktop/SPOTIFY/'
path='DATA/ORIGEN/'
archivo_1='Streaming_History_Audio_2016_2019_0.json'
archivo_2='Streaming_History_Audio_2019_2020_1.json'
archivo_3='Streaming_History_Audio_2020_2021_2.json'
archivo_4='Streaming_History_Audio_2021_2023_3.json'
archivo_5='Streaming_History_Audio_2023_2024_4.json'

df_1619=pd.read_json(path_base+path+archivo_1)
df_1920=pd.read_json(path_base+path+archivo_2)
df_2021=pd.read_json(path_base+path+archivo_3)
df_2123=pd.read_json(path_base+path+archivo_4)
df_2324=pd.read_json(path_base+path+archivo_5)

#Se juntas los datos de todos los años en un único df y eliminamos los duplicados que hemos generado
df=pd.concat([df_1619,df_1920,df_2021,df_2123,df_2324])
df.drop_duplicates(inplace=True)

#La columna ts (fecha) es de tipo object, la ponemos como datetime
df['ts'] = pd.to_datetime(df['ts'])

#Se seleccionan y se renombran las columnas
df=df[['ts','ms_played','master_metadata_track_name', 'master_metadata_album_artist_name','master_metadata_album_album_name', 'episode_name','episode_show_name', 'reason_start','reason_end', 'shuffle', 'skipped']]

df.rename(columns=({'ts':'fecha','ms_played':'milisegundos',
                   'master_metadata_track_name':'Nombre_pista',
                   'master_metadata_album_artist_name':'Nombre_artista',
                   'master_metadata_album_album_name':'Nombre_album',
                   'episode_name':'Nombre_episodio',
                   'episode_show_name':'Nombre_show_episodio',
                   'reason_start':'razon_comienzo',
                   'reason_end':'razon_fin',
                   'shuffle':'aleatorio',
                   'skipped':'saltado'}), inplace=True)

df_podcast=df[df['Nombre_show_episodio'].notna()]
df_podcast.dropna(axis=1, how='all', inplace=True)
df_music=df[~df['Nombre_show_episodio'].notna()]
df_music.dropna(axis=1, how='all', inplace=True)

#Tablas resultantes
df_comienzo=df['razon_comienzo'].drop_duplicates().reset_index(drop=True).to_frame()
df_comienzo.insert(0, 'idnt_comienzo', df_comienzo.index + 1)

df_fin=df['razon_fin'].drop_duplicates().reset_index(drop=True).to_frame()
df_fin.insert(0, 'idnt_fin', df_fin.index + 1)

df_artistas=df_music['Nombre_artista'].drop_duplicates().reset_index(drop=True).to_frame()
df_artistas.insert(0, 'idnt_artista', df_artistas.index + 1)

df_album=df_music['Nombre_album'].drop_duplicates().reset_index(drop=True).to_frame()
df_album.insert(0, 'idnt_album', df_album.index + 1)

df_cancion=df_music[['Nombre_pista','Nombre_artista','Nombre_album']].drop_duplicates().reset_index(drop=True)
df_cancion = df_cancion.merge(df_album[['idnt_album', 'Nombre_album']],how='left',on='Nombre_album')
df_cancion = df_cancion.merge(df_artistas[['idnt_artista', 'Nombre_artista']],how='left',on='Nombre_artista')
df_cancion=df_cancion[['Nombre_pista','idnt_album','idnt_artista']]
df_cancion.insert(0, 'idnt_pista', df_cancion.index + 1)

df_musica=df_music[['fecha','milisegundos','Nombre_pista','razon_comienzo','razon_fin','aleatorio','saltado']]
df_musica=df_musica.merge(df_cancion[['Nombre_pista','idnt_pista']],how='left',on='Nombre_pista')
df_musica=df_musica.merge(df_comienzo[['razon_comienzo','idnt_comienzo']],how='left',on='razon_comienzo')
df_musica=df_musica.merge(df_fin[['razon_fin','idnt_fin']],how='left',on='razon_fin')
df_musica=df_musica[['fecha','milisegundos','idnt_pista','idnt_comienzo','idnt_fin','aleatorio','saltado']]
df_musica.insert(0, 'idnt_music', df_musica.index + 1)

df_podcast=df_podcast[['Nombre_show_episodio']].drop_duplicates().reset_index(drop=True)
df_podcast.insert(0,"idnt_podcast",df_podcast.index+1)

df_episodio=df_podcast[['Nombre_episodio','Nombre_show_episodio']].drop_duplicates().reset_index(drop=True)
df_episodio=df_episodio.merge(df_show[['Nombre_show_episodio','idnt_show']],how='left',on='Nombre_show_episodio')
df_episodio=df_episodio[['Nombre_episodio','idnt_show']]
df_episodio.insert(0, 'idnt_pista', df_episodio.index + 1)

df_podcasts=df_podcast[['fecha','milisegundos','Nombre_episodio','razon_comienzo','razon_fin','aleatorio','saltado']]S
df_podcasts=df_podcasts.merge(df_episodio[['Nombre_episodio','idnt_episodio']],how='left',on='Nombre_episodio')
df_podcasts=df_podcasts.merge(df_comienzo[['razon_comienzo','idnt_comienzo']],how='left',on='razon_comienzo')
df_podcasts=df_podcasts.merge(df_fin[['razon_fin','idnt_fin']],how='left',on='razon_fin')
df_podcasts=df_podcasts[['fecha','milisegundos','idnt_episodio','idnt_comienzo','idnt_fin','aleatorio','saltado']]

#Guardamos las información obtenida en csv en la siguiente ruta
ruta_fin=path_base+'DATA/REPORTING/'

#Creamos una lista con todas las tablas que queremos descargar
tablas = [
    ("RazonComienzo", df_comienzo),
    ("RazonFin", df_fin),
    ("Artistas", df_artistas),
    ("Albumes", df_album),
    ("Canciones", df_cancion),
    ("Musica", df_musica),
    ("Shows", df_show),
    ("Episodios", df_episodio),
    ("Podcasts", df_podcasts)
]

guardar_tabla(tablas, ruta_fin)