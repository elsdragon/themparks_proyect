import pandas as pd
import glob
import os

pattern = 'queutimes_dlpStudios_*.csv'
# Paso 1: Encontrar todos los archivos que coincidan con "queue_time_*.csv"
file_list = glob.glob(pattern)

dfs = []

# Itera sobre cada archivo, carga su contenido en un DataFrame y lo añade a la lista
for file in file_list:
    df = pd.read_csv(file)
    dfs.append(df)


if "dlp_database.csv" in os.listdir():
    dlp_df = pd.read_csv("dlp_database.csv")
    dfs.append(dlp_df)


data_join = pd.concat(dfs, ignore_index=True)

# Guardar el DataFrame final como un nuevo archivo CSV con el nombre "dlp_database.csv"
data_join.to_csv("dlp_database.csv", index=False)

# Eliminar los archivos 
for file in file_list:
    os.remove(file)

print('Procesamiento terminado.')
