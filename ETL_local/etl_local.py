
import pandas as pd
import glob
import os

# Configuración de rutas y patrones de búsqueda
PATTERN = '*.csv'
INPUT_PATH = "/home/lucia/Documentos/themeparks/data_themeparks"
OUTPUT_FILE = "themparks_database.csv"

# Definición de zonas horarias por parque
TIMEZONES = {
    'shanghaidisneyresort': 'Asia/Shanghai',
    'disneyland': 'America/Los_Angeles',
    'disneycaliforniaadventure': 'America/Los_Angeles',
    'waltdisneystudiosparis': 'Europe/Paris',
    'disneylandparkparis': 'Europe/Paris',
    'epcot': 'America/New_York',
    'animalkingdom': 'America/New_York',
    'disneyhollywoodstudios': 'America/New_York',
    'disneymagickingdom': 'America/New_York'
}

def extract_files():
    """Lee los archivos CSV de la carpeta de entrada y los combina en una lista de DataFrames."""
    file_list = glob.glob(os.path.join(INPUT_PATH, PATTERN))
    dfs = []

    for file in file_list:
        df = pd.read_csv(file)
        
        # Convertir is_open a booleano
        df['is_open'] = df['is_open'].replace({'False': False, 'True': True}).astype(bool)
        
        # Convertir columnas numéricas
        df['wait_time'] = pd.to_numeric(df['wait_time'], errors='coerce').fillna(0).astype(int)
        df['land_id'] = pd.to_numeric(df['land_id'], errors='coerce').fillna(0).astype(int)
        df['ride_id'] = pd.to_numeric(df['ride_id'], errors='coerce').fillna(0).astype(int)
        
        dfs.append(df)

    # Agregar datos previos si existe el archivo OUTPUT_FILE
    if os.path.exists(OUTPUT_FILE):
        thempark_df = pd.read_csv(OUTPUT_FILE)
        dfs.append(thempark_df)

    return dfs

def transform(dfs):
    """Transforma los datos aplicando filtrado, limpieza y ajuste de zonas horarias."""
    df = pd.concat(dfs, ignore_index=True)  # Concatenar todos los DataFrames
    df.drop_duplicates(inplace=True)

    # Convertir last_update a datetime y eliminar valores nulos
    df['last_update'] = pd.to_datetime(df['last_update'], errors='coerce')
    df.dropna(subset=['last_update'], inplace=True)

    # Filtrar parques que no están abiertos y tienen tiempo de espera 0
    df = df.groupby(['last_update', 'park_name']).filter(
        lambda x: not all((x['is_open'] == False) & (x['wait_time'] == 0))
    )

    # Eliminar registros donde land_name termine en "(Closed)"
    df = df[~df['land_name'].str.endswith('(Closed)', na=False)]

    # Agregar columna de zona horaria
    df['timezone'] = df['park_name'].str.lower().map(TIMEZONES)

    # Convertir last_update a UTC
    df['last_update'] = pd.to_datetime(df['last_update'], utc=True)

    # Procesar zonas horarias
    dfs_processed = []
    for zone in df['timezone'].dropna().unique():  # Evitar zonas NaN
        df_zone = df[df['timezone'] == zone].copy()
        df_zone['local_time'] = df_zone['last_update'].dt.tz_convert(zone)

        # Extraer fecha y hora local
        df_zone['local_date'] = df_zone['local_time'].dt.date
        df_zone['local_hour'] = df_zone['local_time'].dt.hour
        df_zone['local_minute'] = df_zone['local_time'].dt.minute
        df_zone['local_day_of_week'] = df_zone['local_time'].dt.dayofweek  
        df_zone['local_weekday'] = df_zone['local_time'].dt.strftime('%A')  

        dfs_processed.append(df_zone)

    # Concatenar DataFrames procesados
    df_final = pd.concat(dfs_processed, ignore_index=True) if dfs_processed else pd.DataFrame()

    return df_final.sort_index()

def load_file(df):
    """Guarda el DataFrame en un archivo CSV."""
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Archivo guardado en {OUTPUT_FILE}")

def main():
    """Ejecuta el ETL completo."""
    print("Extrayendo archivos...")
    dfs = extract_files()
    
    print("Transformando datos...")
    df = transform(dfs)
    
    print("Guardando datos procesados...")
    load_file(df)
    
    print("ETL completado con éxito.")

if __name__ == "__main__":
    main()
