from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage
import pandas as pd
from datetime import datetime
import os

# Buckets y archivos en GCP
INPUT_BUCKET = 'datos_parques'
OUTPUT_BUCKET = 'datos_consolidados_themparks'
OUTPUT_FILE = 'themparks_database.csv'
TEMP_DIR = "/tmp"

# Zonas horarias por parque
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
    """Descarga archivos CSV desde Cloud Storage y los almacena en /tmp."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(INPUT_BUCKET)
    blobs = bucket.list_blobs(prefix='')

    local_files = []
    for blob in blobs:
        if blob.name.endswith('.csv'):
            local_path = os.path.join(TEMP_DIR, os.path.basename(blob.name))
            blob.download_to_filename(local_path)
            local_files.append(local_path)

    return local_files

def transform_data(ti):
    """Transforma los archivos CSV descargados y los consolida en un DataFrame."""
    local_files = ti.xcom_pull(task_ids='extract_task')
    all_data = []

    for file_path in local_files:
        try:
            df = pd.read_csv(file_path)

            # Convertir tipos de datos
            df['is_open'] = df['is_open'].replace({'False': False, 'True': True}).astype(bool)
            df['wait_time'] = pd.to_numeric(df['wait_time'], errors='coerce').fillna(0).astype(int)
            df['land_id'] = pd.to_numeric(df['land_id'], errors='coerce').fillna(0).astype(int)
            df['ride_id'] = pd.to_numeric(df['ride_id'], errors='coerce').fillna(0).astype(int)

            all_data.append(df)
        except Exception as e:
            print(f"Error procesando {file_path}: {e}")

    # Leer archivo existente en Cloud Storage (si ya existe)
    storage_client = storage.Client()
    bucket = storage_client.bucket(OUTPUT_BUCKET)
    blob = bucket.blob(f"consolidados/{OUTPUT_FILE}")
    existing_data = []

    if blob.exists():
        existing_path = os.path.join(TEMP_DIR, f"existing_{OUTPUT_FILE}")
        blob.download_to_filename(existing_path)
        existing_data.append(pd.read_csv(existing_path))
        os.remove(existing_path)

    # Unir nuevos y antiguos datos
    all_data.extend(existing_data)
    df = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    df.drop_duplicates(inplace=True)

    # Convertir timestamps y filtrar datos incorrectos
    df['last_update'] = pd.to_datetime(df['last_update'], errors='coerce', utc=True)
    df.dropna(subset=['last_update'], inplace=True)

    # Filtrar parques cerrados
    df = df.groupby(['last_update', 'park_name']).filter(
        lambda x: not all((x['is_open'] == False) & (x['wait_time'] == 0))
    )

    # Eliminar registros donde `land_name` termina en "(Closed)"
    df = df[~df['land_name'].str.endswith('(Closed)', na=False)]

    # Agregar información de zona horaria
    df['timezone'] = df['park_name'].str.lower().map(TIMEZONES)

    # Convertir `last_update` a la hora local del parque
    dfs_processed = []
    for zone in df['timezone'].dropna().unique():
        df_zone = df[df['timezone'] == zone].copy()
        df_zone['local_time'] = df_zone['last_update'].dt.tz_convert(zone)
        df_zone['local_date'] = df_zone['local_time'].dt.date
        df_zone['local_hour'] = df_zone['local_time'].dt.hour
        df_zone['local_minute'] = df_zone['local_time'].dt.minute
        df_zone['local_day_of_week'] = df_zone['local_time'].dt.dayofweek
        df_zone['local_weekday'] = df_zone['local_time'].dt.strftime('%A')
        dfs_processed.append(df_zone)

    # Consolidar DataFrames procesados
    df_final = pd.concat(dfs_processed, ignore_index=True) if dfs_processed else pd.DataFrame()
    df_final.to_csv(os.path.join(TEMP_DIR, OUTPUT_FILE), index=False)

def load_data():
    """Sube el DataFrame transformado a Cloud Storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(OUTPUT_BUCKET)
    blob = bucket.blob(f"consolidados/{OUTPUT_FILE}")

    temp_file_path = os.path.join(TEMP_DIR, OUTPUT_FILE)
    blob.upload_from_filename(temp_file_path)

    # Eliminar archivo temporal después de subirlo
    os.remove(temp_file_path)

def cleanup_files(ti):
    """Elimina archivos locales y originales en Cloud Storage después de la carga."""
    storage_client = storage.Client()

    # Eliminar archivos locales
    local_files = ti.xcom_pull(task_ids='extract_task')
    for file_path in local_files:
        try:
            os.remove(file_path)  # Eliminar archivo local en /tmp/
            print(f"Archivo local eliminado: {file_path}")
        except Exception as e:
            print(f"Error eliminando archivo local {file_path}: {e}")

    # Eliminar los archivos originales del bucket de entrada
    bucket_input = storage_client.bucket(INPUT_BUCKET)
    for file_path in local_files:
        try:
            blob_name = os.path.basename(file_path)
            blob = bucket_input.blob(blob_name)
            blob.delete()  # Elimina el archivo del bucket de entrada
            print(f"Archivo eliminado del bucket de entrada: {blob_name}")
        except Exception as e:
            print(f"Error eliminando {blob_name} del bucket de entrada: {e}")

with DAG(
    'etl_gcs_themeparks',
    default_args={'owner': 'airflow', 'retries': 1},
    description='ETL para consolidar datos de parques temáticos en GCP',
    schedule_interval='0 1 * * *',  # Corre todos los días a la 1 AM UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_files,
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load_data,
    )

    cleanup_task = PythonOperator(
        task_id='cleanup_task',
        python_callable=cleanup_files,
        trigger_rule='all_success',  # Asegura que cleanup_task solo se ejecute si load_task es exitoso
    )

    # Establecer las dependencias de las tareas
    extract_task >> transform_task >> load_task >> cleanup_task
