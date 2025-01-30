from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage
import pandas as pd
from datetime import datetime
import os

BUCKET_NAME = 'datos_parques'
OUTPUT_BUCKET = 'datos_consolidados_themparks'
OUTPUT_FILE = 'consolidated_queue_times.csv'

def extract_data():
    """Descargar archivos desde Cloud Storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix='')
    local_files = []
    for blob in blobs:
        if blob.name.endswith('.csv'):
            local_path = f"/tmp/{os.path.basename(blob.name)}"
            blob.download_to_filename(local_path)
            local_files.append(local_path)
    return local_files

def transform_data(**context):
    """Combinar archivos CSV, eliminar duplicados y realizar limpieza."""
    local_files = context['task_instance'].xcom_pull(task_ids='extract_task')
    all_data = []
    
    # Cargar y combinar todos los archivos CSV
    for file_path in local_files:
        df = pd.read_csv(file_path)
        
        # Eliminar duplicados en cada archivo antes de combinar
        df = df.drop_duplicates(subset=['land_id', 'ride_id', 'last_update'])
        
        all_data.append(df)
    
    # Combinar todos los DataFrames en uno solo
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # Eliminar duplicados en el DataFrame combinado
    combined_df = combined_df.drop_duplicates(subset=['land_id', 'ride_id', 'last_update'])
    
    # Guardar el archivo consolidado temporalmente
    combined_df.to_csv(f"/tmp/{OUTPUT_FILE}", index=False)

def load_data():
    """Subir los registros consolidados a Cloud Storage, agregando a un archivo existente si es necesario."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(OUTPUT_BUCKET)
    blob = bucket.blob(f"consolidados/{OUTPUT_FILE}")

    # Verificar si el archivo ya existe en Cloud Storage
    if blob.exists():
        # Si el archivo ya existe, descargarlo y añadir los nuevos datos
        local_existing_file = f"/tmp/existing_{OUTPUT_FILE}"
        blob.download_to_filename(local_existing_file)

        # Leer el archivo existente y los nuevos datos
        existing_df = pd.read_csv(local_existing_file)
        new_df = pd.read_csv(f"/tmp/{OUTPUT_FILE}")

        # Concatenar los archivos existentes con los nuevos, y eliminar duplicados
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=['land_id', 'ride_id', 'last_update'])
        
        # Guardar el archivo actualizado
        combined_df.to_csv(f"/tmp/{OUTPUT_FILE}", index=False)

    # Si el archivo no existe, solo subimos el archivo nuevo
    blob.upload_from_filename(f"/tmp/{OUTPUT_FILE}")

with DAG(
    'etl_gcs_append',
    default_args={'owner': 'airflow', 'retries': 1},
    description='ETL con Cloud Storage y Airflow (con append)',
    schedule_interval='0 0 * * 1',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_data,
    )
    
    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
        provide_context=True,
    )
    
    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load_data,
    )

    extract_task >> transform_task >> load_task

