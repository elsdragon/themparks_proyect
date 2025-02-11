from google.cloud import storage
import os
from datetime import datetime

# Configuración
bucket_name = 'datos_consolidados_themparks'  # Nombre del bucket
blob_name = 'consolidados/themparks_database.csv'  # Nombre del archivo en el bucket

# Crear una carpeta con la fecha actual en formato 'YYYY-MM-DD'
current_date = datetime.now().strftime('%Y-%m-%d')
local_dir = os.path.join(os.getcwd(), current_date)

# Crear el directorio si no existe
if not os.path.exists(local_dir):
    os.makedirs(local_dir)

# Ruta local donde se guardará el archivo
local_path = os.path.join(local_dir, 'themparks_database.csv')

# Inicializar el cliente de Google Cloud Storage
storage_client = storage.Client()

# Referencia al bucket
bucket = storage_client.bucket(bucket_name)

# Referencia al archivo (blob) en el bucket
blob = bucket.blob(blob_name)

# Descargar el archivo
blob.download_to_filename(local_path)

print(f"Archivo descargado a: {os.path.abspath(local_path)}")
