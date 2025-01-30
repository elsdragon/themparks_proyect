'''  
Instalar Google Cloud SDK
Comprobar la instalacion:
gcloud version

Inicia sesión:
gcloud auth login 

Verifica autentificación:
gcloud auth application-default login

Configura el proyecto:
gcloud config list

Asegúrate de que el proyecto correcto está configurado en core/project.
Si el proyecto no es el correcto, selecciona el correcto con:

gcloud config set project themparks

Probar el acceso:
gsutil ls gs://datos_parques

EJECUTAR EL SRIPT:

Descarga los datos del bucket por defecto en local en carpeta por defecto.

'''

from google.cloud import storage
import os

# Configura el nombre de tu bucket y la carpeta destino
BUCKET_NAME = "datos_parques"
DEST_FOLDER = "/home/lucia/Documentos/themeparks/data_themeparks"

# Autenticación (Asegúrate de haber configurado tu cuenta con `gcloud auth application-default login`)
def authenticate():
    return storage.Client()

# Función para descargar todos los archivos
def descargar_archivos(bucket_name, destination_folder):
    client = authenticate()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()

    # Crear la carpeta si no existe
    os.makedirs(destination_folder, exist_ok=True)

    for blob in blobs:
        file_path = os.path.join(destination_folder, blob.name)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        blob.download_to_filename(file_path)
        print(f"Descargado: {file_path}")

if __name__ == "__main__":
    descargar_archivos(BUCKET_NAME, DEST_FOLDER)
