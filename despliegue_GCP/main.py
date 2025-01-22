import requests
import csv
import json
from datetime import datetime
from google.cloud import storage
import logging
import os

# Inicializar cliente de Google Cloud Storage
client = storage.Client()

output_bucket_name = 'datos_parques'  # Nombre del bucket para los datos
log_bucket_name = 'logs_themparks'    # Nombre del bucket para los logs
dict_bucket_name = 'dict_themparks'  # Nombre del bucket donde está el JSON

# Configuración del logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def upload_to_gcs(bucket_name, file_path, destination_blob_name):
    """Sube un archivo a un bucket de Google Cloud Storage"""
    try:
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(file_path)
        logging.info(f"Archivo subido a GCS: gs://{bucket_name}/{destination_blob_name}")
    except Exception as e:
        logging.error(f"Error al subir archivo a GCS: {e}", exc_info=True)
        raise


def get_queu_information(request):
    """Google Cloud Function para obtener la información de la cola desde las APIs y almacenarla en GCS"""
    try:
        # Descargar el archivo JSON desde el bucket `dict_themparks`
        dict_bucket = client.get_bucket(dict_bucket_name)
        blob = dict_bucket.blob('url_routes_disney.json')  # Ruta del archivo en el bucket
        json_data = blob.download_as_text()
        urls_file_name = json.loads(json_data)

        logging.info(f"Archivo JSON cargado exitosamente desde el bucket: {dict_bucket_name}")

        # Fecha actual para generar los nombres de archivo y logs
        current_time = datetime.now()
        formatted_date = current_time.strftime('%Y%m%d')

        for url_api, file_name in urls_file_name.items():
            # Obtener el nombre del archivo sin la extensión .json
            base_file_name = os.path.splitext(file_name)[0]
            
            # Extraer el nombre del parque del nombre del archivo (ej. "animalkingdom" de "database_animalkingdom")
            park_name = base_file_name.replace('database_', '')

            # Crear un nombre de archivo para almacenar el CSV
            output_file_name = f"queutimes_{base_file_name}_{formatted_date}.csv"

            # Solicitar los datos de la API
            logging.info(f"Procesando API: {url_api} para el archivo: {output_file_name}")
            response = requests.get(url_api)
            response.raise_for_status()
            data = response.json()

            # Ruta temporal del archivo
            temp_output_file = f"/tmp/{output_file_name}"

            # Leer archivo existente (si existe) y agregarle los nuevos datos
            bucket = client.get_bucket(output_bucket_name)
            blob = bucket.blob(output_file_name)

            # Si el archivo ya existe, lo descargamos para agregarle los nuevos datos
            if blob.exists():
                logging.info(f"Archivo {output_file_name} ya existe en GCS. Agregando nuevos datos.")
                existing_file = blob.download_as_text()

                # Escribir los datos existentes y nuevos en el archivo CSV
                with open(temp_output_file, 'w', newline='') as csvfile:
                    fieldnames = ['land_id', 'land_name', 'ride_id', 'ride_name', 'is_open', 'wait_time', 'last_update', 'park_name']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                    # Escribir las filas existentes
                    csvfile.write(existing_file)

                    # Escribir las filas nuevas, añadiendo el nombre del parque
                    writer.writeheader()
                    for land in data.get('lands', []):
                        land_id = land.get('id', 'N/A')
                        land_name = land.get('name', 'N/A')

                        for ride in land.get('rides', []):
                            ride_id = ride.get('id', 'N/A')
                            ride_name = ride.get('name', 'N/A')
                            is_open = ride.get('is_open', 'N/A')
                            wait_time = ride.get('wait_time', 'N/A')
                            last_update = ride.get('last_updated', 'N/A')

                            # Agregar la columna park_name
                            writer.writerow({'land_id': land_id, 'land_name': land_name, 'ride_id': ride_id,
                                             'ride_name': ride_name, 'is_open': is_open, 'wait_time': wait_time,
                                             'last_update': last_update, 'park_name': park_name})
            else:
                # Si el archivo no existe, simplemente guardamos los datos
                logging.info(f"Archivo {output_file_name} no existe en GCS. Creando uno nuevo.")
                with open(temp_output_file, 'w', newline='') as csvfile:
                    fieldnames = ['land_id', 'land_name', 'ride_id', 'ride_name', 'is_open', 'wait_time', 'last_update', 'park_name']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()

                    for land in data.get('lands', []):
                        land_id = land.get('id', 'N/A')
                        land_name = land.get('name', 'N/A')

                        for ride in land.get('rides', []):
                            ride_id = ride.get('id', 'N/A')
                            ride_name = ride.get('name', 'N/A')
                            is_open = ride.get('is_open', 'N/A')
                            wait_time = ride.get('wait_time', 'N/A')
                            last_update = ride.get('last_updated', 'N/A')

                            # Agregar la columna park_name
                            writer.writerow({'land_id': land_id, 'land_name': land_name, 'ride_id': ride_id,
                                             'ride_name': ride_name, 'is_open': is_open, 'wait_time': wait_time,
                                             'last_update': last_update, 'park_name': park_name})

            # Subir el archivo combinado (nuevo o actualizado) al bucket 'datos_parques'
            upload_to_gcs(output_bucket_name, temp_output_file, output_file_name)
            os.remove(temp_output_file)  # Eliminar archivo temporal

        logging.info("Proceso completado exitosamente.")
        return "Proceso completado exitosamente.", 200

    except requests.exceptions.RequestException as e:
        logging.error(f"Error al obtener datos de la API: {e}")
        return f"Error al obtener datos de la API: {e}", 500
    except Exception as e:
        logging.error(f"Error inesperado: {e}", exc_info=True)
        return f"Error inesperado: {e}", 500
