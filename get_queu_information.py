import requests
import csv
import json
from datetime import datetime
import os
import logging

# Configuración del logging
log_file = "queu_information_parks.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

OUTPUT_JSON = "url_routes.json"
output_directory = '/home/lucia/Documentos/themeparks/'

def get_queu_information(api_url, output_file):
    """
    Obtiene la información de la API y guarda los datos en un archivo CSV.

    Args:
        api_url (str): URL de la API que proporciona la información de los parques.
        output_file (str): Ruta donde se guardará el archivo CSV con la información.
    """
    try:
        logging.info(f"Solicitando datos de la API: {api_url}")
        response = requests.get(api_url)
        response.raise_for_status()  # Verifica si la solicitud fue exitosa
        data = response.json()  # Convierte la respuesta a formato JSON

        with open(output_file, 'a', newline='') as csvfile:
            fieldnames = ['land_id', 'land_name', 'ride_id', 'ride_name', 'is_open', 'wait_time', 'last_update']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Escribe la cabecera solo si el archivo está vacío
            if csvfile.tell() == 0:
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

                    writer.writerow({'land_id': land_id, 'land_name': land_name, 'ride_id': ride_id,
                                     'ride_name': ride_name, 'is_open': is_open, 'wait_time': wait_time,
                                     'last_update': last_update})

        logging.info(f"Datos guardados correctamente en el archivo: {output_file}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Error al obtener datos de la API: {e}")

with open(OUTPUT_JSON, mode='r', encoding='utf-8') as json_file:
    try:
        urls_file_name = json.load(json_file)
        logging.info(f"Archivo JSON '{OUTPUT_JSON}' cargado exitosamente.")
    except json.JSONDecodeError as e:
        logging.error(f"Error al leer el archivo JSON: {e}")
        raise
    except FileNotFoundError as e:
        logging.error(f"Archivo JSON no encontrado: {e}")
        raise
    except Exception as e:
        logging.error(f"Error inesperado al leer el archivo JSON: {e}")
        raise

for url_api, file_name in urls_file_name.items():
    # Crear el directorio correspondiente al nombre del parque si no existe
    directory_path = os.path.join(output_directory, file_name)
    os.makedirs(directory_path, exist_ok=True)
    logging.info(f"Directorio creado o ya existente: {directory_path}")

    # Crear el nombre base para el archivo de salida
    base_filename = f'queutimes_{file_name}_'
    current_time = datetime.now()
    formatted_date = current_time.strftime('%Y%m%d')
    output_file = os.path.join(directory_path, f"{base_filename}{formatted_date}.csv")

    logging.info(f"Creando archivo de salida: {output_file}")

    get_queu_information(url_api, output_file)

logging.info("Proceso completado.")
