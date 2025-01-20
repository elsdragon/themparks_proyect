import json
import logging

# Configuración del logging
log_file = "script_urls.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Ruta del archivo JSON que contiene los parques y las claves
PATH_JSON = "/home/lucia/Documentos/themeparks/list_themparks/parks_id_name.json"

# URL base de la API
URL_BASE = "https://queue-times.com/parks/{}/queue_times.json"
FILE_BASE = "database_{}.json"
OUTPUT_JSON = "url_routes.json"

def create_json_api_urls_file(json_file_path=PATH_JSON, 
                              api_url_base=URL_BASE, 
                              file_base=FILE_BASE, 
                              output_json=OUTPUT_JSON):
    """
    Crea una lista de URLs basadas en las claves de un archivo JSON de parques y guarda el diccionario en un archivo JSON.

    Args:
        json_file_path (str): Ruta del archivo JSON que contiene los parques y las claves.
        api_url_base (str): URL base de la API donde se inserta el ID del parque.
    
    Returns:
        dict: Un diccionario con las URLs como claves y los nombres de los archivos como valores.
    """
    try:
        # Cargar el archivo JSON
        with open(json_file_path, mode='r', encoding='utf-8') as json_file:
            parks_dict = json.load(json_file)
            url_name_dict = {}

            # Crear las URLs con las claves del diccionario
            for park_id, park_name in parks_dict.items():
                api_url = api_url_base.format(park_id)  # Formatear la URL con el ID del parque

                # Limpiar el nombre del parque (quitar espacios y convertir a minúsculas)
                park_name_cleaned = park_name.replace(" ", "").lower()
                file_name = file_base.format(park_name_cleaned)

                url_name_dict[api_url] = file_name

            # Guardar el diccionario como un archivo JSON
            with open(output_json, mode='w', encoding='utf-8') as output_file:
                json.dump(url_name_dict, output_file, indent=4, ensure_ascii=False)
            
            # Log de éxito
            logging.info(f"Archivo JSON 'url_routes.json' creado con éxito.")

    except FileNotFoundError:
        logging.error(f"El archivo JSON no se encontró: {json_file_path}")
    except json.JSONDecodeError as e:
        logging.error(f"Error al leer el archivo JSON: {e}")
    except Exception as e:
        logging.error(f"Error inesperado: {e}")

# Script principal
if __name__ == "__main__":
    create_json_api_urls_file()
