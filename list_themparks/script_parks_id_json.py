import csv
import json
import logging

# Configuración del logging
log_file = "script_parks_id.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Ruta del archivo CSV
PATH_CSV = "/home/lucia/Documentos/themeparks/list_themparks/park_information.csv"
PATH_JSON = "/home/lucia/Documentos/themeparks/list_themparks/parks_id_name.json"

def get_json_id_park(csv_file_path=PATH_CSV, json_file_path=PATH_JSON):
    """
    Convierte un archivo CSV de información de parques en un archivo JSON que mapea ID de parque con nombres.

    Args:
        csv_file_path (str): Ruta del archivo CSV.
        json_file_path (str): Ruta del archivo JSON de salida.
    """
    try:
        # Verificar y leer el archivo CSV
        logging.info(f"Intentando abrir el archivo CSV: {csv_file_path}")
        with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
            try:
                csv_reader = csv.DictReader(csv_file)
                parks_dict = {}
                
                # Procesar cada fila del CSV
                for row in csv_reader:
                    id_park = row.get('id_park', None)
                    park_name = row.get('Park', None)

                    # Validar campos obligatorios
                    if not id_park or not park_name:
                        logging.warning(f"Fila incompleta encontrada: {row}")
                        continue
                    
                    parks_dict[id_park] = park_name

                logging.info(f"Archivo CSV procesado correctamente: {csv_file_path}")

            except csv.Error as e:
                logging.error(f"Error al leer el archivo CSV: {e}")
                return
        
        # Guardar datos como JSON
        logging.info(f"Intentando crear el archivo JSON: {json_file_path}")
        with open(json_file_path, mode='w', encoding='utf-8') as json_file:
            try:
                json.dump(parks_dict, json_file, indent=4, ensure_ascii=False)
                logging.info(f"Archivo JSON creado exitosamente: {json_file_path}")
            except Exception as e:
                logging.error(f"Error al escribir el archivo JSON: {e}")

    except FileNotFoundError:
        logging.error(f"El archivo CSV no se encontró: {csv_file_path}")
    except Exception as e:
        logging.error(f"Error inesperado: {e}")

# Script principal
if __name__ == "__main__":
    get_json_id_park()
