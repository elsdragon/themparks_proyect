import requests
import csv
import logging

# Configuración del logging
log_file = "script_park_list.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Constante para la URL de la API
API_URL = "https://queue-times.com/parks.json"

# Constante para el path de salida CSV
PATH_CSV = '/home/lucia/Documentos/themeparks/list_themparks/park_information.csv'

def get_park_information(path_csv=PATH_CSV, api_url=API_URL):
    try:
        logging.info("Iniciando la solicitud a la API.")
        response = requests.get(api_url)
        response.raise_for_status()  # Check if the request was successful
        # Registrar éxito en la conexión con la API
        logging.info("Conexión exitosa a la API.")
        data = response.json()  # Convert the response to JSON format

        with open(path_csv, 'w', newline='') as csvfile:
            fieldnames = ['Entertainment Company', 'id_park','Park', 'Country', 'Continent', 'Latitude', 'Longitude', 'Timezone']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()

            for entertainment_company in data:
                company_name = entertainment_company.get('name', 'N/A')

                for park in entertainment_company.get('parks', []):
                    id_park = park.get('id', 'N/A')
                    park_name = park.get('name', 'N/A')
                    country = park.get('country', 'N/A')
                    continent = park.get('continent', 'N/A')
                    latitude = park.get('latitude', 'N/A')
                    longitude = park.get('longitude', 'N/A')
                    timezone = park.get('timezone', 'N/A')

                    writer.writerow({'Entertainment Company': company_name,'id_park': id_park, 'Park': park_name, 'Country': country,
                                     'Continent': continent, 'Latitude': latitude, 'Longitude': longitude,
                                     'Timezone': timezone})
            
                    # Registrar éxito en la creación del archivo
        logging.info(f"Archivo CSV creado exitosamente en: {path_csv}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from the API: {e}")
    
    except Exception as e:
        logging.error(f"Error al procesar los datos o escribir el archivo: {e}")

if __name__ == "__main__":
    # Llamar a la función principal
    get_park_information()
