#!/usr/bin/env python
# coding: utf-8

# In[8]:


import requests
import csv
import time
from datetime import datetime


# In[9]:


def get_queu_information(api_url, output_file):
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Check if the request was successful
        data = response.json()  # Convert the response to JSON format

        with open(output_file, 'a', newline='') as csvfile:
            fieldnames = ['fecha', 'land_id', 'land_name', 'ride_id', 'ride_name', 'is_open', 'wait_time', 'last_update']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            if csvfile.tell() == 0:
                writer.writeheader()

            fecha = datetime.now()
            for land in data.get('lands', []):
                land_id = land.get('id', 'N/A')
                land_name = land.get('name', 'N/A')

                for ride in land.get('rides', []):
                    ride_id = ride.get('id', 'N/A')
                    ride_name = ride.get('name', 'N/A')
                    is_open = ride.get('is_open', 'N/A')
                    wait_time = ride.get('wait_time', 'N/A')
                    last_update = ride.get('last_updated', 'N/A')

                    writer.writerow({'fecha': fecha, 'land_id': land_id, 'land_name': land_name, 'ride_id': ride_id,
                                     'ride_name': ride_name, 'is_open': is_open, 'wait_time': wait_time,
                                     'last_update': last_update})

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from the API: {e}")


# In[10]:


total_hours = 2
interval_minutes = 30

# Directorio y nombre del archivo
output_directory = '/home/lucia/Documentos/themeparks/'
#output_directory = 'gs://databaseparks/DLP'
base_filename = 'queutimes_dlpStudios_'

# Bucle principal
for hour in range(total_hours):
    current_time = datetime.now()
    formatted_date = current_time.strftime('%Y%m%d')
    output_file = f"{output_directory}{base_filename}{formatted_date}.csv"

    # URL de la API
    api_url = "https://queue-times.com/parks/28/queue_times.json"

    # Obtener datos y guardar en el archivo correspondiente
    get_queu_information(api_url, output_file)

        # Esperar el intervalo antes de la siguiente solicitud
    time.sleep(interval_minutes * 60)

print("Bucle completado.")

