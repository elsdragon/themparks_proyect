# themparks_proyect
Proyecto parques temáticos

OBJETIVO: Implementar en la nube la consulta a una api 'queu_times' cada hora donde se guarden los datos de colas de las atracciones de los parque.

HERRAMIENTAS: Se ha usado un IDE en local Visual Studio Code con Python y jupyter notebooks. Librerias como pandas, os, datetime, time, json, loggings, csv, requests, numpy, matplotlib.pyplot, sklearn, data_profiling

STEP 1: Se ha implementado un script en local que guarda en un csv los datos de los parques como ubicación, nombre, país, continente y la id_park que nos permita hacer la consulta posterior. 'park_information.csv'

STEP 2: Se ha creado un diccionario que guarda como clave la id y como valor el nombre del parque. Para ello se ha creado un script que genere este diccionario. 'parks_id_name.json'

Durante algún tiempo se han hecho pruebas en local de la ETL con el parque Disneyland Studios Paris (DLP_pruebas)

STEP 3: Se ha generado un diccionario donde la clave es la url de la api ya formateada y el valor el nombre del archivo ya formateado con el que se quiere guardar la consulta.

STEP 4: Se ha creado un script de python que accediendo al diccionario del punto 3 nos permite hacer una consulta por cada valor guardando el archivo cada una en su carpeta por días. ('get_queu_information.py)

# DESPLIEGUE EN GCP

Se han desplegado varias utilidades en GCP:

1. Se ha programado una Google Function la función que solicita cada hora información a la api queu_times con datos de tiempos de espera de atracciones de parques Disney. Dicha función se ejecuta cada hora gracias a Cloud Scheduler. Se guarda un archivo por parque y dia que va guardando la información. 

2. Se ha programado un ETL con Apache Airflow desplegado con Google Composer para limpiar duplicados y unir los archivos generados de forma diaria en el paso previo. (SE TIENE QUE TERMINAR ESTE APARTADO CON NUEVAS FUNCIONALIDADES)
