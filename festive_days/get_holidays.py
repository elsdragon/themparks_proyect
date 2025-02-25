import os
import google_auth_oauthlib.flow
from googleapiclient.discovery import build
from datetime import datetime
import json

# Paso 1: Autenticarse con OAuth 2.0
CLIENT_SECRET_FILE = "client_secret.json"  # Asegúrate de que este archivo esté presente
SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']

# Obtener el año en curso
current_year = datetime.now().year
start_date = f"{current_year}-01-01T00:00:00Z"  # Fecha de inicio del año
end_date = f"{current_year}-12-31T23:59:59Z"  # Fecha de fin del año

# Diccionario de calendarios de parques
PARKS_CALENDARS = {
    "Magic Kingdom": "en.usa#holiday@group.v.calendar.google.com",
    "Epcot": "en.usa#holiday@group.v.calendar.google.com",
    "Animal Kingdom": "en.usa#holiday@group.v.calendar.google.com",
    "Hollywood Studios": "en.usa#holiday@group.v.calendar.google.com",
    "Disneyland": "en.usa#holiday@group.v.calendar.google.com",
    "Disney California Adventure": "en.usa#holiday@group.v.calendar.google.com",
    "Disneyland París": "fr.french#holiday@group.v.calendar.google.com",
    "Walt Disney Studios París": "fr.french#holiday@group.v.calendar.google.com",
    "Disneyland Hong Kong": "zh.china#holiday@group.v.calendar.google.com",
    "Shanghai Disney Resort": "zh.china#holiday@group.v.calendar.google.com",
    "Tokyo Disneyland": "en.japanese#holiday@group.v.calendar.google.com",
    "Tokyo DisneySea": "en.japanese#holiday@group.v.calendar.google.com"
}

# Paso 2: Autenticación con Google
flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
    CLIENT_SECRET_FILE, SCOPES)
credentials = flow.run_local_server(port=0)

# Crear un diccionario para almacenar los festivos de todos los parques
all_parks_holidays = {}

# Función para obtener los festivos de cada parque
def get_holidays_for_park(service, calendar_id):
    events_result = service.events().list(
        calendarId=calendar_id,
        singleEvents=True,
        orderBy='startTime',
        timeMin=start_date,
        timeMax=end_date
    ).execute()

    events = events_result.get('items', [])
    holidays = {}

    for event in events:
        event_date = event['start'].get('date')
        if event_date:
            holidays[event_date] = event['summary']

    # Ordenar las fechas de los festivos
    return dict(sorted(holidays.items()))

# Paso 3: Usar la API de Google Calendar para obtener los festivos de cada parque
service = build('calendar', 'v3', credentials=credentials)

# Recorrer cada parque y obtener sus festivos
for park, calendar_id in PARKS_CALENDARS.items():
    print(f"Obteniendo festivos para {park}...")
    holidays = get_holidays_for_park(service, calendar_id)
    
    # Almacenar los festivos en el diccionario general
    all_parks_holidays[park] = holidays

# Paso 4: Guardar los festivos en un archivo JSON
with open('festivos_parques.json', 'w', encoding='utf-8') as f:
    json.dump(all_parks_holidays, f, indent=4, ensure_ascii=False)

print("Festivos de todos los parques guardados en 'festivos_parques.json'")
