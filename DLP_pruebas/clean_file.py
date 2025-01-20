import pandas as pd
from sklearn.preprocessing import LabelEncoder

data = pd.read_csv('dlp_database.csv')
print('El tamaño del archivo es:', data.shape)

# Convierte la columna de fecha en un objeto de fecha
data['fecha'] = pd.to_datetime(data['fecha'])

# Eliminar duplicados
data = data.drop_duplicates()

# Extrae las partes de la fecha
data['year'] = data['fecha'].dt.year
data['month'] = data['fecha'].dt.month
data['day'] = data['fecha'].dt.day
data['hour'] = data['fecha'].dt.hour
data['minute'] = data['fecha'].dt.minute
data['day_week'] = data['fecha'].dt.weekday

#Eliminar columna fecha
data = data.drop('fecha', axis=1)

#Filtrar por el id = 25 que es un land ya cerrado y no aporta información
data = data[data['land_id']!= 25]

# Eliminar las columnas de id de land y ride son datos duplicados 
data = data.drop('land_id', axis=1)
data = data.drop('ride_id', axis=1)

#Tratar la columna de last_update que es tambien una fecha
data['last_update'] = pd.to_datetime(data['last_update'])

data['year_update'] = data['last_update'].dt.year
data['month_update'] = data['last_update'].dt.month
data['day_update'] = data['last_update'].dt.day
data['hour_update'] = data['last_update'].dt.hour
data['minute_update'] = data['last_update'].dt.minute
data['day_week_update'] = data['last_update'].dt.weekday

# Eliminar la columna
data = data.drop('last_update', axis=1)

data = data[data['year']== 2024]
data = data.drop('year', axis=1)
data = data.drop('year_update', axis=1)


print('El tamaño del archivo tratado es:', data.shape)
data.to_csv('dlp_dataclean.csv', index=False)

print('Limpieza terminada.')
