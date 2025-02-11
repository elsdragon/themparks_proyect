# Databricks notebook source
# MAGIC %md
# MAGIC # ANALISIS CON PYSPARK DATABRICKS (FECHA 11 DE FEBRERO DE 2025)

# COMMAND ----------

# MAGIC %md
# MAGIC ### IMPORTAR LIBRERIAS

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, min, max, collect_list, avg, 
    row_number, desc
)

from pyspark.sql.types import NumericType
from pyspark.sql import Window
 
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

import os
from datetime import datetime


# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear carpeta

# COMMAND ----------

from datetime import datetime

# Obtener la fecha actual en formato YYYY-MM-DD
fecha_actual = datetime.today().strftime('%Y-%m-%d')

# Ruta para crear la carpeta en DBFS
directorio_actual = "/dbfs/FileStore/results"
nombre_carpeta = f"{directorio_actual}/{fecha_actual}"

# Crear la carpeta si no existe
dbutils.fs.mkdirs(nombre_carpeta)

# Verificar que la carpeta se ha creado
print(f"Carpeta creada en DBFS: {nombre_carpeta}")



# COMMAND ----------

# MAGIC %md
# MAGIC ### INICIO SESIÓN CON SPARK

# COMMAND ----------

# Crear sesión de Spark
spark = SparkSession.builder.appName("EDA_ThemeParks").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ### CARGA DEL ARCHIVO
# MAGIC

# COMMAND ----------


INPUT_PATH = "dbfs:/FileStore/thempark_key/themparks_database.csv"
df_spark = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Visualización del esquema de datos

# COMMAND ----------

df_spark.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Visualización de registros

# COMMAND ----------

df_spark.show()

# COMMAND ----------

# Obtener el número total de registros en el DataFrame
num_registros = df_spark.count()

# Imprimir el número de registros
print(f"Número total de registros: {num_registros}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## FECHAS DE LOS DATOS

# COMMAND ----------

# Obtener la fecha mínima y máxima de la columna 'local_date'
fechas = df_spark.agg(
    min("local_date").alias("fecha_minima_local"),
    max("local_date").alias("fecha_maxima_local"),
    min("last_update").alias("fecha_mimina_actualización"),
    max("last_update").alias("fecha_máxima_actualización")
).collect()[0]

# Imprimir las fechas mínima y máxima
fecha_minima_local = fechas["fecha_minima_local"]
fecha_maxima_local = fechas["fecha_maxima_local"]
fecha_minima_act = fechas["fecha_mimina_actualización"]
fecha_maxima_act = fechas["fecha_máxima_actualización"]

print(f"Fecha mínima local: {fecha_minima_local}")
print(f"Fecha máxima local: {fecha_maxima_local}")
print(f"Fecha minima de actualización: {fecha_minima_act}")
print(f"Fecha máxima de actualización: {fecha_maxima_act}")


# COMMAND ----------

# MAGIC %md
# MAGIC LAS FECHAS SON ENTRE EL 21 DE ENERO DEL 2025 Y EL 11 DE FEBRERO DE 2025

# COMMAND ----------

# MAGIC %md
# MAGIC El primer y ultimo dia no tienen registros completos vamos a eliminarlos.

# COMMAND ----------

# Filtrar los registros eliminando los que tengan la fecha de hoy y el primer día que no están completos
df_filtered = df_spark.filter(
    (col("local_date") != fecha_maxima_local) & (col("local_date") != fecha_minima_local)
)


df_filtered.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Comprobamos el número de registros

# COMMAND ----------

num_registros_filter = df_filtered.count()

porcentaje_delete_register = ((num_registros-num_registros_filter)/num_registros)*100

# Imprimir el número de registros
print(f"Número total de registros actuales: {num_registros_filter}")
print(f'Porcentaje de registros eliminados: {porcentaje_delete_register:.2f}%')

# COMMAND ----------

# MAGIC %md
# MAGIC Datos estadísticos

# COMMAND ----------

df_filtered.describe().show()

# COMMAND ----------

# Filtrar solo las columnas numéricas
numeric_columns = [c[0] for c in df_filtered.dtypes if isinstance(df_filtered.schema[c[0]].dataType, NumericType)]

# Aplicar describe solo a las columnas numéricas
df_filtered.select(numeric_columns).describe().show()


# COMMAND ----------

# MAGIC %md
# MAGIC 1. Cobertura temporal amplia:  Se registraron datos desde la medianoche hasta las 23:58.
# MAGIC 2. Distribución de datos uniforme: Hay datos de toda la semana.
# MAGIC 3. Variabilidad en tiempos de espera: Desde 0 minutos (atracciones sin fila o cerradas) hasta 215 minutos (atracciones con altísima demanda).
# MAGIC 4. Rangos amplios de registros: La desviación estándar indica que las colas y horarios de registro varían bastante.
# MAGIC 5. No hay valores nulos.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Numero de días:

# COMMAND ----------

# Contar los valores distintos de 'local_date' para saber cuántos días de datos hay
num_distinct_days = df_filtered.select("local_date").distinct().count()

# Mostrar el resultado
print(f"Número de días para el estudio: {num_distinct_days}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Parques incluidos:

# COMMAND ----------

print(f'Parques incluidos en el estudio: ')
df_filtered.select('park_name').distinct().show(truncate = False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estudio de atracciones cerradas o abiertas.

# COMMAND ----------

print(f'Número de registros de atracciones abiertas y cerradas:')
df_filtered.groupBy("is_open").count().show()


# COMMAND ----------

# MAGIC %md
# MAGIC Hay muchos registros con valores de apertura falsos que pueden deberse a que el parque estaba cerrado en ese momento o atracciones en rehabilitación. Por ello vamos a filtrar los resgistros por las atracciones que estaban abiertas para nuestro estudio.

# COMMAND ----------

# MAGIC %md
# MAGIC # Separación de atracciones abiertas y cerradas

# COMMAND ----------

# Filtrar atracciones cerradas
df_closed = df_filtered.filter((col("is_open") == False) & (col("wait_time") == 0))

# Filtrar el resto de atracciones
df_open = df_filtered.filter(~((col("is_open") == False) & (col("wait_time") == 0)))

# Mostrar resultados
print("Atracciones cerradas:")
df_closed.show()



# COMMAND ----------


print("Atracciones abiertas o con espera:")
df_open.show()

# COMMAND ----------

# Guardar el DataFrame 'df_closed' como un archivo CSV
df_closed.write.csv(f"{nombre_carpeta}/closed_rides_{fecha_actual}.csv", header=True)

# Guardar el DataFrame 'df_open' como un archivo CSV
df_open.write.csv(f"{nombre_carpeta}/opened_rides_{fecha_actual}.csv", header=True)



# COMMAND ----------

# MAGIC %md
# MAGIC Analizar las atracciones que estuvieron cerradas todo un día:

# COMMAND ----------

# Contar los registros cerrados por ride_name, local_date y park_name
df_closed_count = df_closed.groupBy("ride_name", "local_date", "park_name").agg(
    count("ride_name").alias("closed_count")
)

# Contar los registros totales por ride_name y local_date
df_total_count = df_filtered.groupBy("ride_name", "local_date", "park_name").agg(
    count("ride_name").alias("total_count")
)

# Unir los dos DataFrames para comparar el número de registros cerrados con el total
df_result = df_closed_count.join(df_total_count, on=["ride_name", "local_date", "park_name"])

# Filtrar donde todos los registros para esa atracción y día están cerrados
df_only_closed_all_day = df_result.filter(col("closed_count") == col("total_count"))

# Mostrar resultados: atracción, parque y fecha
df_only_closed_all_day.select("ride_name", "park_name", "local_date").show(truncate=False)


# COMMAND ----------

df_only_closed_all_day.write.csv(f"{nombre_carpeta}/closed_rides_all_day_{fecha_actual}.csv", header=True)


# COMMAND ----------

# MAGIC %md
# MAGIC Eliminamos estos registros:

# COMMAND ----------

# Obtener los registros de atracciones cerradas todo un día (ya calculado antes)
df_only_closed_all_day = df_result.filter(col("closed_count") == col("total_count")).select("ride_name", "local_date", "park_name")

# Realizar un left anti join para eliminar estos registros de df_filtered
df_filtered_clean = df_filtered.join(
    df_only_closed_all_day, 
    on=["ride_name", "local_date", "park_name"], 
    how="left_anti"
)

# Verificar que se eliminaron correctamente
df_filtered_clean.show(truncate=False)


# COMMAND ----------

df_only_closed = df_closed.join(df_open, on=["ride_name"], how="left_anti")

print("Atracciones cerradas en todos los días:")
df_only_closed.select("ride_name", "park_name").distinct().show(truncate=False)


# COMMAND ----------

df_only_closed_distinct = df_only_closed.select("ride_name", "park_name").distinct()

df_grouped_by_park = df_only_closed_distinct.groupBy("park_name").agg(collect_list("ride_name").alias("closed_rides"))

df_grouped_by_park.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC Eliminamos esos registros:

# COMMAND ----------

# Hacer un left anti join para eliminar estas atracciones de df_filtered_clean
df_filtered_final = df_filtered_clean.join(
    df_only_closed.select("ride_name", "park_name").distinct(), 
    on=["ride_name", "park_name"], 
    how="left_anti"
)

# Verificar que se eliminaron correctamente
df_filtered_final.show(truncate=False)


# COMMAND ----------

df_filtered_final.write.csv(f"{nombre_carpeta}/clean_dataset_{fecha_actual}.csv", header=True)

# COMMAND ----------

df_filtered_final.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC Comprobar si hay registros de horarios cerrados del parque.
# MAGIC

# COMMAND ----------

# Contar total de atracciones por parque, fecha y hora
df_total_attractions = df_filtered_final.groupBy("park_name", "local_date", "local_hour").agg(
    count("ride_name").alias("total_rides")
)

# Contar atracciones cerradas por parque, fecha y hora
df_closed_attractions = df_filtered_final.filter((col("is_open") == False) & (col("wait_time") == 0)) \
    .groupBy("park_name", "local_date", "local_hour").agg(
    count("ride_name").alias("closed_rides")
)

# Unir ambos DataFrames
df_park_closure = df_total_attractions.join(df_closed_attractions, 
                                            on=["park_name", "local_date", "local_hour"], 
                                            how="left")

df_park_closure = df_park_closure.fillna({"closed_rides": 0})

df_park_closure.show()

# Identificar las horas donde todas las atracciones están cerradas
df_park_closure = df_park_closure.withColumn("is_park_closed", col("closed_rides") == col("total_rides"))

# Filtrar solo las horas donde el parque estuvo completamente cerrado
df_park_closed_hours = df_park_closure.filter(col("is_park_closed") == True).select("park_name", "local_date", "local_hour")

# Mostrar las horas de cierre por parque y fecha
df_park_closed_hours.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC No hay registros en horas de cierre del parque.

# COMMAND ----------

# MAGIC %md
# MAGIC # Tiempos de espera (Atracciones abiertas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Por parques

# COMMAND ----------


# Calcular promedio, máximo y mínimo de wait_time por parque
df_spark_stats_open = df_filtered_final.groupBy("park_name").agg(
    avg("wait_time").alias("avg_wait_time"),
    max("wait_time").alias("max_wait_time")
)

df_spark_stats_open.show(truncate=False)



# COMMAND ----------


# Parque con menor tiempo medio de espera
park_lowest_avg = df_spark_stats_open.orderBy(col("avg_wait_time").asc()).limit(1)
# Parque con mayor tiempo medio de espera
park_highest_avg = df_spark_stats_open.orderBy(col("avg_wait_time").desc()).limit(1)

# Parque con mayor tiempo de espera registrado
park_max_wait = df_spark_stats_open.orderBy(col("max_wait_time").desc()).limit(1)

# Mostrar resultados
print("Parque con menor tiempo medio de espera:")
park_lowest_avg.show(truncate=False)

print("Parque con mayor tiempo medio de espera:")
park_highest_avg.show(truncate=False)


print("Parque con mayor tiempo de espera registrado:")
park_max_wait.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC Dia de la semana con mayor tiempo medio de espera por parque:

# COMMAND ----------

df_spark_stats_days_avg = df_filtered_final.groupBy("park_name", "local_weekday").agg(
    avg("wait_time").alias("avg_wait_time"),
    max("wait_time").alias("max_wait_time")
)

df_max_avg_wait_per_day = df_spark_stats_days_avg.withColumn(
    "max_avg_wait_day", 
    row_number().over(Window.partitionBy("park_name").orderBy(col("avg_wait_time").desc()))
).filter(col("max_avg_wait_day") == 1).drop("max_avg_wait_day")

df_max_wait_per_day = df_spark_stats_days_avg.withColumn(
    "max_wait_day", 
    row_number().over(Window.partitionBy("park_name").orderBy(col("max_wait_time").desc()))
).filter(col("max_wait_day") == 1).drop("max_wait_day")

print("Dia de la semana con más media de tiempos de espera por parque:")
df_max_avg_wait_per_day.orderBy('avg_wait_time', ascending= False).show(truncate=False)


# COMMAND ----------

print("Dia de la semana con el tiempo de espera máximo por parque:")
df_max_wait_per_day.orderBy('max_wait_time', ascending=False).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # POR ATRACCIONES

# COMMAND ----------


df_spark_stats_rides = df_filtered_final.groupBy("park_name","ride_name").agg(
    avg("wait_time").alias("avg_wait_time"),
    max("wait_time").alias("max_wait_time"),
)

df_spark_stats_rides.orderBy("avg_wait_time", ascending=False).show(truncate=False)

# COMMAND ----------

df_spark_stats_rides.orderBy("max_wait_time", ascending=False).show(truncate=False)

# COMMAND ----------

# Atracción con mayor tiempo medio de espera
ride_highest_avg = df_spark_stats_rides.orderBy(col("avg_wait_time").desc()).limit(1)

# Atracción con mayor tiempo de espera registrado
ride_max_wait = df_spark_stats_rides.orderBy(col("max_wait_time").desc()).limit(1)

print("Atracción con mayor tiempo medio de espera:")
ride_highest_avg.show(truncate=False)

print("Atracción con mayor tiempo de espera registrado:")
ride_max_wait.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC Comprobamos que día de la semana tienen la media más alta de tiempos de espera:

# COMMAND ----------

df_spark_stats_days_avg = df_filtered_final.groupBy("park_name", "ride_name", "local_weekday").agg(
    avg("wait_time").alias("avg_wait_time")
)

# Para cada parque y atracción, encontrar el día de la semana con la mayor media de espera
df_max_avg_wait_per_day = df_spark_stats_days_avg.withColumn(
    "max_avg_wait_day", 
    row_number().over(Window.partitionBy("park_name", "ride_name").orderBy(col("avg_wait_time").desc()))
).filter(col("max_avg_wait_day") == 1).drop("max_avg_wait_day")

# Mostrar resultados
df_max_avg_wait_per_day.orderBy("avg_wait_time", ascending=False).show(truncate=False)


# COMMAND ----------

df_max_avg_wait_per_day_pd = df_max_avg_wait_per_day.toPandas()

df_avg_wait_per_day = df_max_avg_wait_per_day_pd.groupby(["park_name", "local_weekday"])["avg_wait_time"].mean().reset_index()

weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
df_avg_wait_per_day['local_weekday'] = pd.Categorical(df_avg_wait_per_day['local_weekday'], categories=weekday_order, ordered=True)
df_avg_wait_per_day = df_avg_wait_per_day.sort_values(by=["local_weekday", "park_name"])


plt.figure(figsize=(14, 7))
sns.barplot(x="local_weekday", y="avg_wait_time", hue="park_name", data=df_avg_wait_per_day)


plt.title("Tiempos Medios de Espera por Día de la Semana y Parque")
plt.xlabel("Día de la Semana")
plt.ylabel("Tiempo Medio de Espera (minutos)")

plt.tight_layout()

plt.show()


# COMMAND ----------

df_spark_stats_days_max = df_filtered_final.groupBy("park_name", "ride_name", "local_weekday").agg(
    max("wait_time").alias("max_wait_time")
)
df_max_wait_per_day = df_spark_stats_days_max.withColumn(
    "max_wait_day", 
    row_number().over(Window.partitionBy("park_name", "ride_name").orderBy(col("max_wait_time").desc()))
).filter(col("max_wait_day") == 1).drop("max_wait_day")

df_max_wait_per_day.orderBy("max_wait_time", ascending=False).show(truncate=False)

# COMMAND ----------

df_max_wait_per_day_pd = df_max_wait_per_day.toPandas()

df_max_wait_per_day = df_max_wait_per_day_pd.groupby(["park_name", "local_weekday"])["max_wait_time"].mean().reset_index()

weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
df_max_wait_per_day['local_weekday'] = pd.Categorical(df_max_wait_per_day['local_weekday'], categories=weekday_order, ordered=True)
df_max_wait_per_day = df_max_wait_per_day.sort_values(by=["local_weekday", "park_name"])


plt.figure(figsize=(14, 7))
sns.barplot(x="local_weekday", y="max_wait_time", hue="park_name", data=df_max_wait_per_day)


plt.title("Tiempos maximos de Espera por Día de la Semana y Parque")
plt.xlabel("Día de la Semana")
plt.ylabel("Tiempo Maximos de Espera (minutos)")

plt.tight_layout()

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # DIAS CON MAS TIEMPO DE ESPERA

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tiempo medio de espera.

# COMMAND ----------

df_avg_wait_per_day = df_filtered_final.groupBy("park_name", "local_date").agg(
    avg("wait_time").alias("avg_wait_time")
)

window_spec = Window.partitionBy("park_name").orderBy(col("avg_wait_time").desc())

df_max_avg_wait_day = df_avg_wait_per_day.withColumn(
    "rank", row_number().over(window_spec)
).filter(col("rank") == 1).drop("rank")

df_max_avg_wait_day.orderBy('local_date').show(truncate=False)


# COMMAND ----------

df_avg_wait_per_day_pd = df_avg_wait_per_day.toPandas()
# Asegurarnos de que 'local_date' esté en formato datetime
df_avg_wait_per_day_pd["local_date"] = pd.to_datetime(df_avg_wait_per_day_pd["local_date"])

# Ordenar los datos por parque y fecha
df_avg_wait_per_day_pd = df_avg_wait_per_day_pd.sort_values(by=["park_name", "local_date"])

# Crear una figura con subgráficos para cada parque
unique_parks = df_avg_wait_per_day_pd["park_name"].unique()
fig, axes = plt.subplots(len(unique_parks), 1, figsize=(12, len(unique_parks)*5), sharex=True)

# Asegurarnos de que 'axes' siempre sea una lista (en caso de un solo parque)
if len(unique_parks) == 1:
    axes = [axes]

# Iterar sobre los parques y graficar la evolución de los tiempos medios de espera
for i, park in enumerate(unique_parks):
    df_park = df_avg_wait_per_day_pd[df_avg_wait_per_day_pd["park_name"] == park]
    axes[i].plot(df_park["local_date"], df_park["avg_wait_time"], label=park)
    axes[i].set_title(f"Evolución de los Tiempos Medios de Espera - {park}")
    axes[i].set_xlabel("Fecha")
    axes[i].set_ylabel("Tiempo Medio de Espera (minutos)")
    axes[i].legend()
    axes[i].tick_params(axis="x", rotation=45)

    # Asegurarse de que las fechas sean legibles en el eje X
    axes[i].xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%Y-%m-%d'))

# Ajustar la presentación del gráfico
plt.tight_layout()

# Mostrar el gráfico
plt.show()


# COMMAND ----------

df_avg_wait_per_day_pd["local_date"] = pd.to_datetime(df_avg_wait_per_day_pd["local_date"])

# Ordenar los datos por fecha
df_avg_wait_per_day_pd = df_avg_wait_per_day_pd.sort_values(by=["local_date"])

# Crear una figura para el gráfico
plt.figure(figsize=(12, 6))

# Iterar sobre los parques y graficar la evolución de los tiempos medios de espera en el mismo gráfico
for park in df_avg_wait_per_day_pd["park_name"].unique():
    df_park = df_avg_wait_per_day_pd[df_avg_wait_per_day_pd["park_name"] == park]
    plt.plot(df_park["local_date"], df_park["avg_wait_time"], label=park)

# Títulos y etiquetas
plt.title("Evolución de los Tiempos Medios de Espera por Parque")
plt.xlabel("Fecha")
plt.ylabel("Tiempo Medio de Espera (minutos)")
plt.xticks(rotation=45)

# Añadir leyenda
plt.legend(title="Parques")

# Ajustar la presentación del gráfico
plt.tight_layout()

# Mostrar el gráfico
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC # POR HORAS

# COMMAND ----------


df_avg_wait_by_hour = df_filtered_final.groupBy("park_name", "local_hour").agg(
    avg("wait_time").alias("avg_wait_time")
)

window_spec = Window.partitionBy("park_name").orderBy(desc("avg_wait_time"))

df_avg_wait_by_hour = df_avg_wait_by_hour.withColumn(
    "rank", row_number().over(window_spec)
)

df_avg_wait_by_hour_result = df_avg_wait_by_hour.filter(col("rank") == 1).drop("rank")

df_avg_wait_by_hour_result.orderBy("avg_wait_time", ascending=False).show(truncate=False)


# COMMAND ----------

df_max_wait_by_hour = df_filtered_final.groupBy("park_name", "local_hour").agg(
    max("wait_time").alias("max_wait_time")
)

window_spec = Window.partitionBy("park_name").orderBy(desc("max_wait_time"))

df_max_wait_by_hour = df_max_wait_by_hour.withColumn(
    "rank", row_number().over(window_spec)
)

df_max_wait_by_hour_result = df_max_wait_by_hour.filter(col("rank") == 1).drop("rank")

df_max_wait_by_hour_result.orderBy("max_wait_time", ascending=False).show(truncate=False)


# COMMAND ----------


df_avg_wait_by_ride_hour = df_filtered_final.groupBy( "park_name", "ride_name", "local_hour").agg(
    avg("wait_time").alias("avg_wait_time")
)

window_spec = Window.partitionBy("ride_name").orderBy(desc("avg_wait_time"))

df_avg_wait_by_ride_hour = df_avg_wait_by_ride_hour.withColumn(
    "rank", row_number().over(window_spec)
)

df_avg_wait_by_ride_hour_result = df_avg_wait_by_ride_hour.filter(col("rank") == 1).drop("rank")

df_avg_wait_by_ride_hour_result.orderBy("avg_wait_time", ascending=False).show(truncate=False)


# COMMAND ----------


df_max_wait_by_ride_hour = df_filtered_final.groupBy("park_name", "ride_name", "local_hour").agg(
    max("wait_time").alias("max_wait_time")
)

# Crear una ventana para obtener la hora con el mayor tiempo de espera por atracción
window_spec = Window.partitionBy("ride_name").orderBy(desc("max_wait_time"))

# Añadir una columna con el ranking para obtener la hora con mayor tiempo de espera
df_max_wait_by_ride_hour = df_max_wait_by_ride_hour.withColumn(
    "rank", row_number().over(window_spec)
)

# Filtrar los registros con el rango 1 (la hora con mayor tiempo de espera para cada atracción)
df_max_wait_by_ride_hour_result = df_max_wait_by_ride_hour.filter(col("rank") == 1).drop("rank")

# Ordenar los resultados por max_wait_time de mayor a menor
df_max_wait_by_ride_hour_result.orderBy("max_wait_time", ascending = False).show(truncate=False)

