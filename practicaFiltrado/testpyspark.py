import pyspark;
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

spark=SparkSession.builder.appName("NetflixDataTest").getOrCreate();

movies_df = spark.read.csv("practicaFiltrado/test_netflix_titles.csv", header=True, inferSchema=True);
movies_df.show();

print(f"{movies_df.dtypes}")

#  Ejercicio 1:
# Filtra todos los registros cuyo rating sea "PG-13".
movies_df.filter(movies_df["rating"] == "PG-13").show();

# Ejercicio 2:
# Filtra todos los títulos (title) añadidos en el año 2021.
movies_df.filter(movies_df["date_added"].contains("2021")).show();

# Ejercicio 3:
# Selecciona los títulos (title) y directores (director) de las películas cuyo tipo (type) sea "Movie" y que tengan una duración superior a 120 minutos.
movies_df.select("title", "director").filter((movies_df["type"] == "Movie") & ((regexp_extract(movies_df["duration"], r'(\d+)', 0)).cast("int") > 120)).show();

# Ejercicio 4:
# Muestra el title y el country de todos los shows (películas o series) que fueron agregados en el año 2020.
movies_df.select("title", "country").filter(movies_df["date_added"].contains("2020")).show();

# Ejercicio 5:
# Muestra el title y el rating de todos los shows cuya clasificación (rating) es "TV-MA" o "R".
movies_df.select("title", "rating").filter((movies_df["rating"]=="TV-MA")|(movies_df["rating"]=="R")).show();

# Ejercicio 6:
# Muestra el title, type, y release_year de todos los shows no clasificados como "Movie".
movies_df.select("title","type", "release_year").filter(~movies_df["type"].isin("Movie")).show();

# Ejercicio 7: 
# Muestra los títulos (title) de todos los shows donde el campo cast contenga el nombre "Smith", sin importar si es mayúscula o minúscula.
movies_df.select("title").filter(movies_df["cast"].rlike("(?i)Smith")).show()


# Ejercicio 8:
# Muestra los países (country) y la cantidad de shows (count) que contienen a "Smith" en el campo cast, sin importar mayúsculas o minúsculas. Agrupa los resultados por country y ordénalos de forma descendente por la cantidad de shows.
movies_df.select("country").filter(movies_df["cast"].rlike("(?i)smith")).groupBy("country").count().show();


spark.stop()