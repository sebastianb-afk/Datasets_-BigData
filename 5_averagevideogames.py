from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("ev2.py").getOrCreate()
sc = spark.sparkContext

# Lectura del archivo CSV
df = spark.read.option("header", True).option("inferSchema", True).option("quote", "\"").csv("/home/ubuntu/Datasets_-BigData/games2.csv")
df = df.withColumnRenamed("", "ID") # Como la primera columna no tiene nombre, se le asigna uno

# Selección de las columnas necesarias
df_filtered = df.select(F.col("Unnamed: 0").alias("ID"), "Title", "Rating", "Genres", "Reviews")

# Validar que el ID sea un número entero y Rating sea un float
df_valid = df_filtered.filter(F.col("ID").rlike("^[0-9]+$") & F.col("Rating").cast("float").isNotNull())

# Convertir la columna ID a entero y Rating a float
df_valid = df_valid.withColumn("ID", F.col("ID").cast("int")).withColumn("Rating", F.col("Rating").cast("float"))

# Aplicar filtro para eliminar filas donde Reviews esté vacío o sea null
df_valid = df_valid.filter(F.trim(F.col("Reviews")) != "")

# Limpiar las comillas y separar los géneros
df_genres = df_valid.withColumn("Genres", F.split(F.regexp_replace(F.col("Genres"), "[\\[\\]']", ""), "; "))
df_genres.cache()

# Aplicar explode a la columna Genres para separar cada género en una fila independiente
df_genres = df_genres.withColumn("Genre", F.explode(F.col("Genres"))).drop("Genres")

# Calcular el número de caracteres en la columna Reviews
df_genres = df_genres.withColumn("ReviewLength", F.length(F.regexp_replace(F.col("Reviews"), "[\\[\\]]", "")))

# Calcular el promedio de Rating y ReviewLength por género
df_genre_stats = df_genres.groupBy("Genre").agg(F.avg("Rating").alias("AvgRating"), F.avg("ReviewLength").alias("AvgReviewLength"))

# Mostrar el DataFrame resultante
df_genre_stats.write.mode("overwrite").option("header", True).csv("/home/ubuntu/5_out.csv")
