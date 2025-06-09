from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("lab6.py").getOrCreate()
sc = spark.sparkContext

# Crear el esquema que se usara para cargar la base de datos de ratings
ratingsSchema = StructType().add("userID", "string").add("movieID", "string").add("rating", "float").add("timestamp", "string")
df_r = spark.read.option("delimeter", ",").option("header", True).schema(ratingsSchema).csv("/home/ubuntu/ml-25m/ratings.csv")
df_r_avg = df_r.groupBy("movieID").agg(F.avg("rating").alias("avg_rating"))

# Crear el esquema que se usara para cargar la base de datos de movies
moviesSchema = StructType().add("movieID", "string").add("title", "string").add("genres", "string")
df_m = spark.read.option("delimeter", ",").option("header", True).schema(moviesSchema).csv("/home/ubuntu/ml-25m/movies.csv") 
df_m_separated = df_m.withColumn("genres", F.split(F.col("genres"), "\|")).withColumn("genre", F.explode(F.col("genres"))).drop("genres")

# Unir los data frames en uno solo segun el ID de la pelicula
df_final = df_m_separated.join(df_r_avg, on="movieID", how="inner").select("movieID", "genre", "avg_rating")

# Agrupar por 'genre' y calcular el promedio de 'average_rating' y el conteo de películas
df_genre_stats = df_final.groupBy("genre").agg(F.avg("avg_rating").alias("avg_rating_by_genre"),F.count("movieID").alias("total_movies")).orderBy("genre")

# Concatenar las columnas en una sola columna tipo string para cada fila
df_final_txt = df_genre_stats.withColumn("output_string", F.concat_ws(", ", F.col("genre"), F.col("avg_rating_by_genre"), F.col("total_movies"))).select("output_string")

# Guardar el DataFrame como archivo de texto
df_final_txt.write.mode("overwrite").text("/home/ubuntu/3_out.txt")
