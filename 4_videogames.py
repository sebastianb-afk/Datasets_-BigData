from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("ev2.py").getOrCreate()
sc = spark.sparkContext

# Lectura del archivo CSV
df = spark.read.option("header", True).option("inferSchema", True).option("quote", "\"").csv("/home/ubuntu/Datasets_-BigData/games2.csv")
df = df.withColumnRenamed("", "ID") # Como la primera columna no tiene nombre, se le asigna uno

# Selección de solamente las columnas necesarias y se filtran las filas donde Plays y ID contienen números
df_filtered = df.select(F.col("Unnamed: 0").alias("ID"),"Title", "Team", "Genres", "Plays").filter(F.col("Plays").rlike("^[0-9]+[Kk]?$")).filter(F.col("ID").rlike("^[0-9]+$"))

# Convertir Plays con "K" a miles
df_clean = df_filtered.withColumn("Plays", F.when(F.col("Plays").endswith("K"), F.regexp_replace(F.col("Plays"), "K", "").cast("float") * 1000).otherwise(F.col("Plays").cast("int")))

# Limpiar las comillas y separar los géneros
df_genres = df_clean.withColumn("Genres", F.split(F.regexp_replace(F.col("Genres"), "[\\[\\]']", ""), "; "))

# Aplicar explode a la columna Genres
df_genres = df_genres.withColumn("Genre", F.explode(F.col("Genres"))).drop("Genres")

# Limpiar las comillas y separar los equipos
df_teams = df_clean.withColumn("Team", F.split(F.regexp_replace(F.col("Team"), "[\\[\\]']", ""), "; "))

# Aplicar explode a la columna Team
df_teams = df_teams.withColumn("TeamMember", F.explode(F.col("Team"))).drop("Team")

# Calcular el videojuego más popular por cada género
df_genres_popular = df_genres.groupBy("Genre").agg(F.max(F.struct(F.col("Plays"), F.col("Title"))).alias("max_plays")).select("Genre", "max_plays.Title", "max_plays.Plays")

# Calcular el videojuego más popular por cada equipo
df_teams_popular = df_teams.groupBy("TeamMember").agg(F.max(F.struct(F.col("Plays"), F.col("Title"))).alias("max_plays")).select("TeamMember", "max_plays.Title", "max_plays.Plays")

# Se guardan los archivos 
df_teams_popular.write.mode("overwrite").option("header", True).csv("/home/ubuntu/4_teamout.csv")
df_genres_popular.write.mode("overwrite").option("header", True).csv("/home/ubuntu/4_genreout.csv")
