import pyspark;
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("NetflixDataTest").getOrCreate();

df = spark.read.csv("practicaFiltrado/test_netflix_titles.csv", header=True, inferSchema=True);

df.show()


spark.stop()