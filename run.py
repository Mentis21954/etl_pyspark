from pyspark.sql import SparkSession

spark = SparkSession.builder \
             .appName("ETL") \
             .master("local[*]") \
             .getOrCreate()

df = spark.read.csv("spotify_artist_data.csv", header=True)
print(df.show(5,0))
df2 = df.distinct()
print(df2.show(5,0))
