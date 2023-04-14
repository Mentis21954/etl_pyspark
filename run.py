from pyspark.sql import SparkSession
from extract import extract_info_from_artist, extract_titles_from_artist
from transform import remove_wrong_values, drop_duplicates_titles, clean_the_text, integrate_data
from load import load_to_database

spark = SparkSession.builder \
             .appName("ETL") \
             .master("local[*]") \
             .getOrCreate()

# find names from csv file
df = spark.read.csv("spotify_artist_data.csv", header=True)
artist_names = df.select('Artist Name').rdd.map(lambda r: r[0]).collect()

# find info and return a dataframe
artist_contents = extract_info_from_artist(artist_names[:2])
# clean the text info from dataframe
content_df = clean_the_text(artist_contents)

for name in artist_names[:2]:
    releases = extract_titles_from_artist(name)
    releases_df = remove_wrong_values(releases)
    releases_df = drop_duplicates_titles(releases_df)

    data = integrate_data(content_df, releases_df, name)
    load_to_database(data)