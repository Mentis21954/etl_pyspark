from pyspark.sql import SparkSession
from extract import extract_info_from_artist, extract_titles_from_artist, extract_playcounts_from_titles_by_artist, find_info_for_titles
from transform import clean_the_text, remove_wrong_values, merge_titles_data, drop_duplicates_titles, integrate_data
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
    # extract
    releases = extract_titles_from_artist(name)
    playcounts = extract_playcounts_from_titles_by_artist(name, releases)
    releases = find_info_for_titles(releases)
    # transform
    releases_df = remove_wrong_values(releases)
    releases_df = merge_titles_data(releases_df, playcounts)
    releases_df = drop_duplicates_titles(releases_df)
    data = integrate_data(content_df, releases_df, name)
    # load
    load_to_database(data)