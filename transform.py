from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace

spark = SparkSession.builder.appName("ETL").master("local[*]").getOrCreate()


def clean_the_text(content: dict):
    content_df = spark.createDataFrame(content)

    # remove new line commands, html tags and "", ''
    content_df = content_df.withColumn('Content', regexp_replace('Content', r'\r+|\n+|\t+', ''))
    content_df = content_df.withColumn('Content', regexp_replace('Content', r'<[^<>]*>', ''))
    content_df = content_df.withColumn('Content', regexp_replace('Content', r'"', ''))
    content_df = content_df.withColumn('Content', regexp_replace('Content', r"'", ''))
    print('Clean the informations text')

    return content_df


def remove_wrong_values(releases: dict):
    df = spark.createDataFrame(releases)

    # find and remove the rows/titles where there are no selling prices in discogs.com
    df = df.dropna(subset=['Discogs Price'])
    print('Remove releases where there no selling price in discogs.com')
    # keep only the rows has positive value of year
    df = df.where(df.Year > 0)
    print('Remove releases where have wrong year value in discogs.com')

    return df


def merge_titles_data(releases_df, playcounts: dict):
    playcount_df = spark.createDataFrame(playcounts)

    df = releases_df.join(playcount_df, on=['Title'], how='inner')
    print('Merge releases and playcounts data')

    return df


def drop_duplicates_titles(df):
    df = df.dropDuplicates(subset=['Title'])
    print('Find and remove the duplicates titles if exist!')

    return df


def integrate_data(content_df, releases_df, name):
    # find content from dataframe for specific artist name
    content = content_df.select('Content').where(content_df.Artist == name).rdd.map(lambda r: r[0]).collect()
    # convert df to pandas df
    releases_df = releases_df.toPandas()
    releases_df = releases_df.set_index('Title')

    # final data
    return {'Artist': name,
            'Description': content[0],
            'Releases': releases_df.to_dict('index')
            }
