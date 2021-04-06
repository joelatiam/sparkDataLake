import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date


config = configparser.ConfigParser()
config.read('dl.cfg')

# AWS Credentials
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

# Files Paths
DATA_SOURCE = config['FILES_PATHS']['DATA_SOURCE']
DATA_DESTINATION = config['FILES_PATHS']['DATA_DESTINATION']



def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):

    # filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    # song_data schema
    songDataSchema =  R([
        Fld("song_id",Str()),
        Fld("title",Str()),
        Fld("year",Int()),
        Fld("duration",Dbl()),
        Fld("artist_id",Str()),
        Fld("artist_name",Str()),
        Fld("artist_location",Str()),
        Fld("artist_latitude",Dbl()),
        Fld("artist_longitude",Dbl()),
    ])
    
    df = spark.read.json(song_data, schema=songDataSchema)

    # songs table extraction
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    
    # songs table to parquet files partitioned by year and artist
    songs_table_out_path = os.path.join(output_data, 'songs/')
    songs_table.write.partitionBy("year", "artist_id")\
        .mode("overwrite")\
        .parquet(songs_table_out_path)

    # artists table extraction
    artists_table = df.select(
        col("artist_id"),
        col("artist_name").alias("name"),
        col("artist_location").alias("location"),
        col("artist_latitude").alias("lattitude"),
        col("artist_longitude").alias("longitude")
        )
    
    # artists table to parquet files
    artists_table_out_path = os.path.join(output_data, 'artists/')

    artists_table.write.mode("overwrite")\
        .parquet(artists_table_out_path)


# def process_log_data(spark, input_data, output_data):
#     # get filepath to log data file
#     log_data =

#     # read log data file
#     df = 
    
#     # filter by actions for song plays
#     df = 

#     # extract columns for users table    
#     artists_table = 
    
#     # write users table to parquet files
#     artists_table

#     # create timestamp column from original timestamp column
#     get_timestamp = udf()
#     df = 
    
#     # create datetime column from original timestamp column
#     get_datetime = udf()
#     df = 
    
#     # extract columns to create time table
#     time_table = 
    
#     # write time table to parquet files partitioned by year and month
#     time_table

#     # read in song data to use for songplays table
#     song_df = 

#     # extract columns from joined song and log datasets to create songplays table 
#     songplays_table = 

#     # write songplays table to parquet files partitioned by year and month
#     songplays_table


def main():
    spark = create_spark_session()
    input_data = DATA_SOURCE # use "s3a://udacity-dend/"  for prod 
    output_data = DATA_DESTINATION # use s3 location for prod
    
    process_song_data(spark, input_data, output_data)    
    # process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
