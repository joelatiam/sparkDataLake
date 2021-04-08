import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear,\
    date_format, monotonically_increasing_id
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl,\
    StringType as Str, IntegerType as Int, LongType as Long, ShortType as Short,\
    DateType as Date


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
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")

    songDataSchema =  R([
        Fld("song_id",Str()),
        Fld("title",Str()),
        Fld("year",Short()),
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
    songs_table_out_path = os.path.join(output_data, "songs/")
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
    artists_table_out_path = os.path.join(output_data, "artists/")
    artists_table.write.mode("overwrite")\
        .parquet(artists_table_out_path)


def process_log_data(spark, input_data, output_data):

    log_data = os.path.join(input_data, "log_data/*/*/*.json")
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # users table  data from song play dataframe 
    users_table = df.select(
        col("userId").alias("user_id").cast(Long()),
        col("firstName").alias("first_name"),
        col("lastName").alias("last_name"),
        col("gender"),
        col("level"),
    )
    
    users_table_out_path = os.path.join(output_data, "users/")
    users_table.write.mode("overwrite")\
        .parquet(users_table_out_path)

    # timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int((int(x)/1000)))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # datetime column from original timestamp column
    get_datetime =  udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.timestamp))
    
    # time table creation
    # and partitioned by year and month on parquet files
    time_table = df.select(
        col("timestamp").alias("start_time"),
        hour("datetime").alias("hour").cast(Short()),
        dayofmonth("datetime").alias("day").cast(Short()),
        weekofyear("datetime").alias("week").cast(Short()),
        month("datetime").alias("month").cast(Short()),
        year("datetime").alias("year").cast(Short()),
        date_format("datetime","E").alias("weekday").cast(Short())
    )
    
    time_table_out_path = os.path.join(output_data, "time/")
    time_table.write.partitionBy("year", "month")\
        .mode("overwrite")\
        .parquet(time_table_out_path)

    # song data to use for songplays table
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    song_df = spark.read.json(song_data)
    song_df = song_df.select(
        col("song_id"),
        col("title"),
        col("artist_id"),
        col("artist_name"),
        col("year"),
        col("duration")
    )

    # songplays table  extracted  from joined song and log datasets
    # then partitioned by year and month on parquet files
    songplays_table = df.join(
        song_df,
        (df.song == song_df.title)
        & (df.artist == song_df.artist_name)
        & (df.length == song_df.duration),
        "left_outer")\
        .select(
            monotonically_increasing_id().alias("songplay_id"),
            col("timestamp").alias("start_time"),
            col("userId").alias('user_id'),
            df.level,
            song_df.song_id,
            song_df.title,
            song_df.artist_id,
            col("sessionId").alias("session_id"),
            df.location,
            col("useragent").alias("user_agent"),
            year('datetime').alias('year'),
            month('datetime').alias('month')
    )

    songplays_table_out_path = os.path.join(output_data, "songplays/")
    songplays_table.write.partitionBy("year", "month")\
        .mode("overwrite")\
        .parquet(songplays_table_out_path)



def main():
    spark = create_spark_session()
    input_data = DATA_SOURCE
    output_data = DATA_DESTINATION
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
