import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function loads data from S3 and extract the songs from song_data then load it back to S3

    Parameters:
    spark       : spark session
    input_data  : this is to get metadata from json file for the extraction
    output_data : using S3 to load back the input_data again after processing

    """


    # get filepath to song data file
    song_data = os.path.join(input_data, "song-data/*/*/*/*.json")

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()


    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs/songs/parquet'), 'overwrite')



    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists/artists/parquet'), 'overwrite')





# ------------------------------------



def process_log_data(spark, input_data, output_data):

    """
    This function loads data from S3 and extract the songs from log_data then load it back to S3 with parquet format.

    Parameters:
        spark       : spark session
        input_data  : this is to get metadata from json file for the extraction
        output_data : using S3 to load back the input_data again after processing.

    """



    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*.json")

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    artists_table = df.select('userId')

    # write users table to parquet files
    user_table = df.select('firstName', 'lastName', 'gender', 'level', 'userId').dropDuplicates()

    user_table.createOrReplaceTempView('users')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())

    df = df.withColumn('timestamp', get_timestamp(col('ts')))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df = df.withColumn('datetime', get_datetime(col('ts')))

    # extract columns to create time table
    time_table = df.select('datetime') \
                    .withColumn('start_time', df.datetime) \
                    .withColumn('hour', hour('datetime')) \
                    .withColumn('day', dayofmonth('datetime')) \
                    .withColumn('week', week('datetime')) \
                    .withColumn('month', month('datetime')) \
                    .withColumn('year', year('datetime')) \
                    .withColumn('weekday', dayofweek('datetime')) \
                    .dropDuplicates()


    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time/time.parquet'), 'overwrite')


    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(
        song_df,
        song_df.artist_name == df.artists,
        'inner'
    ) \
    .distinct() \
    .select(
        col('start_time'),
        col('userId'),
        col('level'),
        col('sessionId'),
        col('location'),
        col('userAgent'),
        col('song_id'),
        col('artist_id')) \
    .withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays/songplays.parquet'), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://mar-datalake/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
