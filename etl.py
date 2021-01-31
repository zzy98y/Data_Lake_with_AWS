import configparser
from datetime import datetime
import os
import pyspark.sql.types as t
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
   """
   Transform song_data in S3 into song_tables and artist_table
   Input: 
   spark: Spark Session we created 
   input_data: Path to input file we store in S3
   output_data: Path to output bucket we created in S3 
   
   """
    # get filepath to song data file
    song_data = input_data + 'song_data' + '/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id','artist_id','title','year','duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").parquet(output_data + '/song_table')

    # extract columns to create artists table
    artists_table = df.select('artist_id','artist_name','artist_location',\
                              'artist_latitude','artist_longitude').dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + '/artist_table')


def process_log_data(spark, input_data, output_data):
   """
   Transform log_data in S3 into user_tables,time_table and songplay_table
   Input: 
   spark: Spark Session we created 
   input_data: Path to input file we store in S3
   output_data: Path to output bucket we created in S3 
   """
    # get filepath to log data file
    log_data = input_data + 'log_data' + '/*/*/*.json'
    song_data = input_data + 'song_data' + '/*/*/*/*.json'
    # read log data file
    log_df = spark.read.json(log_data)
    # filter by actions for song plays
    log_df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    uers_table = log_df.select('userId','firstName','lastName','gender','level').drop_Duplicates()
    
    # write users table to parquet files
    users_table.write.mode.parquet(output_data,'/user_table')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:datetime.fromtimestamp(x/1000.0))
    timestamp_df = log_df.withColumn("start_time", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: x,t.TimestampType())
    datetime_df = timestamp_df.select('start_time').drop_Duplicates.withColumn('datetime',get_datetime('start_time'))
    
    # extract columns to create time table
    time_table = datetime_df.select('start_time',
                                   hour('datetime').alias('hour'),
                                   dayofmonth('datetime').alias('day'),
                                   weekofyear('datetime').alias('week'),
                                   month('datetime').alias('month'),
                                   year('datetime').alias('year'),
                                   dayofweek('datetime').alias('weekday'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + '/time_table')

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = log_df.join(songs_df,log_df.artist == song_df.artist_name).select(
                        'ts','userId','song_id','artist_id','level','sessionId','location','userAgent').\
                        drop_Duplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data + '/songplay_table')


def main():
    spark = create_spark_session()
    input_data = "s3n://udacity-dend/"
    output_data = "s3n://sparkify-data-lake"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
