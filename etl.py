
import configparser
from datetime import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import udf, col, row_number
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_timestamp


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']



def create_spark_session():
    ''''
    Function to create SparkSession Instance
    Args:
        none
    Returns:
            spark object 
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    ''' 
        Function to Process Song data to get songs and artists tables data
            Args:
                spark: the ouptut from create_spark_session().
                input_data:JSON file "song_data" stored in AWS S3.
                output_data : the path for saving parquet files.
           Returns:
                   None.
    '''
    # get filepath to song data file
    song_data = input_data+"song_data/A/B/C/TRABCEI128F424C983.json"
    
    # reading song data file
    df = spark.read.json(song_data)

    
    # extracting columns to create songs table
    songs_table = df.select( "song_id", "title", "artist_id", "year", "duration")\
                    .drop_duplicates()    
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet("songs.parquet")

    # extract columns to create artists table
    artists_table = df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude")\
                      .drop_duplicates()

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet("artist.parquet")

def process_log_data(spark, input_data, output_data):
    ''' 
        Function to Process Log data to get Time and songplays tables data
            Args:
                spark: the ouptut from create_spark_session().
                input_data:JSON file "song_data" stored in AWS S3.
                output_data : the path for saving parquet files.
           Returns:
                   None.
    '''
    # get filepath to log data file
    log_data = input_data+"log_data/2018/11/2018-11-13-events.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df[df['page']=="NextSong"]

    # extract columns for users table    
    users_table =df.select("userId","firstName", "lastName", "gender","level" ).drop_duplicates()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet("users.parquet")

    # create datetime column from original timestamp column
    df = df.withColumn("ts_timestamp", to_timestamp(col('ts')/1000))

    # extract columns to create time table
    time_table = df.select('ts_timestamp',hour('ts_timestamp').alias('hour'),\
                           dayofmonth('ts_timestamp').alias('day'),
                           weekofyear('ts_timestamp').alias('week'),month('ts_timestamp').alias('Month'),\
                           year('ts_timestamp').alias('Year'),\
                           date_format('ts_timestamp', 'EEEE').alias('dayName'))
       
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("Year", "Month").mode('overwrite').parquet("time.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.parquet("songs.parquet")
    artist_df = spark.read.parquet("artist.parquet")
    
    #Join song and artist data to get song data
    song_artist_df = song_df.join(artist_df,(song_df.artist_id == artist_df.artist_id))\
                            .select(artist_df.artist_id, artist_df.artist_name, song_df.song_id,\
                                    song_df.title, song_df.duration)

    
    # extract columns from joined song and log datasets to create songplays table         
    songplays_table = df.join(song_artist_df,(df.song == song_artist_df.title) & \
                              ( df.length == song_artist_df.duration)&\
                              (df.artist == song_artist_df.artist_name))\
            .select(df.ts_timestamp,df.userId,df.level,song_artist_df.song_id,song_artist_df.artist_id,df.sessionId\
                                ,df.location, df.userAgent)
    
   #create songplay ID column
    w = Window().orderBy('song_id')
    songplays_table = songplays_table.withColumn("songplay_id", row_number().over(w))
   
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').parquet('songplays.parquet')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
