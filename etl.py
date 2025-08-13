import configparser
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_unixtime
from pyspark.sql.functions import (year, month, dayofmonth,
                                   hour, weekofyear, dayofweek,
                                   date_format)
from pyspark.sql.types import (StructType, StructField as Fld,
                            DoubleType as Dbl, StringType as Str,
                            IntegerType as Int, DataType as Date,
                            TimestampType as Ts)


# config = configparser.ConfigParser()
# config.read('dl.cfg')

# os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
        Description: Creates spark session.

        Returns: spark session object
    # """
    # AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
    # AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

    #.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    spark = SparkSession \
        .builder \
        .getOrCreate()
    
    #spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    #spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    #spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", AWS_ACCESS_KEY_ID)
    #spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", AWS_SECRET_ACCESS_KEY)
    #spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
    #spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3n.endpoint", "s3.amazonaws.com")
    return spark

def song_schema():
    """
    Description: Provides the schema for the staging_songs table.

    Returns:
        spark dataframe schema object
    """
    return StructType([
        Fld("num_songs", Int()),
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dbl()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_name", Str()),
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("duration", Dbl()),
        Fld("year",Int())
    ])

def process_song_data(spark, input_data, output_data):
    """
    Description: Read in songs data from json files.
                 Outputs songs and artists dimension tables in parquet files in S3.

    Arguments:
        spark: the spark session object. 
        input_data: path to the S3 bucket containing input json files.
        output_data: path to S3 bucket that will contain output parquet files. 

    Returns:
        None
    """
    # get file path
    song_data = input_data

    # read song data file
    df = spark.read.json(song_data, schema=song_schema(),
                         recursiveFileLookup=True)
    #df.printSchema()

    # extract columns to create songs table
    song_table = df.select([
        "song_id",
        "title",
        "artist_id",
        "year",
        "duration"
    ]).distinct().where(col("song_id").isNotNull())

    #print(f"Song table data: {song_table.show()}")
    droping_dublicate_before_writing_to_parquet = song_table.dropDuplicates()

    # Writing song table to parquet files partition by year and artist
    droping_dublicate_before_writing_to_parquet.write.parquet(
        f"{output_data}/songs_table",
        mode="overwrite",
        partitionBy=["year","artist_id"]
    )
    print("Success:Song table written to parquet.")

    # extract columns to create artist table
    artist_table = df.select([
        'artist_id', 'artist_name',
        'artist_location', 'artist_latitude',
        'artist_longitude'
    ]).dropDuplicates()

    #print("Artist table: ",artist_table.show())

    # writing artist table to parquet files
    artist_table.write.parquet(
        f'{output_data}/artists_table',
        mode="overwrite",

    )
    print("Success:Artist table written to parquet.")

    

    
def main():
    """
    Description: Calls functions to create spark session, read from S3
    and perform ETL to S3 Data Lake.

    Returns:
        None
    """
    spark = create_spark_session()
    input_data = "D:/workspace/Data_Lake/song_data/"
    output_data = "D:/workspace/Data_Lake/output_files/"
    return process_song_data(spark,input_data,output_data)

    
if __name__ == "__main__":
    main()