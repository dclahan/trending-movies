import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date, year, month, dayofmonth

'''
glue job params
--path_tmdb     s3://trending-movies-clahad/tmdb_raw/
--path_output   s3://trending-movies-clahad/data/movie_trends_database/tmdb_mentions_db/
--date          2024-12-18
'''

def process_tmdb(glueContext, input_path, date):
    """ 
    Reads raw TMDB movie data and
    selects relevant quantitative fields for given date

    Args:
    glueContext : GlueContext object
    input_path (str) : input path for TMDB movie data
    date (str) : date in 'YYYY-MM-DD' format to filter TMDB data

    Returns:
    tmdb_stats (Dataframe) : movie stats from TMDB data
    """
    # Read the TMDB raw data
    spark = glueContext.spark_session
    tmdb_raw = spark.read.json(input_path)

    # Select relevant quantitative fields
    tmdb_stats = tmdb_raw.select('id', 'popularity', 'vote_average', 'vote_count',
                                 to_date('query_datetime').alias('date'))

    # Filter based on the date
    tmdb_stats = tmdb_stats.filter(col("date") == date)

    return tmdb_stats

def save_tmdb_stat(glueContext, tmdb_stats, output_path):
    """ 
    Saves TMDB movie stats in Parquet format partitioned by year, month, and day

    Args:
    glueContext : GlueContext object
    tmdb_stats (Dataframe) : movie stats from TMDB API
    output_path (str) : output path to save the given dataframe
    """
    # Add year, month, day columns for partitioning
    tmdb_stats_save = tmdb_stats.withColumn('year', year('date')) \
                                .withColumn('month', month('date')) \
                                .withColumn('day', dayofmonth('date'))

    # Save with year, month, day partitions
    tmdb_stats_save.write.partitionBy('year', 'month', 'day').mode("overwrite").parquet(output_path)

if __name__ == "__main__":
    # Parse arguments
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'path_tmdb', 'path_output', 'date'])

    # Initialize GlueContext and Job
    sc = SparkContext()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # Process TMDB movie data
    tmdb_stats = process_tmdb(glueContext, args['path_tmdb'], args['date'])

    # Save the TMDB movie stats
    save_tmdb_stat(glueContext, tmdb_stats, args['path_output'])

    # Commit the Glue job
    job.commit()
