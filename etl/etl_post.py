import argparse
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, unix_timestamp, to_date, year, month, dayofmonth,\
                                  udf, col, array_contains, expr, broadcast, when, lit
from pyspark.sql.types import StringType, ArrayType
import re

# Default number of partitions
num_partitions_def = 4

def create_spark_session(shuffle_partition_size):
    """ Creates Spark Session object with appropriate configurations.

    Args:
    shuffle_partition_size (int) : Spark shuffle partition size

    Returns:
    spark: Spark Session object
    """
    spark = SparkSession \
        .builder \
        .appName("ETL post") \
        .getOrCreate()

    # Set the log level
    spark.sparkContext.setLogLevel('WARN')
    # Set dataframe shuffle partition size
    spark.conf.set("spark.sql.shuffle.partitions", shuffle_partition_size)
    # Enable dynamic partition overwrite
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    return spark

def process_movies(spark, input_path):
    """ Reads movie list into a Spark dataframe and processes keywords

    Args:
    spark : Spark Session object
    input_path (str): input path for the CSV file

    Returns:
    movies (Dataframe) : movie data with names and corresponding keywords
    """
    # Read movie data
    movies = spark.read.csv(input_path, header=True)

    # Unify movies keywords (lowercase, no punctuation)
    def unify_keyword(keyword):
        return keyword.lower() # should be already but its ok
    unify_keyword_udf=udf(lambda x: unify_keyword(x), StringType())

    # Alternate keywords (with or without 'the' prefix)
    def alternate_keyword(keyword):
        if keyword[0:3] == 'the':
            return keyword[3:]
        else:
            return "the" + keyword
    alternate_keyword_udf=udf(lambda x: alternate_keyword(x), StringType())

    # Add unified and alternate keyword columns
    movies = movies.withColumn("unified_keyword", unify_keyword_udf(movies.title_simple))
    movies = movies.withColumn("alternate_keyword", alternate_keyword_udf(movies.unified_keyword))

    return movies

def process_posts(spark, input_path, date, movie_names, num_partitions):
    """ Reads raw posts and applies UDFs for text processing

    Args:
    spark : Spark Session object
    input_path (str) : input path for raw posts
    date (str) : date in 'YYYY-MM-DD' format to filter posts
    movie_names (str) : list of movie names to search in posts text (in simple_title format!)
    num_partitions (int) : number of partition to use

    Returns:
    posts (Dataframe) : processed posts data
    """
    # Read raw post data
    posts_raw = spark.read.json(input_path)

    # Repartition
    posts_raw = posts_raw.repartition(num_partitions)

    # Get the final text of the posts based on reposted and extended status
    def get_final_text(title, selftext):
        return f"{title} {selftext}" if selftext else title
    get_final_text_udf = udf(
        lambda title, selftext: get_final_text(title, selftext), StringType()
    )

    def get_simple_text(final_text):
        return re.sub(r'[^A-Za-z0-9]', '', final_text.lower())
    get_simple_text_udf = udf(get_simple_text, StringType())

    # Convert subreddit name to lowercase and remove "r/"
    def get_subreddit(subreddit):
        return subreddit[2:].lower() if subreddit else subreddit
    get_subreddit_udf = udf(get_subreddit, StringType())

    # Select the relevant columns from the raw data
    reddit_posts = posts_raw.select(
        "id",
        get_subreddit_udf("subreddit_name_prefixed").alias("subreddit"),
        get_final_text_udf("title", "selftext").alias("final_text"),
        to_date(from_unixtime(col("created_utc"))).alias("date"),
        "upvote_ratio"
    )
    reddit_posts = reddit_posts.withColumn("simple_text", get_simple_text_udf(reddit_posts.final_text))

    # Filter based on the date
    reddit_posts = reddit_posts.filter(col("date") == date)

    # Get movie mentions in the text of posts
    def movie_mention(text):
        mentions = []
        for movie in movie_names:
            if movie in text:
                mentions.append(movie)
        return mentions
    movie_mention_udf = udf(movie_mention, ArrayType(StringType()))

    reddit_posts = reddit_posts.withColumn('text_mentions', movie_mention_udf(reddit_posts.simple_text))

    return reddit_posts

def compute_post_movie_stats(movies, reddit_posts):
    """ Calculates Reddit post text count of movies and tracks subreddits of mentions

    Args:
    movies (DataFrame) : movies with names
    reddit_posts (DataFrame) : Reddit posts with subreddit, upvote ratio, and mentions of movies

    Returns:
    post_stats (DataFrame) : Reddit post count statistics and subreddits of mentions
    """
    # Movies mentioned in the text
    mention_text = broadcast(movies).join(
        reddit_posts,
        expr("array_contains(text_mentions, name)"),
        "left"
    )
    mention_text = mention_text.withColumn("match", when(col("id").isNull(), 0).otherwise(1))

    # Track subreddits where mentions occur
    subreddit_mentions = mention_text.groupBy("id", "subreddit").sum("match").withColumnRenamed("sum(match)", "mention_count")

    # Mention counts based on text
    count_text = mention_text.groupBy("id").sum("match").withColumnRenamed("sum(match)", "text_count")

    post_stats = count_text \
                .join(subreddit_mentions, on=["id"], how="inner")

    return post_stats



def save_post_stat(spark, post_stats, output_path, date):
    """ Saves post count statistics in Parquet format partitioned by year, month and day

    Args:
    spark : Spark Session object
    post_stats (Dataframe) : post count statistics based on keyword in post and the subreddit it was mentioned in
    output_path (str) : output path to save the given dataframe
    date (str) : date (YYYY-MM-DD format) on which post count statistics are calculated
    """
    # Add year, month, day columns for partitioning
    post_stats_save = post_stats.withColumn('date', to_date(lit(date)))
    post_stats_save = post_stats_save.withColumn('year', year('date')) \
                                       .withColumn('month', month('date')) \
                                       .withColumn('day', dayofmonth('date')) \

    # Merge partitions before saving
    post_stats_save = post_stats_save.repartition(1)

    # Save with year, month, day partitions
    post_stats_save.write.partitionBy('year','month','day').mode("overwrite").parquet(output_path)
    return

if __name__ == "__main__":

    # Parse arguments
    parser = argparse.ArgumentParser(description="ETL post")
    parser.add_argument("path_post", type=str, help="Path for raw post data")
    parser.add_argument("path_movie", type=str, help="Path for movie data")
    parser.add_argument("output", type=str, help="Output path to save post stats")
    parser.add_argument("date", type=str, help="Date in 'YYYY-MM-DD' format to filter posts")
    parser.add_argument("--num_partitions", type=str, default=num_partitions_def, help="Number of partitions to use")
    args = parser.parse_args()

    # Create the spark session
    spark = create_spark_session(args.num_partitions)

    # Process movie data and persist it
    movies = process_movies(spark, args.path_movie)
    movies.persist(StorageLevel.MEMORY_AND_DISK)

    # movie mentions in the text of posts
    movie_names = [str(row.title_simple) for row in movies.select('title_simple').collect()]

    # Process raw post data and persist it
    posts = process_posts(spark, args.path_post, args.date, movie_names, args.num_partitions)
    posts.persist(StorageLevel.MEMORY_AND_DISK)

    # Compute post stats for movies
    post_stats = compute_post_movie_stats(movies, posts)

    # Save the post stats
    save_post_stat(spark, post_stats, args.output, args.date)