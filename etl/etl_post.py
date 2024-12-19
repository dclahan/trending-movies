from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import from_unixtime, to_date, year, month, dayofmonth, udf, col, array_contains, expr, lit, when
from pyspark.sql.types import StringType, ArrayType
import sys
import re

# Initialize GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Get job arguments
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'path_post', 'path_movie', 'output', 'date', 'num_partitions'
])

# Set Spark configurations
spark.conf.set("spark.sql.shuffle.partitions", args['num_partitions'])
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Define functions
def process_movies(input_path):
    """Reads movie list into a Spark DataFrame and processes keywords."""
    movies = spark.read.csv(input_path, header=True)

    def unify_keyword(keyword):
        return keyword.lower() if keyword else None
    unify_keyword_udf = udf(lambda x: unify_keyword(x), StringType())

    def alternate_keyword(keyword):
        if keyword and keyword.startswith('the'):
            return keyword[3:]
        elif keyword:
            return "the" + keyword
        return None
    alternate_keyword_udf = udf(lambda x: alternate_keyword(x), StringType())

    movies = movies.withColumn("unified_keyword", unify_keyword_udf(movies.title_simple))
    movies = movies.withColumn("alternate_keyword", alternate_keyword_udf(movies.unified_keyword))
    return movies

def process_posts(input_path, date, movie_names, num_partitions):
    """Reads raw posts and processes text data."""
    posts_raw = spark.read.json(input_path).repartition(num_partitions)

    def get_final_text(title, selftext):
        return f"{title} {selftext}" if selftext else title
    get_final_text_udf = udf(
        lambda title, selftext: get_final_text(title, selftext), StringType()
    )

    def get_simple_text(final_text):
        return re.sub(r'[^A-Za-z0-9]', '', final_text.lower()) if final_text else None
    get_simple_text_udf = udf(get_simple_text, StringType())

    def get_subreddit(subreddit):
        return subreddit[2:].lower() if subreddit else None
    get_subreddit_udf = udf(get_subreddit, StringType())

    reddit_posts = posts_raw.select(
        "id",
        get_subreddit_udf("subreddit_name_prefixed").alias("subreddit"),
        get_final_text_udf("title", "selftext").alias("final_text"),
        to_date(from_unixtime(col("created_utc"))).alias("date"),
        "upvote_ratio"
    )
    reddit_posts = reddit_posts.withColumn("simple_text", get_simple_text_udf(reddit_posts.final_text))
    reddit_posts = reddit_posts.filter(col("date") == date)

    def movie_mention(text):
        mentions = [movie for movie in movie_names if movie in text] if text else []
        return mentions
    movie_mention_udf = udf(movie_mention, ArrayType(StringType()))

    reddit_posts = reddit_posts.withColumn('text_mentions', movie_mention_udf(reddit_posts.simple_text))
    return reddit_posts

def compute_post_movie_stats(movies, reddit_posts):
    """Calculates Reddit post statistics for movie mentions."""
    mention_text = movies.join(
        reddit_posts,
        expr("array_contains(text_mentions, unified_keyword)"),
        "left"
    )
    mention_text = mention_text.withColumn("match", when(col("id").isNull(), 0).otherwise(1))

    subreddit_mentions = mention_text.groupBy("id", "subreddit").sum("match").withColumnRenamed("sum(match)", "mention_count")
    count_text = mention_text.groupBy("id").sum("match").withColumnRenamed("sum(match)", "text_count")

    post_stats = count_text.join(subreddit_mentions, on=["id"], how="inner")
    return post_stats

def save_post_stat(post_stats, output_path, date):
    """Saves post statistics in Parquet format partitioned by year, month, and day."""
    post_stats_save = post_stats.withColumn('date', to_date(lit(date)))
    post_stats_save = post_stats_save.withColumn('year', year('date')) \
                                     .withColumn('month', month('date')) \
                                     .withColumn('day', dayofmonth('date'))
    post_stats_save.write.partitionBy('year', 'month', 'day').mode("overwrite").parquet(output_path)

# Main workflow
movies = process_movies(args['path_movie'])
movie_names = [str(row.unified_keyword) for row in movies.select('unified_keyword').collect()]
posts = process_posts(args['path_post'], args['date'], movie_names, int(args['num_partitions']))
post_stats = compute_post_movie_stats(movies, posts)
save_post_stat(post_stats, args['output'], args['date'])
