import os

# Directory names
DB_DIR = '../databases'
LOG_DIR = '../logs'
# TWEET_DIR = 'tweet_raw'
POSTS_DIR = 'posts_raw'
TMDB_DIR = 'tbdm_raw'

# Directories
dirDB = os.path.join(os.getcwd(), DB_DIR)
dirLogs = os.path.join(os.getcwd(), LOG_DIR)
dirPosts = os.path.join(os.getcwd(), POSTS_DIR)
# dirTweet = os.path.join(os.getcwd(), TWEET_DIR)
dirTmdb = os.path.join(os.getcwd(), TMDB_DIR)

# Database parameters
dbName = 'stream.db'
dbPath = os.path.join(dirDB, dbName)
tbName = 'reddit_posts'

# streaming limits per minute
streamCount = 100
streamPeriod = 60 # seconds

# S3 parameters
s3_bucket = 'trending-movies-clahad'
s3_key_reddit = 'posts_raw'
s3_key_tmdb = 'tmdb_raw'
s3_upload_try = 5

# movies filepath
movies_fname = '../movies_list.csv'
movies_fpath = os.path.join(os.getcwd(), movies_fname)
