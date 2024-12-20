import os
import sys
import logging
import configparser
import time
import json
import sqlite3
import praw
import pandas as pd
from datetime import date, datetime, timedelta
from settings import dbPath, tbName, dirDB, dirLogs, streamCount, streamPeriod, movies_fpath
from utils import createDir, setupLogger, connectToDB, filterStringbySubstrings

# SQL queries for table creation and data insertion
CREATE_TABLE_SQL =  '''CREATE TABLE IF NOT EXISTS {} (
                        id_str varchar(128) not null,
                        reddit_datetime varchar(256),
                        reddit_raw text,
                        primary key (id_str)
                    )'''.format(tbName)
INSERT_TABLE_SQL =  '''INSERT INTO {} values (?, ?, ?)'''.format(tbName)

class RedditStreamListener:
    def __init__(self, reddit, cursor, limit_count, limit_time, subreddits, trackphrases):
        """ Stream listener class constructor

        Args:
        reddit: PRAW Reddit instance
        cursor: database connection cursor
        limit_count (int): maximum number of posts to stream per session
        limit_time (float): maximum duration (seconds) to stream per session
        subreddits (string): subreddits to listen to
        """
        self.reddit = reddit
        self.cursor = cursor
        self.limit_count = limit_count
        self.limit_time = limit_time
        self.subreddits = subreddits
        self.trackphrases = trackphrases
        self.count = 0
        self.time_start = datetime.now()
        self.numReport = 100

    def listen(self):
        """ Listens to new posts and saves them to the database."""
        subreddit = self.reddit.subreddit(self.subreddits)
        for submission in subreddit.stream.submissions():
            if self.count % self.numReport == 0:
                logging.info(f"Number of posts streamed since the start: {self.count}")

            self.count += 1
            if filterStringbySubstrings(submission.title, self.trackphrases):
                post_id = submission.id
                post_datetime = datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S')
                subdict = vars(submission)
                del subdict["_reddit"]
                del subdict["subreddit"]
                del subdict["author"]
                if "poll_data" in subdict:
                    del subdict["poll_data"]
                post_raw = json.dumps(subdict)

                try:
                    self.cursor.execute(INSERT_TABLE_SQL,
                                        (post_id, post_datetime, post_raw))
                except sqlite3.Error as e:
                    logging.error(str(e))
                    logging.error(f"Post {post_id} could not be inserted into the table")

            if self.count >= self.limit_count:
                logging.info(f"Reached {self.limit_count} posts limit, stopping the stream")
                break

            if (datetime.now() - self.time_start).total_seconds() >= self.limit_time:
                logging.info(f"Reached {self.limit_time} seconds time limit, stopping the stream")
                break

def createRedditInstance(client_id, client_secret, user_agent):
    """ Creates a PRAW Reddit instance using Reddit credentials

    Args:
    client_id (str): Reddit app client ID
    client_secret (str): Reddit app client secret
    user_agent (str): User agent for the app

    Returns:
    reddit: PRAW Reddit instance
    """
    reddit = praw.Reddit(client_id=client_id,
                         client_secret=client_secret,
                         user_agent=user_agent)
    reddit.config.log_requests = 1
    reddit.config.store_json_result = True
    return reddit

def startRedditStream(reddit, cursor, limit_count, limit_time, subreddits, trackPhrases):
    """ Starts Reddit stream using PRAW

    Args:
    reddit: PRAW Reddit instance
    cursor: database connection cursor
    limit_count (int): maximum number of posts to stream per session
    limit_time (float): maximum duration (seconds) to stream per session
    subreddits (string): subreddits to listen to
    """
    redditStreamListener = RedditStreamListener(reddit, cursor, limit_count, limit_time, subreddits, trackPhrases)
    logging.info("Reddit stream listener is initialized")
    logging.info("Starting the Reddit stream")
    redditStreamListener.listen()
    logging.info("The Reddit stream disconnected")

def createTable(cursor, create_table_sql):
    """ Executes the given sql query for table creation

    Args:
    cursor : database connection cursor
    create_table_sql (str): sql query for table creation

    Returns:
    boolean : whether the query execution was successful or not
    """
    # Create the table if not exists
    try:
        cursor.execute(create_table_sql)
    except sqlite3.Error as e:
        logging.error(str(e))
        return False
    return True

def countRowsTable(cursor, tableName):
    """ Counts and logs number of rows in the given table

    Args:
    cursor : database connection cursor
    tableName (str): table name
    """
    # Report current number of rows
    try:
        cursor.execute('SELECT COUNT(*) FROM {}'.format(tableName))
        numRow = cursor.fetchone()[0]
    except sqlite3.Error as e:
        logging.error(str(e))
        logging.info('Could not count the number of rows for {}'.format(tableName))
    else:
        logging.info('Table {} has {} rows'.format(tableName, numRow))

def getTrackPhrases(path):
    """ Read Movie data (id, name, hashtag) and creates phrases for tracking

    Args:
    path (str): path for Movie CSV file

    Returns:
    phrases (str list): Movie hashtag and name
    """
    # Return Movie hashtag and name as track phrases
    df = pd.read_csv(path)
    phrases = df['title_simple'].to_list()
    logging.info('{} phrases are defined for tracking tweets'.format(len(phrases)))
    logging.info('Track phrases for tweets:')
    logging.info(phrases)
    return phrases

if __name__ == "__main__":
    # Create the required directories if not exits
    if not createDir(dirLogs):
        sys.exit('The directory "{}" could not be created'.format(dirLogs))
    if not createDir(dirDB):
        sys.exit('The directory "{}" could not be created'.format(dirDB))

    # Setup the logger
    logName = date.today().strftime("%Y-%m-%d") + '-reddit-posts-stream.log'
    setupLogger(dirLogs, logName)

    # Connect to the database
    conn = connectToDB(dbPath)
    if conn is None:
        logging.error('Error while connecting to the database')
        sys.exit(1)
    cursor = conn.cursor()

    # Create table to store streamed reddit-posts
    if not createTable(cursor, CREATE_TABLE_SQL):
        sys.exit(1)

    # Report number of rows in the reddit-posts table
    countRowsTable(cursor, tbName)

    # Read the API configuration file
    config = configparser.ConfigParser()
    config.read('./../api.cfg') #not good, use argparser + absolute paths

    # Create tweepy api
    api = createRedditInstance(
        config.get('REDDIT','CLIENT_ID'),
        config.get('REDDIT','CLIENT_SECRET'),
        config.get('REDDIT','USER_AGENT'),
    )

    # Prepare phrases for tracking
    trackPhrases = getTrackPhrases(movies_fpath)
    subReddits = ["movies","Film", "TrueFilm", "criterion", "Letterboxd"]

    # Start streaming tweets
    for sub in subReddits:
        startRedditStream(api, cursor, streamCount, streamPeriod, sub, trackPhrases)
        time.sleep(75)

    # Close the database connection
    try:
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(str(e))
        logging.info('Error while closing the database connection')
    else:
        logging.info('Closed the database connection')

    sys.exit(0)
