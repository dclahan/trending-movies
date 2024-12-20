import os
import sys
import logging
import argparse
import configparser
import sqlite3
import time
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from datetime import date, datetime, timedelta
from settings import dbPath, tbName, dirPosts, dirLogs, s3_bucket, s3_key_reddit, s3_upload_try
from utils import createDir, setupLogger, connectToDB, uploadFileToS3

def getPastHourInterval():
    """
    Calculates the time interval for the past hour

    Returns:
    dt_start (datetime) : datetime object for the past hour start
    datetime_start (str) : formatted datetime for the past hour start
    datetime_end (str) : formatted datetime for the current hour start
    """
    # Get the search period in terms of UTC time
    dt_cur = datetime.utcnow()
    logging.info('Current UTC time is {}'.format(dt_cur.strftime("%Y-%m-%d %H:%M:%S")))
    # Start and end of the time interval
    dt_end = dt_cur.replace(minute=0, second=0, microsecond=0)
    dt_start = dt_end - timedelta(hours=1)
    # Formatted datetime strings
    datetime_end = dt_end.strftime("%Y-%m-%d %H:%M:%S")
    datetime_start = dt_start.strftime("%Y-%m-%d %H:%M:%S")
    return dt_start, datetime_start, datetime_end

def getHourlyIntervals(dt_start: datetime, dt_end: datetime):
    """
    Calculates the formatted datetime for all hourly intervals in a given time range.

    Args:
    dt_start (datetime): Start of the time range (inclusive).
    dt_end (datetime): End of the time range (exclusive).

    Returns:
    hourly_intervals (list): List of formatted datetime strings for each hour in the range.
    """
    # Ensure start and end are in hourly format
    dt_start = dt_start.replace(minute=0, second=0, microsecond=0)
    dt_end = dt_end.replace(minute=0, second=0, microsecond=0)

    # Logging the range
    logging.info('Generating hourly intervals from {} to {}'.format(
        dt_start.strftime("%Y-%m-%d %H:%M:%S"), dt_end.strftime("%Y-%m-%d %H:%M:%S")
    ))

    # Generate the hourly intervals
    current_hour = dt_start
    hourly_intervals = []
    while current_hour < dt_end:
        hourly_intervals.append(current_hour.strftime("%Y-%m-%d %H:%M:%S"))
        current_hour += timedelta(hours=1)

    return hourly_intervals

def retrieveDataFromDB(conn, datetime_start, datetime_end):
    """
    Retrieves the posts from the database for given time interval

    Args:
    conn : database connection object
    datetime_start (str) : datetime interval start
    datetime_end (str) : datetime interval end

    Returns:
    df (pandas dataframe) : raw post data
    """
    logging.info('Search interval for posts: {} - {}'.format(datetime_start, datetime_end))
    # Construct the select query
    select_query = '''SELECT reddit_raw FROM {} WHERE
                        reddit_datetime > '{}' AND reddit_datetime < '{}'
                   '''.format(tbName, datetime_start, datetime_end)
    # Retrieve the data
    df = pd.read_sql_query(select_query, conn)
    return df

def getLocalPartitionedPath(dir_save, dt_save):
    """
    Returns a partitioned directory and path based on the given datetime

    Args:
    dir_save (str) : the local directory for saving
    dt_save (datetime) : datetime used for directory setup

    Returns:
    local_dir (str) : dir_save/<year>/<month>/<day>/<hour>
    local_path (str) : dir_save/<year>/<month>/<day>/<hour>/<year-month-day-hour>.json
    """
    year, month, day, hour = dt_save.strftime("%Y-%m-%d-%H").split('-')
    local_dir = os.path.join(dir_save, year, month, day, hour)
    local_path = os.path.join(local_dir, '{}-{}-{}-{}.json'.format(year, month, day, hour))
    logging.info('Local path to save raw data: {}'.format(local_path))
    return local_dir, local_path

def getS3PartitionedPath(key_prefix, dt_save):
    """
    Returns a partitioned S3 path based on the given datetime

    Args:
    key_prefix (str) : key prefix on S3
    dt_save (datetime) : datetime used for directory setup

    Returns:
    s3_path (str) : key_prefix/<year>/<month>/<day>/<hour>/<year-month-day-hour>.json
    """
    year, month, day, hour = dt_save.strftime("%Y-%m-%d-%H").split('-')
    s3_path = '{}/{}/{}/{}/{}/{}-{}-{}-{}.json'.format(
        key_prefix, year, month, day, hour, year, month, day, hour
    )
    logging.info('S3 key for upload: {}'.format(s3_path))
    return s3_path

def savePostsLocal(df, local_path):
    """
    Saves raw posts locally using the given path

    Args:
    df (pandas dataframe) : raw reddit data
    local_path (str) : path to save locally

    Returns:
    boolean : whether the file saving is successful or not
    """
    logging.info('Writing reddit data into {}'.format(local_path))
    isSuccess = False
    try:
        fp = open(local_path, 'w')
        for row in df.iloc[:,0].tolist():
            fp.write('{}\n'.format(row))
    except Exception as e:
        logging.error(str(e))
        logging.error('Error while writing posts to the file')
    else:
        isSuccess = True
        logging.info('posts are successfully saved to the file')
    finally:
        if fp:
            fp.close()
    return isSuccess


def main(args, dt_save, datetime_start, datetime_end):
    # # Get datetime interval for the past hour
    # dt_save, datetime_start, datetime_end = getPastHourInterval()

    # Connect to the database
    conn = connectToDB(dbPath)
    if conn is None:
        logging.error('Error while connecting to the database')
        sys.exit(1)

    # Get the post data streamed in the past hour
    df = retrieveDataFromDB(conn, datetime_start, datetime_end)

    # Close the database connection
    try:
        conn.close()
    except Exception as e:
        logging.error(str(e))
        logging.info('Error while closing the database connection')
    else:
        logging.info('Closed the database connection')

    # Check the number of records
    num_post = len(df)
    if num_post == 0:
        logging.error('No post was retrieved from the database')
        # return    # uncomment to backfill
        sys.exit(1)
    else:
        logging.info('{} posts were retrieved from the database'.format(num_post))

    # Local directory and path to save
    local_dir, local_path = getLocalPartitionedPath(dirPosts, dt_save)

    # Create the local directory for saving
    if not createDir(local_dir):
        logging.error('The directory "{}" could not be created')
        # return    # uncomment to backfill
        sys.exit(1)

    # Save the file locally
    if not savePostsLocal(df, local_path):
        logging.error('The post could not be saved to local, will not upload to S3')
        # return    # uncomment to backfill
        sys.exit(1)

    # Get S3 path to transfer the file
    s3_path = getS3PartitionedPath(s3_key_reddit, dt_save)

    # Read the API configuration file
    config = configparser.ConfigParser()
    config.read(args.path_config)

    # Upload the file to S3
    for i in range(s3_upload_try):
        isUploaded = uploadFileToS3(local_path, s3_bucket, s3_path,
                                    config.get('AWS','ACCESS_KEY_ID'),
                                    config.get('AWS','SECRET_ACCESS_KEY'),
                                    config.get('AWS','REGION')
        )
        # If not uploaded successfully, wait and try again
        if isUploaded:
            break
        else:
            logging.warning('Waiting 2 seconds and will try to upload to S3 again')
            time.sleep(2)

if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Upload TMDB movie data to S3",
        add_help=True
    )
    parser.add_argument("path_config", type=str,
                        help="Path to configuration file with API credentials")
    args = parser.parse_args()

    # Create the required directories if not exits
    if not createDir(dirLogs):
        sys.exit('The directory "{}" could not be created'.format(dirLogs))
    if not createDir(dirPosts):
        sys.exit('The directory "{}" could not be created'.format(dirPosts))

    # Setup the logger
    logName = date.today().strftime("%Y-%m-%d") + '-post-upload.log'
    setupLogger(dirLogs, logName)

    # regular mode
    # Get datetime interval for the past hour
    dt_save, datetime_start, datetime_end = getPastHourInterval()
    main(args, dt_save, datetime_start, datetime_end)

    """
    # backfill S3 mode
    start_time = datetime.utcnow() - timedelta(hours=240) # ten day backfill
    end_time = datetime.utcnow() - timedelta(hours=3)
    dt_save = start_time

    hourly_intervals = getHourlyIntervals(start_time, end_time)

    for i in range(len(hourly_intervals) - 1):
        main(args, dt_save, hourly_intervals[i], hourly_intervals[i+1])
        dt_save = dt_save + timedelta(hours=1)
    """

    sys.exit(0)
