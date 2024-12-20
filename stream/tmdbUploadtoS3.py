import os
import sys
import logging
import argparse
import configparser
import json
import time
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from tmdbv3api import TMDb, Movie, Discover
from datetime import date, datetime
from settings import dirTmdb, dirLogs, s3_bucket, s3_key_tmdb, s3_upload_try
from utils import createDir, setupLogger, uploadFileToS3

def initTmdb(api_key):
    """
    Returns a TMDB object initialized with the given api key

    Args:
    api_key (str) : TMDB api key

    Returns:
    tmdb : TMDB object
    """
    tmdb = TMDb()
    tmdb.api_key = api_key
    tmdb.language = 'en'
    return tmdb

def getMovieIds(path):
    """
    Read movie data (id, name, indentifier) and returns their id as list

    Args:
    path (str): path for movie CSV file

    Returns:
    movie_ids (int list): list of movie ids
    """
    # Read the movies CSV file
    df_movie = pd.read_csv(path)
    # Get the id
    movie_ids = df_movie['id'].tolist()
    logging.info('{} movies are defined for searching'.format(len(movie_ids)))
    logging.info('movie ids:')
    logging.info(movie_ids)
    return movie_ids

def queryMovieFromTmdb(movie_ids):
    """
    Collect current movie data from tmdb api

    Args:
    movie_ids (int list): list of movie ids

    Returns:
    movies_data (str list): list of each movie raw data in JSON format
    """
    # Create a Movie object
    # Iterate over each movie and get their details
    movies_data = []
    for id in movie_ids:
        try:
            movie = Movie()
            movie = movie.details(id, append_to_response = "")
            movie_dict = movie.__dict__
            query_datetime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            movie_dict['query_datetime'] = query_datetime
            for k,v in movie_dict.items():
                if type(v) not in [int, float, str, bool, list, dict, tuple, type(None)]:
                    try:
                        movie_dict[k] = v._json
                    except:
                        movie_dict[k] = None
        except Exception as e:
            logging.error(str(e))
            logging.error('movie id={} could not be queried properly'.format(id))
        else:
            movies_data.append(json.dumps(movie_dict))
    # Check how many movies are successfully queried
    if len(movies_data) == len(movie_ids):
        logging.info('All {} movies are queried successfully'.format(len(movies_data)))
    else:
        logging.warning('{}/{} movies are queried successfully'.format(len(movies_data),len(movie_ids)))
    return movies_data

def getLocalPartitionedPath(dir_save, dt_save):
    """
    Returns a partitioned directory and path based on the given datetime

    Args:
    dir_save (str) : the local directory for saving
    dt_save (datetime) : datetime used for directory setup

    Returns:
    local_dir (str) : dir_save/<year>/<month>/<day>
    local_path (str) : dir_save/<year>/<month>/<day>/<year-month-day>.json
    """
    year, month, day = dt_save.strftime("%Y-%m-%d").split('-')
    local_dir = os.path.join(dir_save, year, month, day)
    local_path = os.path.join(local_dir, '{}-{}-{}.json'.format(year, month, day))
    logging.info('Local path to save raw data: {}'.format(local_path))
    return local_dir, local_path

def getS3PartitionedPath(key_prefix, dt_save):
    """
    Returns a partitioned S3 path based on the given datetime

    Args:
    key_prefix (str) : key prefix on S3
    dt_save (datetime) : datetime used for directory setup

    Returns:
    s3_path (str) : key_prefix/<year>/<month>/<day>/<year-month-day>.json
    """
    year, month, day = dt_save.strftime("%Y-%m-%d").split('-')
    s3_path = '{}/{}/{}/{}/{}-{}-{}.json'.format(
        key_prefix, year, month, day, year, month, day
    )
    logging.info('S3 key for upload: {}'.format(s3_path))
    return s3_path

def saveTmdbLocal(movies_data, local_path):
    """
    Saves raw tmdb movie data locally using the given path

    Args:
    movies_data (str list) : raw tmdb movie data
    local_path (str) : path to save locally

    Returns:
    boolean : whether the file saving is successful or not
    """
    logging.info('Writing tmdb movies data into {}'.format(local_path))
    isSuccess = False
    try:
        fp = open(local_path, 'w')
        for movie_data in movies_data:
            fp.write('{}\n'.format(movie_data))
    except Exception as e:
        logging.error(str(e))
        logging.error('Error while writing tmdb movies data to the file')
    else:
        isSuccess = True
        logging.info('Tmdb movies data are successfully saved to the file')
    finally:
        if fp:
            fp.close()
    return isSuccess

if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Upload TMDB movie data to S3",
        add_help=True
    )
    parser.add_argument("path_config", type=str,
                        help="Path to configuration file with API credentials")
    parser.add_argument("path_movie", type=str,
                        help="Path to the CSV file with movie information")
    args = parser.parse_args()

    # Create the required directories if not exits
    if not createDir(dirLogs):
        sys.exit('The directory "{}" could not be created'.format(dirLogs))
    if not createDir(dirTmdb):
        sys.exit('The directory "{}" could not be created'.format(dirTmdb))

    # Setup the logger
    logName = date.today().strftime("%Y-%m-%d") + '-tmdb-upload.log'
    setupLogger(dirLogs, logName)

    # Read the API configuration file
    config = configparser.ConfigParser()
    config.read(args.path_config)

    # Initialize TMDB object
    tmdb = initTmdb(config.get('TMDB','API_KEY'))

    # Get the movie ids
    movie_ids = getMovieIds(args.path_movie)

    # Query each movie details
    movies_data = queryMovieFromTmdb(movie_ids)
    if len(movies_data) == 0:
        logging.error('Could not get any movies detail from TMDB')
        sys.exit(1)

    # Local directory and path to save
    dt_cur = datetime.utcnow()
    local_dir, local_path = getLocalPartitionedPath(dirTmdb, dt_cur)

    # Create the local directory for saving
    if not createDir(local_dir):
        logging.error('The directory "{}" could not be created')
        sys.exit(1)

    # Save the file locally
    if not saveTmdbLocal(movies_data, local_path):
        logging.error('The tweet could not be saved to local, will not upload to S3')
        sys.exit(1)

    # Get S3 path to transfer the file
    s3_path = getS3PartitionedPath(s3_key_tmdb, dt_cur)

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

    sys.exit(0)