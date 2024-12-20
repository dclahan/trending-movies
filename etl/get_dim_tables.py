import os
import logging
import argparse
import configparser
import pandas as pd
from datetime import date, timedelta
from tmdbv3api import TMDb, Movie

def initTmdb(api_key):
    """ Returns a TMDB object initialized with the given api key

    Args:
    api_key (str) : TMDB api key

    Returns:
    tmdb : TMDB object
    """
    tmdb = TMDb()
    tmdb.api_key = api_key
    tmdb.language = 'en'
    return tmdb

def getMovieShowsId(path):
    """ Read Movie  data (id, name, hashtag) and returns their id as list

    Args:
    path (str): path for Movie CSV file

    Returns:
    movies_id (int list): list of movies id
    """
    # Read the Movie  CSV file
    df_mov = pd.read_csv(path)
    # Get the ids of movies
    movies_id = df_mov['id'].tolist()
    logging.info('{} Movie are defined for searching'.format(len(movies_id)))
    logging.info('Movie id:')
    logging.info(movies_id)
    return movies_id

def get_dim_tmdb(movies_id):
    """ Queries Movie details from TMDB and
        extracts genres, production_companies and movie data

    Args:
    movies_id (int list): list of movies id

    Returns:
    df_genre (dataframe) : genre id and name
    df_prod_co (dataframe) : prod_co id, name and origin country
    df_movie (dataframe) : Movie id, name, original name, genre ids and prod_co ids
    """
    # Create the Movie object
    mov = Movie()

    # Retrieve raw Movie data
    movies_raw = []
    for movie_id in movies_id:
        movie_detail = mov.details(movie_id)
        movies_raw.append(movie_detail.__dict__)

    # Define the columns
    genre_columns = ['id', 'name']
    prod_co_columns = ['id', 'name', 'origin_country']
    movie_columns = ['id', 'name', 'original_name', 'original_language', 'genres_id', 'production_companies_id']

    # Initialize the dataframes
    df_genre = pd.DataFrame(columns=genre_columns)
    df_prod_co = pd.DataFrame(columns=prod_co_columns)
    df_movie = pd.DataFrame(columns=movie_columns)

    # Extract from the raw data
    for movie_data in movies_raw:
        # Genres
        for genre in movie_data.get('genres', []):
            genre_id, genre_name = int(genre['id']), genre['name']
            if not ((df_genre['id'] == genre_id) & (df_genre['name'] == genre_name)).any():
                df_genre.loc[len(df_genre),:] = genre_id, genre_name
        # production_companies
        for prod_co in movie_data.get('production_companies', []):
            prod_co_id, prod_co_name, prod_co_country = int(prod_co['id']), prod_co['name'], prod_co['origin_country']
            if not ((df_prod_co['id'] == prod_co_id) & (df_prod_co['name'] == prod_co_name)).any():
                df_prod_co.loc[len(df_prod_co),:] = prod_co_id, prod_co_name, prod_co_country
        # Movie
        genres_id = []
        for genre in movie_data.get('genres', []):
            genres_id.append(int(genre['id']))
        production_companies_id = []
        for prod_co in movie_data.get('production_companies', []):
            production_companies_id.append(int(prod_co['id']))
        df_movie.loc[len(df_movie),:] = [int(movie_data['id']), movie_data['title'],
            movie_data['original_title'], movie_data['original_language'], genres_id , production_companies_id]

    return df_genre, df_prod_co, df_movie

def get_dim_date(date = date.today(), num_years = 1):
    """ Creates the date dataframe

    Args:
    date (datetime.date): input date on which starting year is determined
    num_years (int) : number of years from the start year

    Returns:
    df_date (dataframe) : with columns [ date (YYYY-MM-DD format),
                            weekday, day, month, year ]
    """
    # Define columns and initialize the dataframe
    date_columns = ['date', 'weekday', 'day', 'month', 'year']
    df_date = pd.DataFrame(columns=date_columns)

    # Iterate given number of years from the start date
    date_start = date.replace(month=1, day=1)
    for i in range(num_years*365):
        date_cur = date_start + timedelta(days=i)
        df_date.loc[i,:] = [date_cur.strftime("%Y-%m-%d"), date_cur.isoweekday(),
                                date_cur.day, date_cur.month, date_cur.year]

    return df_date

def save_dim_tables(df_list, filenames, path):
    """ Saves the dataframes as CSV files

    Args:
    df_list : list of dataframes
    filenames : list of filenames
    path (str) : directory path in which dataframes are saved

    Returns:
    boolean to denote success or failure
    """
    # Check if the given path is a directory
    if not os.path.isdir(path):
        logging.error('The output path {} is not a directory'.format(path))
        return False

    # Check the number of dataframes and filenames
    if len(df_list) != len(filenames):
        logging.error('The number of dataframes and filenames do not match')
        return False

    # Save the dataframes as CSV files
    try:
        for i, filename in enumerate(filenames):
            path_save = os.path.join(path, filename)
            df_list[i].to_csv(path_save, header=True, index=False)
    except Exception as e:
        logging.error(str(e))
        return False

    return True

if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Upload TMDB Movie data to S3",
        add_help=True
    )
    parser.add_argument("path_config", type=str,
                        help="Path to configuration file with API credentials")
    parser.add_argument("path_movie", type=str,
                        help="Path to the CSV file with Movie information")
    parser.add_argument("--path_output", type=str, default=os.getcwd(),
                        help="Path to save the dimention tables")
    args = parser.parse_args()

    # Setup the logger
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s: %(message)s')
    logging.info('---------------- Logger started ----------------')

    # Read the API configuration file
    config = configparser.ConfigParser()
    config.read(args.path_config)

    # Initialize TMDB object
    tmdb = initTmdb(config.get('TMDB','API_KEY'))

    # Get the Movie  id
    movies_id = getMovieShowsId(args.path_movie)

    # Dimention tables from TMDB raw Movie data
    df_genre, df_prod_co, df_movie = get_dim_tmdb(movies_id)

    # Date dimention
    df_date = get_dim_date()

    # Save the dimention tables
    if save_dim_tables([df_genre, df_prod_co, df_movie, df_date],
                        ['genres.csv', 'production_companies.csv', 'mov.csv', 'date.csv'],
                        args.path_output):
        logging.info('The dimention tables are saved successfully')
    else:
        logging.error('Problem occured while saving the dimention tables')