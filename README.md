# Dolan Clahan DataBase Final Project:
### EDA for Movie Trend Analytics

## Introduction
Enterprises increasingly rely on data-driven decisions, blending structured and unstructured data to uncover insights and improve user experience. This project proposes developing an Enterprise Data Architecture (EDA) to analyze movie trends using structured data from The Movie Database (TMDB) and unstructured data from social media platforms such as Twitter and Reddit. The goal is to create an automated analytics pipeline that identifies popular and trending movies based on TMDB statistics and social media mentions.

This project aims to develop an EDA that can provide actionable insights through daily analytics updates. It addresses the challenges of data quality, governance, and integration, demonstrating the value of combining diverse data sources to uncover trends and enhance decision-making. The solution is designed for businesses, movie production houses, and streaming platforms seeking to track user engagement and leverage analytics for marketing and audience engagement strategies. Applicable in real-time scenarios, such as promoting upcoming releases or optimizing content recommendations, this solution has global applicability, particularly in the entertainment industry where understanding audience preferences is crucial.

## Data Sources
Structured Data (TMDB):
 - Movie metadata (titles, genres, release dates).
 - Popularity metrics and vote statistics.
Unstructured Data (Twitter and Reddit):
 - Tweets and Reddit posts mentioning movies (hashtags, keywords, sentiment).

## Entity Relationship Diagram (ERD)
The database will consist of a main dimension table of movies, having a child table of genre ids to be able to find information on multi-genred movies. There are three fact tables that have a primary foreign key of movie_ids, and primary keys of dates. The fact tables are designed to represent the number of mentions of a movie on respected social media platforms per day.

![image](/pics/ERD-tmdb.png) 
<!-- include abs path https://github.com/dclahan/.... -->

## Data Streaming
### Reddit object
The Reddit object of Reddit API has a complex nested JSON structure

Simplified view with important fields:

`{
    "title": "Hildur Gudnadottir (\“Chernobyl\”, \“Joker\”) to Score \“28 Years Later II: The Bone Temple\”", 
    "selftext": "Looks like my goat is back at it again on the fire scores", 
    "subreddit_name_prefixed": "r/movies", 
    "upvote_ratio": 0.92,    “id” : “1hbf5xs”,   “created_utc” : 1734045174
}`

### TMDB object
Movie details are provided as JSON from TMDB API and tmdbv3api serves this data as movie details object. This Python object is reverted back to its raw JSON format and then saved.

The most important fields for our purpose:

`{
    "title": "12 Angry Men",  "genre_ids": [18 ],   "id": 389,
    "popularity": 47.734,  "poster_path": "/ow3wq89wM8qd5X7hWKxiRfsFf9C.jpg",
    "release_date": "1957-04-10",   "vote_average": 8.5,   "vote_count": 8699
}`

## Data Pipeline

Using AWS products to stream data into S3 buckets, and then perform ETL on that data.

![image](/pics/workflow.jpg)


glue stuff
s3 stuff
document!!!
airflow cron jobs
DOCUMENT!!!  



# TODO:
- [ ] update resume with all this exciting new stuff and systems and products ur using
### PT2
 - [x] finish streaming
 - [x] connect to S3
 - [x] backfill s3 with data collected so far (Generate datetime intervals of one hour for the past since the 9th or 10th I guess)
 - [x] tmdb stream to s3
 - [x] cron jobs (LIGHTSAIL) 
    - Want to run redditStreamer and then tmdb/redditUploadtoS3 every hour every day (for now - remember to stop that)
 - [x] ETL get cookin (to make db like ERD)
 - [~] aws glue get it happening
### PT 3
 - [ ] Bert analize text (for business purposes??)
### PT 4
 - [ ] make analytics tables, trends graphs
 - [ ] host live updates (iffy)
