# Dolan Clahan DataBase Final Project:
### EDA for Movie Trend Analytics

## Introduction
Enterprises increasingly rely on data-driven decisions, blending structured and unstructured data to uncover insights and improve user experience. This project proposes developing an **Enterprise Data Architecture (EDA)** to analyze movie trends using structured data from **The Movie Database (TMDB)** and unstructured data from social media platforms such as **Twitter** and **Reddit**. The goal is to create an automated analytics pipeline that identifies popular and trending movies based on TMDB statistics and social media mentions.

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

## Data dictionary
Data Lake architecture on AWS Simple Storage Service (S3) is used to store both raw reddit post and TMDB data and also analytics tables described below. Apache Spark is used for ETL jobs.

`post_clean` fact table:
| Column | Type | Description |
| ------ | ---- | ----------- |
| `id` | `long` | Movie id | 
| `date` | `date` | UTC date of posts |
| `subreddit` | `string` | Subreddit of post that mentions movie |
| `text_count` | `long` | Number of posts whose text include the movie name |
| `post_count` | `long` | Number of posts which mentions the movie either in title or body |
 
 `tmdb_clean` fact table:
| Column | Type | Description |
| ------ | ---- | ----------- |
| `id` | `long` | Movie id | 
| `date` | `date` | UTC date |
| `vote_count` | `long` | Number of votes for the movie on TMDB website |
| `vote_average` | `double` | Average vote for the movie on TMDB website |
| `popularity` | `double` | Popularity of the movie on TMDB website |

 `date` dimention table:
| Column | Type | Description |
| ------ | ---- | ----------- |
| `date` | `date` | UTC date | 
| `weekday` | `int` | Day of the week starting from 1 (Monday) |
| `day` | `int` | Day of the month |
| `month` | `int` | Month |
| `year` | `int` | Year |

 `mov` dimention table:
| Column | Type | Description |
| ------ | ---- | ----------- |
| `id` | `long` | Movie id | 
| `name` | `string` | Name of the movie |
| `original_name` | `string` | Original name of the movie |
| `original_language` | `string` | Original language of the movie |
| `genres_id` | `array<int>` | Array of genre id which the movie corresponds to |
| `production_companies_id` | `array<int>` | Array of production company id which the movie corresponds to |

 `genres` table:
| Column | Type | Description |
| ------ | ---- | ----------- |
| `id` | `int` | Genre id | 
| `name` | `string` | Genre name |

 `production_companies` table:
| Column | Type | Description |
| ------ | ---- | ----------- |
| `id` | `int` | Network id | 
| `name` | `string` | Network name |
| `origin_country` | `string` | Network's country of origin |


## Data Pipeline

Using AWS products to stream data into S3 buckets, and then perform ETL on that data.

![image](/pics/workflow.jpg)

The data pipeline for this project involves automating data collection, processing, and transformation into a format suitable for analysis and visualization. The following components and tools were used:

1. **Data Streaming and Collection**: The data streamer fetches raw data from Reddit and the TMDB API. Linux cron jobs running on an Amazon Lightsail instance manage this streaming process. The raw data is periodically uploaded to an Amazon S3 bucket for storage.

2. **ETL (Extract, Transform, Load) Pipeline**: The ETL process was implemented using Amazon Glue services. The key steps include:
   - **Extraction**: Raw Reddit posts and TMDB data are extracted from the S3 bucket.
   - **Data Cleaning**: Corrupt or incorrectly stored values are identified and corrected. This step ensures the integrity and consistency of the data.
   - **Data Transformation**: The cleaned data is structured into fact tables and dimension tables for tracking daily movie mentions on Reddit, as well as populatrity metrics from TMDB. 
   
3. **Data Upload**: The transformed data, now conforming to the Entity-Relationship Diagram (ERD) outlined earlier, is uploaded back to the S3 bucket. This clean data is accessible via the S3 Web client and serves as the backend for the application, which visualizes daily movie trends from Reddit posts and TMDB popularity metrics.

## Data Visualization Dashboard

The application is built using the Bokeh library and Python. The Bokeh library provides a flexible and interactive visualization framework for creating interactive and dynamic data visualizations. Given more time, I would have liked to be able to host the application on a cloud platform like AWS or GCP, but I was unable to find a suitable solution within the time frame. 

The application is designed to visualize the daily trends for each movie mentioned in a Reddit post over the past week. The implementation results in a dashboard that displays the number of mentions, popularity, and a ranking of the top movies mentioned in the past week. The dashboard is interactive and allows users to select a movie and a date range to view the trends for that movie.

|![image](/pics/dashboard.png)|
|*--*|
|Dashboard View of Movie Trends|

|![image](/pics/wicked_stats.png)|
|*--*|
|Dashboard View of Movie Trends For the movie "Wicked", showing an increase in mentions and popularity over time.|

|![image](/pics/total_mentions.png)|
|*--*|
|Bar Chart showing the total number of mentions for each movie in the past week.|

|![image](/pics/top_20_mentions.png)|
|*--*|
|Summary of the top 20 movies mentioned in the past week.|


## Business Applications

The project has the potential to generate significant value for businesses in various ways:

**Market Analysis for Movie Studios**:  
   - Track real-time and historical movie discussions and sentiment on social platforms like Reddit.  
   - Provide insights into audience reactions, enabling better marketing and release strategies.  

**Audience Sentiment Insights**:  
   - Deliver sentiment analyses for movies, helping studios understand public perception post-release or during promotional campaigns.  
   - Identify key pain points or praise-worthy elements that can influence sequels or spinoffs.  

**Competitive Analysis for Streaming Platforms**:  
   - Monitor trending movies and topics to curate or prioritize popular content on platforms like Netflix, Amazon Prime, or Hulu.  

**Advertising and Sponsorship Opportunities**:  
   - Help brands identify movies or genres with rising popularity to target their sponsorships or ads for maximum reach.  

**Investment Insights**:  
   - Provide data for private equity firms or investors to gauge potential success of upcoming films, franchises, or production houses based on popularity trends.  

**Event Timing Optimization**:  
   - Help cinemas, festivals, or streaming services plan events, premieres, or promotions based on trending movie discussions.  

**Content Creation Guidance**:  
   - Support writers, bloggers, and influencers with data on trending movies or themes to tailor their content for maximum engagement.  


## Machine Learning Implementations

The project opens avenues for machine learning applications to derive deeper insights and enhance functionality:

**Sentiment Analysis**:
   - Implementing a BERT tokenizer or similar NLP models to classify the sentiment (positive, negative, or neutral) of Reddit posts mentioning movies. This data can provide a sentiment-driven popularity index for each movie.

**Trend Prediction**:
   - Utilizing time-series analysis to predict future trends in movie discussions on Reddit. This could involve models like ARIMA, LSTMs, or Prophet for forecasting.

**Topic Modeling**:
   - Applying topic modeling techniques such as Latent Dirichlet Allocation (LDA) to identify recurring themes in Reddit posts about movies, e.g., genre-specific trends or discussions about directors and actors.

## Future Directions

The current implementation is limited by the financial constraints of accessing premium data sources. Specifically:

**Integration of Twitter Data**:
   - Accessing Twitter’s premium API tier would allow for streaming larger volumes of tweets. This would significantly expand the dataset and improve trend analysis by integrating discussions from another major social media platform.

**Additional Data Sources**:
   - Incorporating data from other platforms such as YouTube, Letterboxd, or IMDb to enhance the analysis and provide a multi-dimensional view of movie popularity. This would require additional web scraping or API integrations. 

**Real-Time Processing**:
   - Transitioning to real-time data ingestion and processing pipelines using tools like Apache Kafka and Spark Streaming to provide up-to-the-minute insights.

**Advanced Analytics Dashboards**:
   - Developing more sophisticated visualizations and dashboards to display movie trends, sentiment analyses, and predictive insights in an interactive and user-friendly format.

<!-- 
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
 - [ ] Bert analize text (for business purposes)
### PT 4
 - [ ] make analytics tables, trends graphs
 - [ ] host live updates (iffy)
-->