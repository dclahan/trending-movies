import pandas as pd
import boto3
from bokeh.io import curdoc
from bokeh.layouts import column, row
from bokeh.models import Select, ColumnDataSource, DateRangeSlider
from bokeh.plotting import figure
import argparse
import configparser

import sys
import pyarrow.parquet as pq
import pyarrow as pa
import s3fs

# Replace with your S3 bucket name and prefix
BUCKET_NAME = "trending-movies-clahad"
PREFIX = "data/movie_trends_database/posts_clean/posts_clean/"

# Load posts_clean data from S3 by accessing the parquet files
def load_posts_clean_data(start_date, end_date,
                    aws_access_key_id, aws_secret_access_key, region_name):
    # Connect to S3
    s3 = s3fs.S3FileSystem( anon=False,
                            key=aws_access_key_id,
                            secret=aws_secret_access_key,
                            client_kwargs={'region_name': region_name}
                            )
    # Generate list of dates within the range
    date_range = pd.date_range(start=start_date, end=end_date)
    posts_clean_data = []

    for single_date in date_range:
        year = single_date.year
        month = single_date.month
        day = single_date.day
        key = f"{PREFIX}year={year}/month={month:02d}/day={day:02d}/post_stats.parquet"

        try:
            base_pya_dataset = pq.ParquetDataset(BUCKET_NAME+'/'+key, filesystem=s3)
            posts_clean_data.append(base_pya_dataset.read_pandas().to_pandas())
        except Exception as e:
            print(e)
            print(f"Data not found for date: {single_date}")

    if posts_clean_data:
        return pd.concat(posts_clean_data, ignore_index=True)
    else:
        return pd.DataFrame()

# Load other data from S3
def load_data(aws_access_key_id, aws_secret_access_key, region_name):
    # Connect to S3
    s3_client = boto3.client('s3',
                            aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key,
                            region_name=region_name
    )
    s3 = s3fs.S3FileSystem( anon=False,
                            key=aws_access_key_id,
                            secret=aws_secret_access_key,
                            client_kwargs={'region_name': region_name}
                            )
    def read_csv_from_s3(key):
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
        return pd.read_csv(obj["Body"])
    def read_parquet_from_s3(key):
        # schema = pa.schema([('SCORE_MTH', pa.string()), ('ACCOUNT_ID', pa.int32())])
        base_pya_dataset = pq.ParquetDataset(BUCKET_NAME+'/'+key, filesystem=s3)
        return base_pya_dataset.read_pandas().to_pandas()

    tmdb_clean = read_parquet_from_s3(f"data/movie_trends_database/posts_clean/tmdb_clean/year=2024/month=12/day=18/data.parquet")
    date_dim = read_csv_from_s3(f"data/movie_trends_database/date_csv/dataload=20241218/date.csv")
    mov_dim = read_csv_from_s3(f"data/movie_trends_database/mov_csv/dataload=20241218/mov.csv")
    genres = read_csv_from_s3(f"data/movie_trends_database/genres_csv/dataload=20241218/genres.csv")
    prod_companies = read_csv_from_s3(f"data/movie_trends_database/production_companies_csv/dataload=20241218/production_companies.csv")

    return tmdb_clean, date_dim, mov_dim, genres, prod_companies


# Parse arguments
parser = argparse.ArgumentParser(
    description="Upload TMDB movie data to S3",
    add_help=True
)
parser.add_argument("path_config", type=str,
                    help="Path to configuration file with API credentials")
args = parser.parse_args()

# Read the API configuration file
config = configparser.ConfigParser()
config.read(args.path_config)


# Specify the date range for the posts_clean data
start_date = "2024-12-08"
end_date = "2024-12-17"

# Load data
posts_clean = load_posts_clean_data(start_date, end_date,
                                    config.get('AWS','ACCESS_KEY_ID'),
                                    config.get('AWS','SECRET_ACCESS_KEY'),
                                    config.get('AWS','REGION')
                                    )
tmdb_clean, date_dim, mov_dim, genres, prod_companies = load_data(
                                                                config.get('AWS','ACCESS_KEY_ID'),
                                                                config.get('AWS','SECRET_ACCESS_KEY'),
                                                                config.get('AWS','REGION')
                                                                )


posts_clean['date_post'] = pd.to_datetime(posts_clean[['year', 'month', 'day']]).dt.date # maybe not needed
posts_clean['date_string'] = pd.to_datetime(posts_clean['date_post']).dt.strftime('%Y-%m-%d')
date_dim['date_string'] = pd.to_datetime(date_dim['date']).dt.strftime('%Y-%m-%d')

# Merge data for visualization
merged_data = pd.merge(posts_clean, tmdb_clean, on=["id"], how="inner")
merged_data = pd.merge(merged_data, date_dim, on="date_string", how="inner")
merged_data = pd.merge(merged_data, mov_dim, on="id", how="inner")

# Convert date column to datetime format
merged_data["date_string"] = pd.to_datetime(merged_data["date_string"])

# Create ColumnDataSource
data_source = ColumnDataSource(merged_data)

# Create plots
mention_plot = figure(title="Number of Mentions", x_axis_type="datetime", height=400, width=800)
mention_plot.line(x="date_string", y="mention_count", source=data_source, line_width=2, color="blue", legend_label="Text Count")
mention_plot.legend.click_policy = "hide"

popularity_plot = figure(title="Movie Popularity and Average Votes", x_axis_type="datetime", height=400, width=800)
popularity_plot.line(x="date_string", y="popularity", source=data_source, line_width=2, color="purple", legend_label="Popularity")
popularity_plot.line(x="date_string", y="vote_average", source=data_source, line_width=2, color="orange", legend_label="Average Votes")
popularity_plot.legend.click_policy = "hide"


total_ments = merged_data.groupby('name')['mention_count'].sum().sort_values(ascending=True)
real_ments = total_ments[:-3]
# print(real_ments[-20:].sort_values(ascending=False))
ments_source = ColumnDataSource(data=dict(name=real_ments.index.tolist(), mention_count=real_ments.values.tolist()))

bar = figure(x_range=ments_source.data['name'], height=350, title="number of mentions",
           toolbar_location=None, tools="")
bar.vbar(x=ments_source.data['name'], top=ments_source.data['mention_count'], width=0.9, color="#998ec3")
bar.xgrid.grid_line_color = None
bar.y_range.start = 0

# Widgets
movie_select = Select(title="Select Movie:", value="All", options=["All"] + merged_data["name"].unique().tolist())
date_slider = DateRangeSlider(title="Select Date Range:", start=min(merged_data["date_string"]),
                            end=max(merged_data["date_string"]),
                            value=(min(merged_data["date_string"]), max(merged_data["date_string"])))

# Update function
def update(attr, old, new):
    selected_movie = movie_select.value
    start_date, end_date = date_slider.value_as_datetime

    filtered_data = merged_data[(pd.to_datetime(merged_data["date_string"]).dt.date >= start_date.date()) & (pd.to_datetime(merged_data["date_string"]).dt.date <= end_date.date())]
    if selected_movie != "All":
        filtered_data = filtered_data[filtered_data["name"] == selected_movie]

    data_source.data = ColumnDataSource.from_df(filtered_data)

movie_select.on_change("value", update)
date_slider.on_change("value", update)

# Layout
layout = column(
    row(movie_select, date_slider),
    mention_plot,
    popularity_plot,
    bar
)

curdoc().add_root(layout)
curdoc().title = "Movie Mentions Dashboard"

# run the app using the following command:
# bokeh serve --show bokeh_script.py --args <path-to-dir>/trending-movies/api.cfg
