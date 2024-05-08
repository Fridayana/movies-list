# movie-pipeline

A workflow management for ETL processing using python programming

## Requirements

Python `>=` 3.9

Add dependencies:

* Opening terminal and execute:
  `cd movie_pipeline`
  `pip3 install -r requirements.txt`

## Configuration

There is a file called app.properties in the project root directory which contains config of your database credentials.

## HOW TO RUN

````
$ cd movie_pipeline
$ python jobs/import_movie_data_to_postgres.py