
![elephant](https://user-images.githubusercontent.com/63891089/194710009-f2e4fc55-c39e-4dff-b468-a1e97bfe7ab9.png)

# Project Description
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# Project Template
To get started with the project, go to the workspace on the next page, where you'll find the project template files. You can work on your project and submit your work through this workspace. Alternatively, you can download the project template files from the Resources folder if you'd like to develop your project locally.

In addition to the data files, the project workspace includes six files:

- `test.ipynb` displays the first few rows of each table to let you check your database.
- `create_tables.py` drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
- `etl.ipynb` reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
- `etl.py` reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.
- `sql_queries.py` contains all your sql queries, and is imported into the last three files above.
- `README.md` provides discussion on your project.

# Module Used
- os
- glob
- psycopg2
- pandas

# Schema for Song Play Analysis
## Fact Table
- songplays = records in log data associated with song plays i.e. records with page NextSong: 
-       songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
## Dimension Tables
- users = users in the app
        user_id, first_name, last_name, gender, level
- songs = songs in music database
        song_id, title, artist_id, year, duration
- artists = artists in music database
        artist_id, name, location, latitude, longitude
- time = timestamps of records in songplays broken down into specific units
        start_time, hour, day, week, month, year, weekday

# Star Schema
![ww](https://user-images.githubusercontent.com/63891089/194772060-44c6103d-1732-4934-b566-1e242141e590.jpg)


# NOTE: You will not be able to run `test.ipynb`, `etl.ipynb`, or `etl.py` until you have run `create_tables.py` at least once to create the sparkifydb database, which these other files connect to.
