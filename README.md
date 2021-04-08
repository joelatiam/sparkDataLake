# Sparkify's DATA LAKE with SPARK & AWS  S3

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

>The **ETL pipeline** extracts data from **S3**, processes them using **Spark**  then load back into  **S3** as a set of **dimensional tables** then transform and load them to **final tables** in the same Redshift schema.
### Usage

```sh
$ #cd to the project home directory/folder
$ pip install -r requirements.txt
$ python3 etl.py
```

### Directory(Folder) Structure

| File/Folder | Description |
| ------ | ------ |
| `dl.cfg` | Configuration file containing our AWS credentials and S3 files path. |
| etl.py | A module containing all our **ETL** functions |
| requirements.txt | A File that lists phython modules needed for our project|
| .gitignore | A file containing directories and files names to be ignored by git |
| README.md | Project Documentation   |


### Database Schema

#### Processed Data
Processed data are stored in parket format

##### Dimension Tables
-  **users**    (*`output path`/users*)

| Column | Datatype | Constrainct | Comment |
| ------ | ------ |------ | ------ |
| user_id | LongType || This is a unique key representing each user and we opted for an LongType type because we are able to extract as a non decimal number format from the log file and as the number of user grows, LongType will be the right  Datatype|
| first_name | StringType | |  |
| last_name | StringType | |  |
| gender | StringType | |  |
| level | StringType | |  |

-  **songs**    (*`output path`/songs*  `partitioned by year and artist id`)

| Column | Datatype | Constrainct | Comment |
| ------ | ------ |------ | ------ |
| song_id | StringType || This is a unique key representing each song and we opted for a StringType type because we are receiving the song ID as a string of different alphabetic characters from the song metadata |
| title | StringType | |  |
| artist_id | StringType |  |  |
| year | ShortType | |  |
| duration | DoubleType | |  |

-  **artists**  (*`output path`/artists*)

| Column | Datatype | Constrainct | Comment |
| ------ | ------ |------ | ------ |
| artist_id | StringType | | This is a unique key representing each artist and we opted for a StringType type because we are receiving the artist ID as a string of different alphabetic characters from the song metadata |
| name | StringType | |  |
| location | StringType | | |
| latitude | DoubleType | |  |
| longitude | DoubleType | |  |

-  **time**  (*`output path`/time*  `partitioned by year and month`)

| Column | Datatype | Constrainct | Comment |
| ------ | ------ |------ | ------ |
| start_time | IntegerType | | This is a  key representing each recorded time in unix timestamp format|
| hour | ShortType | |  |
| day | ShortType | | |
| week | ShortType | |  |
| month | ShortType | |  |
| year | ShortType | |  |
| weekday | ShortType | |  |

##### Fact Tables
-  **songplays**    (*`output path`/songplays*  `partitioned by year and month`)

| Column | Datatype | Constrainct | Comment |
| ------ | ------ |------ | ------ |
| songplay_id | LongType | | This is an auto increament key representing each recorded songplay, LongType type helps us to care less about the quantity of songplay records we will store as the platform grows  |
| start_time | IntegerType | |  |
| user_id | LongType | |  |
| level | StringType | | |
| song_id | StringType | | the song id stored in artists table is of StringType data type |
| artist_id | StringType || the artist id stored in artists table is of StringType data type |
| session_id | StringType | |  |
| location | StringType | |  |
| user_agent | StringType | |  |

### ETL pipeline
The ELT processes are operated using **Dataframe** functions of **Spark** as follow :
-   Read **json** data from AWS **S3**
-   Process them and store them back to S3 in **parquet** format
-   To ensure that data are not duplicated, they are saved in **overwrite** mode

The process is used by runing **etl.py** which calls functions `process_song_data` extract data from the song logs and `process_log_data` to extract data from the song play log

##### Process Songs Data (*`source path`/song_data/*/*/*/*.json*)

Songs Data are retrieved from the `song_data` directory wich contains songs data data in json format stored in Amazon S3.

Data extracted from `song_data` table which are `songs` and `artists` are described in the **Database Schema** section.

-  **songs**
>Songs data consist of
>`[song_id, title, artist_id, year, duration]` extracted from `song_data` logs, 
>After Data extraction and columns rename songs are saved by being partitioned by `year` and `artist_id`
-  **artists**
>Data related to artists are
>`[artist_id, artist_name, artist_location, artist_latitude, artist_longitude]` which are extracted from `song_data`

##### Process Log/Events Files

Events or Logs Data are retrieved from the `log_data` directory wich contains users logs data data in json format stored in Amazon S3.

Data extracted from `log_data` table which are `time`, `users` and `songplay` are described in the **Database Schema** section.

-  **time**
>Time records consist of `[start_time, hour, day, week, month, year, weekday]` 
> Using a `pyspark.sql.functions` `udf` function that we name `get_timestamp`, we convert values of `ts` column to a `timestamp` then we define another function named `get_datetime` which helps us to get the datetime format from the timestamp from there, we're able to extract the year, month, week, ... values
> We store time data by partitioning by `year` and `month`

-  **users**
>Data related to users are
>`[artist_id, artist_name, artist_location, artist_latitude, artist_longitude]` which are extracted from `staging_songs`, 
>We store users record after extracting users related columns and renaming some of them

-  **songplay** 
>Songplay records consist ofb`[songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent, year, month]`, to get data found in the `song_data`, we first read a song dataframe the **left join** it to our log dataframe by matching the song title, artist name and song length
>We use the Spark's `monotonically_increasing_id` function to generate Ids for our songplay records
>Then we save the records by partitioning by `year` and `month`
### Technologies/Tools

Following are different technologies or tools used in this project :

* [PySpark](https://spark.apache.org/docs/latest/api/python/index.html) - An interface for Apache Spark in Python
* [Python 3](https://www.python.org/) - Awesome programming language, easy to learn and with awesome data science libraries and communities
* [AWS S3](https://aws.amazon.com/s3/) -  A service offered by Amazon Web Services that provides object storage through a web service interface
* [psycopg2](https://www.psycopg.org/) - Popular PostgreSQL database adapter for the Python programming language
* [configparser](https://docs.python.org/3/library/configparser.html) - Helps to work with configuration files

### Contributors
**Joël Atiamutu** *[github](https://github.com/joelatiam)  [gitlab](https://gitlab.com/joelatiam)*
