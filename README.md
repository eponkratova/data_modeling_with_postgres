# Data Modeling with Postgres
---
author: "EPonkratova"
date: "April 15, 2019"
---
The project shows proper use of documentation.

The README file includes a summary of the project, how to run the Python scripts, and an explanation of the files in the repository. Comments are used effectively and each function has a docstring.

The project code is clean and modular.

Scripts have an intuitive, easy-to-follow structure with code separated into logical functions. Naming for variables and functions follows the PEP8 style guidelines.


## Problem statement
Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Solution
To develop a data warehousing solution for the purposes of external and internal reporting and BI which will allow to merge data from multiple data sources - currently only json files.

### Approach
The start schema will be used which allows integrating new measures to the previously created measures/dimension tables or adding new measures/dimension tables in the process. 
  - As the company wants to track listening behavior of the subscribers, our model will have only one fact table which contains the list of every song play. There are four attributes that are references to the dimension tables (user_id, start_time, song_id, and artist_id).
  -  The dimension tables will represent categories the company wants to use in reporting i.e. users=subscribers, artists and songs. In addition, the time dimension was added as every record from the fact table will have a date assigned to the dim time table. This way we will store records granulated on a daily/hourly level but we we'll aslo have fast access to weekly, monthly, and yearly statistics.
  - The grain of the fact table is a song played per a user.
###### Physical model of the data warehouse 
![Physial model](https://github.com/eponkratova/data_modeling_with_postgres/blob/master/pics/screenshot.png)

### Practical implementation
To identify data types to be used for the columns, the structure of the json files was inspected - a limitation is that only one file from each folder was reviewed, thus, not complete data profiling was done and not all cases of values of different lenght were considered:
```sh
#Importing the pandas libarary
import pandas as pd
#Reading the log file and checking the column types
df_log = pd.read_json('data/log_data/2018/11/2018-11-01-events.json', lines=True)
df_log.dtypes
```
| artist        | object  |
|---------------|---------|
| auth          | object  |
| firstName     | object  |
| gender        | object  |
| itemInSession | int64   |
| lastName      | object  |
| length        | float64 |
| level         | object  |
| location      | object  |
| method        | object  |
| page          | object  |
| registration  | int64   |
| sessionId     | int64   |
| song          | object  |
| status        | int64   |
| ts            | int64   |
| userAgent     | object  |
| userId        | int64   |

```
#Reading songs file
df_song = pd.read_json('data/song_data/A/B/C/TRABCEI128F424C983.json', lines=True)
df_song.dtypes
```
| artist_id        | object  |
|------------------|---------|
| artist_latitude  | float64 |
| artist_location  | object  |
| artist_longitude | float64 |
| artist_name      | object  |
| duration         | float64 |
| num_songs        | int64   |
| song_id          | object  |
| title            | object  |
| year             | int64   |

After the semi- data profiling, the etl job was created and launched - see the steps and files used below.
![Process](https://github.com/eponkratova/data_modeling_with_postgres/blob/master/pics/screenshot2.png)

ETL completion was tracked based on the number of input and output rows -
| songplays | 6820 |
|-----------|------|
| users     | 6820 |
| songs     | 72   |
| artists   | 72   |
| time      | 6820 |

## Limitations
1. No complete data profiling was done which increase the chance of an error and thus, the job failure;
3. It seems that the script did not do the proper artist_id and song_id matching because as per the current results there was no matching entry. Potentially, more testing is required.
