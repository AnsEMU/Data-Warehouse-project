# Data-Warehouse-project
The 3rd Project of Udacity Nanodegree of Data Engineering 


## Project: Data Warehouse

### Purpose of the Project
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project, I have built an ETL pipeline for a database hosted on Redshift. The data was loaded from S3 to staging tables on Redshift and SQL statements were executed that created the analytics tables from these staging tables.

### Files Description
* **create_table.py** - This file will create the facts and dimensions tables for the star schema in Redshift.
* **etl.py** - This file will load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
* **sql_queries.py** - This file will define the SQL statements, which will be imported into the two other files above.

### Project Steps
1. Schemas for the fact and dimension tables were designed.
2. SQL CREATE statements and DROP statements were written for each of these tables in sql_queries.py.
3. A redshift cluster is created in AWS console and an IAM role is attached to the cluster that has appropriate read access to S3 bucket where the data resides. 
4. Redshift database and IAM role info is stored in dwh.cfg.
5. create_tables.py is run to connect to the database and create these tables.
6. The table creation and schemas are tested in query editor in the redshift database.
7. etl.py is run to load the data from s3 to staging tables and then from staging tables to analtics tables on redshift.

### Query results
> select * from songplays limit 5;
```
songplay_id start_time user_id level song_id artist_id session_id location user_agent
14	2018-11-02 01:34:17.0	83	free			82	Lubbock, TX	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36"
30	2018-11-02 03:34:34.0	86	free			170	La Crosse-Onalaska, WI-MN	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.94 Safari/537.36"
46	2018-11-02 09:04:16.0	15	paid			172	Chicago-Naperville-Elgin, IL-IN-WI	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36"
62	2018-11-02 09:13:37.0	89	free	SOGKLRH12AB0187E8A	AR0HQE41187B9A28D3	88	Cedar Rapids, IA	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36"
78	2018-11-02 09:26:49.0	15	paid			172	Chicago-Naperville-Elgin, IL-IN-WI	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36"
```

> select count(*) from songplays;
```
count
8390
```
> select songplay_id, sp.artist_id, name, year from songplays sp join artists a on sp.artist_id = a.artist_id join time t on sp.start_time = t.start_time where sp.artist_id = 'AR0HQE41187B9A28D3' and level = 'free';
```
songplay_id artist_id name year
62	AR0HQE41187B9A28D3	Come Sail Away	2018
62	AR0HQE41187B9A28D3	Wild World	2018
62	AR0HQE41187B9A28D3	I Only Want To Be With You	2018
62	AR0HQE41187B9A28D3	The Boxer	2018
```
