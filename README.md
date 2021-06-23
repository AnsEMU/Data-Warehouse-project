## Project: Data Warehouse

### Project objective
A music streaming company, called Sparkify, has been scaling up their user and song database and want to move their processes and data onto the cloud. S3 is were thire data located, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project, An ETL pipeline has been built for a database hosted on Redshift. The data was loaded from S3 to staging tables on Redshift and SQL commands were executed for the creation of the analytics tables from these staging tables.

### Files Description
* **create_table.py** - This file will create the fact and dimension tables for the star schema in Redshift.
* **etl.py** - This file will load data from S3 into staging tables on Redshift and then process that data into your analytical tables on Redshift.
* **sql_queries.py** - This file will define the SQL commands, which will be imported into the two other files above.

### Project Steps
1. Schemas for the fact table and dimension tables were designed.
![proj yables](https://user-images.githubusercontent.com/12682524/123097594-93e96f80-d430-11eb-8074-5f52146f2980.png)
2. SQL DROP commands and CREATE commands were written for each of these tables in sql_queries.py.
3. A redshift cluster is already created in IaC and an IAM role is attached also.
4. Redshift database and IAM role info with host is stored in dwh.cfg.
5. create_tables.py is run to connect to the database and drop tables if exists then create these tables.
6. The table creation and schemas are tested in query editor in the redshift database with results as showen in the bottom.
7. etl.py is run to load the data from S3 to staging tables and then from staging tables to analtical tables on redshift.

### Query results
> select * from factsongplays limit 5;
```
songplay_id,start_time,userid,level,song_id,artist_id,sessionid,location,useragent
5,2018-11-13 18:39:37.796,15,paid,SOVAEBW12AB0182CE6,AR756JL1187FB3D3A9,417,"Chicago-Naperville-Elgin, IL-IN-WI","""Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36"""
69,2018-11-14 23:14:07.796,49,paid,SOWGZFG12A8151AF41,ARC8CQZ1187B98DECA,576,"San Francisco-Oakland-Hayward, CA",Mozilla/5.0 (Windows NT 5.1; rv:31.0) Gecko/20100101 Firefox/31.0
133,2018-11-29 16:00:01.796,49,paid,SOTNWCI12AAF3B2028,ARS54I31187FB46721,1041,"San Francisco-Oakland-Hayward, CA",Mozilla/5.0 (Windows NT 5.1; rv:31.0) Gecko/20100101 Firefox/31.0
197,2018-11-24 17:29:19.796,29,paid,SODKJWI12A8151BD74,ARM0P6Z1187FB4D466,898,"Atlanta-Sandy Springs-Roswell, GA","""Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.78.2 (KHTML, like Gecko) Version/7.0.6 Safari/537.78.2"""
261,2018-11-28 22:56:08.796,73,paid,SOBONKR12A58A7A7E0,AR5E44Z1187B9A1D74,954,"Tampa-St. Petersburg-Clearwater, FL","""Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.78.2 (KHTML, like Gecko) Version/7.0.6 Safari/537.78.2"""
```

> select count(*) from factsongplays;
```
count
319
```
> select a.artist_name,s.title,count(f.songplay_id)
from factsongplays f
join dimartists a on a.artist_id = f.artist_id
join dimsongs s on s.song_id = f.song_id
where  f.level = 'paid'
group by 1,2
order by 3 desc
limit 10;
```
artist_name      ,title                                                ,count
Dwight Yoakam    ,You're The One                                       ,1073
Ron Carter       ,I CAN'T GET STARTED                                  ,72
B.o.B            ,Nothin' On You [feat. Bruno Mars] (Album Version)    ,64
Lonnie Gordon    ,Catch You Baby (Steve Pitron & Max Sanna Radio Edit) ,45
Kid Cudi         ,Make Her Say                                         ,20
Muse             ,Supermassive Black Hole (Album Version)              ,16
Kid Cudi         ,Up Up & Away                                         ,10
Fisher           ,Rianna                                               ,9
Linkin Park      ,Given Up (Album Version)                             ,6
David Arkenstone ,Waterfall (Spirit Of The Rainforest Album Version)   ,4
```
