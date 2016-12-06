--this file converts external tables into usable tables for mapping and prediction

-- connected to aws
-- start hadoop / postgres
-- switch to w205 user
-- spark should already be set up

-- start metastore
-- start sparksql-
--cd /data/spark15/bin/
--spark-sql

--validate counts
--find counts

--yellow count
--select count(*) from yellow_rides_schema;
-- 36118719 (initial run)
-- 105,125,606 (with July 2015 - June 2016 loaded)

--yellow taxi rides are basis of rides
--FIRST is create; later adds of green or yellow would be insert
DROP TABLE rides_yg;

CREATE TABLE rides_yg AS
SELECT "Yellow" as ride_source, passenger_count, round(pickup_longitude,6) as pickup_longt, round(pickup_latitude,6) as pickup_lat, 
year(tpep_pickup_datetime) as pickup_year, month(tpep_pickup_datetime) as pickup_month, 
dayofmonth(tpep_pickup_datetime) as pickup_day, 
weekofyear(tpep_pickup_datetime) as pickup_week, from_unixtime(unix_timestamp(tpep_pickup_datetime),'EEEEE') as pickup_weekday,
hour(tpep_pickup_datetime) as pickup_hour,
count(*) as ride_count
FROM yellow_rides_schema
GROUP BY passenger_count, round(pickup_longitude,6), round(pickup_latitude,6), year(tpep_pickup_datetime), 
month(tpep_pickup_datetime), dayofmonth(tpep_pickup_datetime), weekofyear(tpep_pickup_datetime), 
from_unixtime(unix_timestamp(tpep_pickup_datetime),'EEEEE'), hour(tpep_pickup_datetime);
--LIMIT 10


INSERT INTO TABLE rides_yg
SELECT "Green" as ride_source, passenger_count, round(pickup_longitude,6) as pickup_longt, round(pickup_latitude,6) as pickup_lat, 
year(lpep_pickup_datetime) as pickup_year, month(lpep_pickup_datetime) as pickup_month, 
dayofmonth(lpep_pickup_datetime) as pickup_day, 
weekofyear(lpep_pickup_datetime) as pickup_week, from_unixtime(unix_timestamp(lpep_pickup_datetime),'EEEEE') as pickup_weekday,
hour(lpep_pickup_datetime) as pickup_hour,
count(*) as ride_count
FROM green_rides_schema
GROUP BY passenger_count, round(pickup_longitude,6), round(pickup_latitude,6), year(lpep_pickup_datetime), 
month(lpep_pickup_datetime), dayofmonth(lpep_pickup_datetime), weekofyear(lpep_pickup_datetime), 
from_unixtime(unix_timestamp(lpep_pickup_datetime),'EEEEE'), hour(lpep_pickup_datetime);

