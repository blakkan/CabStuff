DROP TABLE yellow_rides_schema;
CREATE EXTERNAL TABLE IF NOT EXISTS yellow_rides_schema
(
vendor_id int,
tpep_pickup_datetime Date, tpep_dropoff_datetime Date,
passenger_count int, trip_distance decimal,
pickup_longitude decimal, pickup_latitude decimal,
RatecodeID int, store_and_fwd_flag string,
dropoff_longitude decimal, dropoff_latitude decimal,
payment_type int, fare_amount decimal,
extra decimal, mta_tax decimal, tip_amount decimal,
tolls_amount decimal, improvement_surcharge decimal,
total_amount decimal
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/yellow';


--check load
--select count(*) from yellow_rides_schema;



DROP TABLE green_rides_schema;
CREATE EXTERNAL TABLE IF NOT EXISTS green_rides_schema
(
VendorID int,
lpep_pickup_datetime Date, lpep_dropoff_datetime Date,
Passenger_count int, Trip_distance decimal,
Pickup_longitude decimal, Pickup_latitude decimal,
RateCodeID int, Store_and_fwd_flag string,
Dropoff_longitude decimal, Dropoff_latitude decimal,
Payment_type int, Fare_amount decimal,
Extra decimal, MTA_tax decimal, Improvement_surcharge  decimal,
Tip_amount decimal, Tolls_amount decimal, Total_amount decimal,
Trip_type int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/green';

--check load
--select count(*) from green_rides_schema;


DROP TABLE fhv_rides_schema;
CREATE EXTERNAL TABLE IF NOT EXISTS fhv_rides_schema
(
Dispatching_base_num int, Pickup_date Date,
locationID int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/fhv';

--check zone
--select count(*) from fhv_rides_schema;

--Used to map FHV rides to neighborhood

DROP TABLE loc_zone_schema;
CREATE EXTERNAL TABLE IF NOT EXISTS loc_zone_schema
(location_id int, borough string, zone string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/support/loc_zone';

