DROP TABLE IF EXISTS ny_weather_temp;
CREATE EXTERNAL TABLE IF NOT EXISTS ny_weather_temp
(
snow_depth decimal,
max_temp_c decimal,
tornado boolean,
visibility decimal,
snow boolean,
max_wind_gust decimal,
temp_c decimal,
date date,
min_temp decimal,
precipitation decimal,
wind_speed decimal,
rain boolean,
dew_point decimal,
temp decimal,
thunder boolean,
min_temp_c decimal,
hail boolean,
sea_level_pressure decimal,
weather_ok boolean,
fog boolean,
indicators char(6),
station_pressure decimal,
max_temp decimal,
max_wind_speed decimal
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/ny_weather';

DROP TABLE IF EXISTS ny_weather;
CREATE TABLE IF NOT EXISTS ny_weather
AS 
SELECT
CAST(date as date) as datestamp,
CAST(weather_ok as boolean) as weather_ok,

CAST(visibility as decimal(10,2)) as visibility,
CAST(fog as boolean) as fog,

CAST(min_temp as decimal(10,2)) as min_temp,
CAST(max_temp as decimal(10,2)) as max_temp,
CAST(temp as decimal(10,2)) as mean_temp,

CAST(rain as boolean) as rain,
CAST(hail as boolean) as hail,
CAST(thunder as boolean) as thunder,
CAST(precipitation as decimal(10,2)) as precipitation,
CAST(dew_point as decimal(10,2)) as dew_point,

CAST(min_temp_c as decimal(10,2)) as min_temp_c,
CAST(max_temp_c as decimal(10,2)) as max_temp_c,
CAST(temp_c as decimal(10,2)) as mean_temp_c,

CAST(station_pressure as decimal(10,2)) as station_pressure,
CAST(sea_level_pressure as decimal(10,2)) as sea_level_pressure,

CAST(snow as boolean) as snow,
CAST(snow_depth as decimal(10,2)) as snow_depth,

CAST(tornado as boolean) as tornado,
CAST(wind_speed as decimal(10,2)) as mean_wind_speed,
CAST(max_wind_speed as decimal(10,2)) as max_wind_speed,
CAST(max_wind_gust as decimal(10,2)) as max_wind_gust
FROM ny_weather_temp;
