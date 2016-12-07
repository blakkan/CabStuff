--once borough/zip added to rides
--join to weather

DROP TABLE rides_weather;
CREATE TABLE rides_weather AS
SELECT r.ride_source, r.passenger_count, r.borough, r.pickup_year, r.pickup_month, r.pickup_day, r.pickup_hour,
w.PRCP, w.SNOW, w.TMAX, w.TMIN, sum(r.ride_count) as r_count
FROM rides_yg_with_boroug r INNER JOIN historical_weather_schema w on r.pickup_year = cast(substr(w.date,1,4) as int) 
and r.pickup_month = cast(substr(w.date,5,2) as int) and r.pickup_day = cast(substr(w.date,7,2) as int)
where r.borough <> 'None' and w.station_name = 'LA GUARDIA AIRPORT NY US'
GROUP BY r.ride_source, r.passenger_count, r.borough, r.pickup_year, r.pickup_month, r.pickup_day, r.pickup_hour,
w.PRCP, w.SNOW, w.TMAX, w.TMIN;


--select sum(r_count) from rides_weather;



INSERT INTO TABLE rides_weather
SELECT "FHV" as ride_source, "1" as passenger_count, lz.borough, cast(substr(f.pickup_date,1,4) as int) as pickup_year, 
cast(substr(f.pickup_date,6,2) as int) as pickup_month, 
cast(substr(f.pickup_date,9,2) as int) as pickup_day, 
cast(substr(f.pickup_date,12,2) as int) as pickup_hour, w.PRCP, w.SNOW, w.TMAX, w.TMIN, count(*) as r_count
FROM fhv_rides_schema f INNER JOIN loc_zone_schema lz ON f.locationID = lz.location_id INNER JOIN historical_weather_schema w 
on cast(substr(f.pickup_date,1,4) as int) = cast(substr(w.date,1,4) as int) 
and cast(substr(f.pickup_date,6,2) as int) = cast(substr(w.date,5,2) as int) 
and cast(substr(f.pickup_date,9,2) as int) = cast(substr(w.date,7,2) as int)
where f.locationID is not null and w.station_name = 'LA GUARDIA AIRPORT NY US'
group by lz.borough, cast(substr(f.pickup_date,1,4) as int), 
cast(substr(f.pickup_date,6,2) as int), 
cast(substr(f.pickup_date,9,2) as int), 
cast(substr(f.pickup_date,12,2) as int), w.PRCP, w.SNOW, w.TMAX, w.TMIN


