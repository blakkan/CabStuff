import io

from pyspark import SparkContext
from pyspark.sql import HiveContext


def build_sql(ride_source, year, month):
    if ride_source == 'Yellow':
		str_sql = "CREATE TABLE tmp_rides_yg AS "
		str_sql += "SELECT 'Yellow' as ride_source, passenger_count, round(pickup_longitude,6) as pickup_longt, round(pickup_latitude,6) as pickup_lat, "
		str_sql += "year(tpep_pickup_datetime) as pickup_year, month(tpep_pickup_datetime) as pickup_month, dayofmonth(tpep_pickup_datetime) as pickup_day, "
		str_sql += "weekofyear(tpep_pickup_datetime) as pickup_week, from_unixtime(unix_timestamp(tpep_pickup_datetime),'EEEEE') as pickup_weekday, hour(tpep_pickup_datetime) as pickup_hour "
		str_sql += "FROM yellow_rides_schema "
		str_sql += "WHERE year(tpep_pickup_datetime) = " + str(year) + " and month(tpep_pickup_datetime) = " + str(month)
	else:
		str_sql = "CREATE TABLE tmp_rides_yg AS "
		str_sql += "SELECT 'Green' as ride_source, passenger_count, round(pickup_longitude,6) as pickup_longt, round(pickup_latitude,6) as pickup_lat,  "
		str_sql += "year(lpep_pickup_datetime) as pickup_year, month(lpep_pickup_datetime) as pickup_month, dayofmonth(lpep_pickup_datetime) as pickup_day,  "
		str_sql += "weekofyear(lpep_pickup_datetime) as pickup_week, from_unixtime(unix_timestamp(lpep_pickup_datetime),'EEEEE') as pickup_weekday, hour(lpep_pickup_datetime) as pickup_hour "
		str_sql += "FROM green_rides_schema "
		str_sql += "WHERE year(lpep_pickup_datetime) = " + str(year) + " and month(lpep_pickup_datetime) = " + str(month)
	return str_sql

# other sql statement doesn't change:

sql_drop = "DROP TABLE tmp_rides_yg "


str_sql2 = "INSERT INTO TABLE rides_yg "
str_sql2 += "SELECT ride_source, passenger_count, pickup_longt, pickup_lat, pickup_year, pickup_month, pickup_day, " 
str_sql2 += "pickup_week, pickup_weekday, pickup_hour, count(*) as ride_count "
str_sql2 += "FROM tmp_rides_yg "
str_sql2 += "GROUP BY  ride_source, passenger_count, pickup_longt, pickup_lat, pickup_year, pickup_month, pickup_day, "
str_sql2 += "pickup_week, pickup_weekday, pickup_hour "


# start Spark and  Hive SQL contexts
sc = SparkContext("local", "Load Rides App")
hc = HiveContext(sc)

for ride_source in ['Yellow', 'Green']:
	for year in [2016, 2015, 2014]:
		if year == 2016:
			for month in range(1,6):
				print "Year: " +  str(year) + " Month: " + str(month)
				str_sql = build_sql(ride_source, year, month)
				hc.sql(sql_drop).show()
				hc.sql(str_sql).show()
				hc.sql(str_sql2).show()
		if year == 2015:
			for week in range(1,12):
							print "Year: " +  str(year) + " Month: " + str(month)
				str_sql = build_sql(ride_source, year, month)
				hc.sql(sql_drop).show()
				hc.sql(str_sql).show()
				hc.sql(str_sql2).show()
		if year == 2014:
			for week in range(7,12):
				print "Year: " +  str(year) + " Month: " + str(month)
				str_sql = build_sql(ride_source, year, month)
				hc.sql(sql_drop).show()
				hc.sql(str_sql).show()
				hc.sql(str_sql2).show()




