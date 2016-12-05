import time

from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf


the_time = time.time()

# start Spark and  Hive SQL contexts
sc = SparkContext("local", "rohit hourly app")
hc = HiveContext(sc)

rdd = hc.sql("select ride_source, pickup_year, pickup_month, pickup_day, pickup_hour, pickup_zipcode, sum(passenger_count) as passenger_count, avg(passenger_count) as avg_passenger_count, sum(ride_count) as ride_count from rides_yg_z group by ride_source, pickup_year, pickup_month, pickup_day, pickup_hour, pickup_zipcode")
hc.sql('DROP TABLE IF EXISTS hourly_rides_yg_by_zipcode')
#rdd.show()

rdd.saveAsTable('hourly_rides_yg_by_zipcode');

print "Took %d seconds " % (time.time() - the_time)
