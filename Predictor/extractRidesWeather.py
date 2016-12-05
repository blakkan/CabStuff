import csv
import time
import pandas

from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf


the_time = time.time()

# start Spark and  Hive SQL contexts
sc = SparkContext("local", "extract ridesWeather")
hc = HiveContext(sc)

df = hc.sql('select * from rides_yg_daily_by_zipcode r join ny_weather w on r.pickup_year = year(w.datestamp) and r.pickup_month = month(w.datestamp) and r.pickup_day = day(w.datestamp)')
#df = hc.sql('select * from ny_weather limit 10')
df.show()

#df.rdd.write.csv('/home/w205/secondary/rohit/data/dailyRidesWeather.csv')
#df.write.format('com.databricks.spark.csv').save('/home/w205/secondary/rohit/data/dailyRidesWeather.csv')
df.toPandas().to_csv('/home/w205/secondary/rohit/data/dailyRidesWeather.csv')
print "Took %d seconds " % (time.time() - the_time)
