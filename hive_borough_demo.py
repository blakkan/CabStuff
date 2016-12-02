from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf


import borough_finder  #Needs numpy and matplotlib installed via pip
import time

#
# This is just a wrapper around the main function; it translates strings into floats....
# This conversion should probably be possible to elminate, it was put in to make the udf
# (user defined function?) work

def bf_string(x, y):
  return borough_finder.find_borough(float(x), float(y))


the_time = time.time()



sc = SparkContext("local", "demo app")

sqlContext = HiveContext(sc)


#
# Read the hive table in and clear the output table, if it exists
#

rdd = sqlContext.sql("FROM rides_yg SELECT *  WHERE pickup_longt IS NOT NULL AND pickup_lat IS NOT NULL LIMIT 20")
sqlContext.sql('DROP TABLE rides_yg_demo')




udf_find_borough = udf( bf_string, StringType() )
rdd3 = rdd.withColumn("borough", udf_find_borough( "pickup_lat", "pickup_longt" ) )

#
# here's where we save it
#

rdd3.saveAsTable('rides_yg_demo');


print "Took %d seconds " % (time.time() - the_time)
