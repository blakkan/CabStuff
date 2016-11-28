from pyspark import SparkContext, SparkConf

from pyspark.sql import SQLContext
from pyspark.sql.types import *

import borough_finder  #Needs numpy and matplotlib installed via pip
import time


def forceAFloat(s):
  try:
	f = float(s)
	return f
  except:
	return 0.0


#conf = (SparkConf()
#	.setMaster("local")
#	.setAppName("My app")
#	.set("spark.executor.memory", "1g"))
#sc = SparkContext(conf = conf)

sc = SparkContext("local", "myApp", pyFiles=['borough_finder.py'] )
sqlContext = SQLContext(sc)

print "loadint textfile"

distData = sc.textFile('file:///home/w205/longlat.csv')

print distData.take(10)

print "splitting"
split_records = distData.map(lambda l: l.split(','))

print split_records.take(10)

the_time = time.time()

#And putting the updated output into new tables
print "starting mapping"
j = split_records.map(lambda s: ( s[0], s[1], borough_finder.find_borough(
	forceAFloat(s[0]),
	forceAFloat(s[1])
)))
print "create data frame"
LonLatMap = sqlContext.createDataFrame(j)
print "save it to sql"
LonLatMap.save("converted_lon_lat")

print "Took %d seconds " % (time.time() - the_time)
