#
#
# Test conversion, accumulate in accumulators
#
#

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


#
# This shares the python files (with their own copies of the hash.. Analogous to
# a broadcast variable) for conversion
#

sc = SparkContext("local", "myApp", pyFiles=['borough_finder.py'] )
sqlContext = SQLContext(sc)

print "loading textfile"

distData = sc.textFile('file:///home/w205/longlat.csv')

print "splitting"
split_records = distData.map(lambda l: l.split(','))


the_time = time.time()

#And putting the updated output into new tables
print "starting mapping"
j = split_records.map(lambda s: ( s[0], s[1], borough_finder.find_borough(
	forceAFloat(s[1]), #note swapping order here
	forceAFloat(s[0])
)))



accumManhattan = sc.accumulator(0)
accumTheBronx = sc.accumulator(0)
accumQueens = sc.accumulator(0)
accumBrooklyn = sc.accumulator(0)
accumStatenIsland = sc.accumulator(0)
accumNone = sc.accumulator(0)
accumMystery = sc.accumulator(0)

accumHash = { "None": accumNone, "Manhattan": accumManhattan, "The Bronx": accumTheBronx, "Staten Island": accumStatenIsland, "Queens": accumQueens, "Brooklyn": accumBrooklyn }


j.foreach(lambda x: accumHash[x[2]].add(1))


for key, value in accumHash.iteritems():
  print "%s: %d" % ( key, value.value )
