from pyspark import SparkContext, SparkConf
import borough_finder  #Needs numpy and matplotlib installed via pip
import time

#conf = (SparkConf()
#	.setMaster("local")
#	.setAppName("My app")
#	.set("spark.executor.memory", "1g"))
#sc = SparkContext(conf = conf)

#beware- need to change the absolute file here
sc = SparkContext("local", "myApp", pyFiles=['borough_finder.py'] )

data = [
( 40.854453, -73.854218), #bronx
( 40.591136,-74.132996),  #staten island
( 40.629709,-73.936615),  #brooklyn
( 40.791061,-73.950348),  #manhattan
( 40.713036,-73.874817)  #queens
] * 2000000
distData = sc.parallelize(data) #normally we'd be reading hive tables
print "About to do 10 million conversions"
the_time = time.time()

#And putting the updated output into new tables
j = distData.map(lambda s: borough_finder.find_borough(s[0], s[1])).collect()

print "Took %d seconds " % (time.time() - the_time)
