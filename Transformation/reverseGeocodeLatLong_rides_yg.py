import io
import reverse_geocoder as rg

import time

from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

# read GeoNames data file
geo = rg.RGeocoder(mode=1, verbose=True, stream=io.StringIO(open('/data/rohit/script/TriState.csv', 'r').read().decode('utf-8')))

def reverse_geocode(lat, long):
        gcode = geo.query([(float(lat), float(long))])
        return "00000" if gcode is None or len(gcode) == 0 else str(gcode[0]["admin2"]).zfill(5)

the_time = time.time()

# start Spark and  Hive SQL contexts
sc = SparkContext("local", "reverse geocoding")
hc = HiveContext(sc)

rdd = hc.sql("FROM rides_yg SELECT *  WHERE pickup_longt IS NOT NULL AND pickup_lat IS NOT NULL")
#print rdd.count()
hc.sql('DROP TABLE IF EXISTS rides_yg_tristate')
#rdd.show()

udf_reverse_geocode = udf(reverse_geocode, StringType() )
rdd2 = rdd.withColumn("pickup_zipcode", udf_reverse_geocode("pickup_lat", "pickup_longt"))
#print rdd2.count()
#rdd2.show()
rdd2.saveAsTable('rides_yg_tristate');

print "Took %d seconds " % (time.time() - the_time)
