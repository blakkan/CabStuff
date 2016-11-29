import io
import reverse_geocoder as rg

from pyspark import SparkContext
from pyspark.sql import HiveContext

# start Spark and  Hive SQL contexts
sc = SparkContext("local", "demo app")
hc = HiveContext(sc)

# read GeoNames data file
geo = rg.RGeocoder(mode=2, verbose=True, stream=io.StringIO(open('/data/rohit/script/US_ascii.csv', 'r').read().decode('utf-8')))

print "Printing first 10 rows from rides table."
sqlQuery = "SELECT * FROM rides_yg limit 10"
hc.sql(sqlQuery).show()

coordinates = (40.742596,-74.153481),(41.316105,-74.127701),(40.786224,-74.043663),(40.736961,-74.038422),(40.748005,-74.032402),(41.031322,-74.02137 ),(40.647068,-74.010513),(40.708969,-74.010262),(40.720478,-74.010147)

print geo.query(coordinates)