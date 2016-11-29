# imports
from pyspark import SparkContext
from pyspark.sql import HiveContext

# start Spark and  Hive SQL contexts
sc = SparkContext("local", "demo app")
hc = HiveContext(sc)

# get the table
print "Printing tables in DB:"
print hc.tableNames()

print "Printing first 10 rows in zip_neighborhood_borough_xref table."
sqlQuery = "SELECT * FROM zip_neighborhood_borough_xref limit 10"
hc.sql(sqlQuery).show()