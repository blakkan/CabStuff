-- zip_neighborhood_borough.sql
-- THIS SCRIPT CREATES MAPPING BETWEEN 
-- ZIP - NEIGHBORHOOD - BOROUGH.
CREATE EXTERNAL TABLE IF NOT EXISTS zip_neighborhood_borough_xref (
zipcode string,
neighborhood string,
borough string
)
ROW FORMAT DELIMITED FIELDS
TERMINATED BY ','
STORED AS TEXTFILE
LOCATION
'/user/w205/xref/zip_neighborhood_borough';