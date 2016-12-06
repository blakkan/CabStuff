# run from folder want these to transfer to

# putting into the /home/w205

wget("https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv")


#log into instance, switch to w205 user and move to support directory
# drop headings
tail -n +2 "taxi+_zone_lookup.csv" > "taxi_zone_map.csv"
rm taxi_zone_lookup.csv

#move into hdfs
# create directories if not created
hdfs dfs -mkdir /user/w205/support

hdfs dfs -mkdir /user/w205/support/loc_zone
hdfs dfs -put taxi_zone_map.csv /user/w205/support/loc_zone
rm taxi_zone_map.csv
