# run from folder want these to transfer to

# putting into the /home/w205

scp -i "Project-w205.pem" /c/Users/andre/Documents/UCB/W205/Project/TLCData/taxi_zone_lookup.csv  root@ec2-52-87-158-95.compute-1.amazonaws.com:/home/w205/support/

scp -i "Project-w205.pem" /c/Users/andre/Documents/UCB/W205/Project/TLCData/NY-zip-long-lat-map.csv  root@ec2-52-87-158-95.compute-1.amazonaws.com:/home/w205/support/


scp -i "Project-w205.pem" /c/Users/andre/Documents/UCB/W205/Project/hive-site.xml root@ec2-52-90-58-78.compute-1.amazonaws.com:/home/w205/spark15/conf

scp -i "Project-w205.pem" /c/Users/andre/Documents/UCB/W205/Project/load_rides_yg.py root@ec2-54-205-217-212.compute-1.amazonaws.com:/home/w205/support/





#log into instance, switch to w205 user and move to support directory
# drop headings
tail -n +2 "taxi_zone_lookup.csv" > "taxi_zone_map.csv"
rm taxi_zone_lookup.csv

tail -n +2 "NY-zip-long-lat-map.csv" > "zip_long_lat_map.csv"
rm NY-zip-long-lat-map.csv

#move into hdfs
# create directories if not created
hdfs dfs -mkdir /user/w205/support

hdfs dfs -mkdir /user/w205/support/loc_zone
hdfs dfs -put taxi_zone_map.csv /user/w205/support/loc_zone
rm taxi_zone_map.csv

hdfs dfs -mkdir /user/w205/support/zip_map
hdfs dfs -put zip_long_lat_map.csv /user/w205/support/zip_map
rm zip_long_lat_map.csv



hdfs dfs -rm /user/w205/support/zip_map/zip_long_lat_map.csv

