# run from folder want these to transfer to

# putting into the /home/w205

# 2016
wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-01.csv
tail -n +2 "green_tripdata_2016-01.csv" > "g_201601.csv"
rm green_tripdata_2016-01.csv
hdfs dfs -put g_201601.csv /user/w205/green
rm g_201601.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-02.csv
tail -n +2 "green_tripdata_2016-02.csv" > "g_201602.csv"
rm green_tripdata_2016-02.csv
hdfs dfs -put g_201602.csv /user/w205/green

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-03.csv
tail -n +2 "green_tripdata_2016-03.csv" > "g_201603.csv"
rm green_tripdata_2016-03.csv
hdfs dfs -put g_201603.csv /user/w205/green
rm g_201603.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-04.csv
tail -n +2 "green_tripdata_2016-04.csv" > "g_201604.csv"
rm green_tripdata_2016-04.csv
hdfs dfs -put g_201604.csv /user/w205/green
rm g_201604.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-05.csv
tail -n +2 "green_tripdata_2016-05.csv" > "g_201605.csv"
rm green_tripdata_2016-05.csv
hdfs dfs -put g_201605.csv /user/w205/green
rm g_201605.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-06.csv
tail -n +2 "green_tripdata_2016-06.csv" > "g_201606.csv"
rm green_tripdata_2016-06.csv
hdfs dfs -put g_201606.csv /user/w205/green
rm g_201606.csv


# 2015
# *****

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2015-01.csv
tail -n +2 "green_tripdata_2015-01.csv" > "g_201501.csv"
rm green_tripdata_2015-01.csv
hdfs dfs -put g_201501.csv /user/w205/green
rm g_201501.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2015-02.csv
tail -n +2 "green_tripdata_2015-02.csv" > "g_201502.csv"
rm green_tripdata_2015-02.csv
hdfs dfs -put g_201502.csv /user/w205/green
rm g_201502.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2015-03.csv
tail -n +2 "green_tripdata_2015-03.csv" > "g_201503.csv"
rm green_tripdata_2015-03.csv
hdfs dfs -put g_201503.csv /user/w205/green
rm g_201503.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2015-04.csv
tail -n +2 "green_tripdata_2015-04.csv" > "g_201504.csv"
rm green_tripdata_2015-04.csv
hdfs dfs -put g_201504.csv /user/w205/green
rm g_201504.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2015-05.csv
tail -n +2 "green_tripdata_2015-05.csv" > "g_201505.csv"
rm green_tripdata_2015-05.csv
hdfs dfs -put g_201505.csv /user/w205/green
rm g_201505.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2015-06.csv
tail -n +2 "green_tripdata_2015-06.csv" > "g_201506.csv"
rm green_tripdata_2015-06.csv
hdfs dfs -put g_201506.csv /user/w205/green
rm g_201506.csv


wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2015-07.csv
tail -n +2 "green_tripdata_2015-07.csv" > "g_201507.csv"
rm green_tripdata_2015-07.csv
hdfs dfs -put g_201507.csv /user/w205/green
rm g_201507.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2015-08.csv
tail -n +2 "green_tripdata_2015-08.csv" > "g_201508.csv"
rm green_tripdata_2015-08.csv
hdfs dfs -put g_201508.csv /user/w205/green
rm g_201508.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2015-09.csv
tail -n +2 "green_tripdata_2015-09.csv" > "g_201509.csv"
rm green_tripdata_2015-09.csv
hdfs dfs -put g_201509.csv /user/w205/green
rm g_201509.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2015-10.csv
tail -n +2 "green_tripdata_2015-10.csv" > "g_201510.csv"
rm green_tripdata_2015-10.csv
hdfs dfs -put g_201510.csv /user/w205/green
rm g_201510.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2015-11.csv
tail -n +2 "green_tripdata_2015-11.csv" > "g_201511.csv"
rm green_tripdata_2015-11.csv
hdfs dfs -put g_201511.csv /user/w205/green
rm g_201511.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2015-12.csv
tail -n +2 "green_tripdata_2015-12.csv" > "g_201512.csv"
rm green_tripdata_2015-12.csv
hdfs dfs -put g_201512.csv /user/w205/green
rm g_201512.csv



# 2014

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2014-07.csv
tail -n +2 "green_tripdata_2014-07.csv" > "g_201407.csv"
rm green_tripdata_2014-07.csv
hdfs dfs -put g_201407.csv /user/w205/green
rm g_201407.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2014-08.csv
tail -n +2 "green_tripdata_2014-08.csv" > "g_201408.csv"
rm green_tripdata_2014-08.csv
hdfs dfs -put g_201408.csv /user/w205/green
rm g_201408.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2014-09.csv
tail -n +2 "green_tripdata_2014-09.csv" > "g_201409.csv"
rm green_tripdata_2014-09.csv
hdfs dfs -put g_201409.csv /user/w205/green
rm g_201409.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2014-10.csv
tail -n +2 "green_tripdata_2014-10.csv" > "g_201410.csv"
rm green_tripdata_2014-10.csv
hdfs dfs -put g_201410.csv /user/w205/green
rm g_201410.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2014-11.csv
tail -n +2 "green_tripdata_2014-11.csv" > "g_201411.csv"
rm green_tripdata_2014-11.csv
hdfs dfs -put g_201411.csv /user/w205/green
rm g_201411.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2014-12.csv
tail -n +2 "green_tripdata_2014-12.csv" > "g_201412.csv"
rm green_tripdata_2014-12.csv
hdfs dfs -put g_201412.csv /user/w205/green
rm g_201412.csv

# 2014 continued...

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2014-01.csv
tail -n +2 "green_tripdata_2014-01.csv" > "g_201401.csv"
rm green_tripdata_2014-01.csv
hdfs dfs -put g_201401.csv /user/w205/green
rm g_201401.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2014-02.csv
tail -n +2 "green_tripdata_2014-02.csv" > "g_201402.csv"
rm green_tripdata_2014-02.csv
hdfs dfs -put g_201402.csv /user/w205/green
rm g_201402.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2014-03.csv
tail -n +2 "green_tripdata_2014-03.csv" > "g_201403.csv"
rm green_tripdata_2014-03.csv
hdfs dfs -put g_201403.csv /user/w205/green
rm g_201403.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2014-04.csv
tail -n +2 "green_tripdata_2014-04.csv" > "g_201404.csv"
rm green_tripdata_2014-04.csv
hdfs dfs -put g_201404.csv /user/w205/green
rm g_201404.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2014-05.csv
tail -n +2 "green_tripdata_2014-05.csv" > "g_201405.csv"
rm green_tripdata_2014-05.csv
hdfs dfs -put g_201405.csv /user/w205/green
rm g_201405.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2014-06.csv
tail -n +2 "green_tripdata_2014-06.csv" > "g_201406.csv"
rm green_tripdata_2014-06.csv
hdfs dfs -put g_201406.csv /user/w205/green
rm g_201406.csv



# 2013
wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2013-08.csv
tail -n +2 "green_tripdata_2013-08.csv" > "g_201308.csv"
rm green_tripdata_2013-08.csv
hdfs dfs -put g_201308.csv /user/w205/green
rm g_201308.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2013-09.csv
tail -n +2 "green_tripdata_2013-09.csv" > "g_201309.csv"
rm green_tripdata_2013-09.csv
hdfs dfs -put g_201309.csv /user/w205/green
rm g_201309.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2013-10.csv
tail -n +2 "green_tripdata_2013-10.csv" > "g_201310.csv"
rm green_tripdata_2013-10.csv
hdfs dfs -put g_201310.csv /user/w205/green
rm g_201310.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2013-11.csv
tail -n +2 "green_tripdata_2013-11.csv" > "g_201311.csv"
rm green_tripdata_2013-11.csv
hdfs dfs -put g_201311.csv /user/w205/green
rm g_201311.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2013-12.csv
tail -n +2 "green_tripdata_2013-12.csv" > "g_201312.csv"
rm green_tripdata_2013-12.csv
hdfs dfs -put g_201312.csv /user/w205/green
rm g_201312.csv

