# run from folder want these to transfer through (doesn't make much difference, as original files get deleted)

# putting into the /home/w205

# 2016
wget https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_2016-01.csv
tail -n +2 "fhv_tripdata_2016-01.csv" > "f_201601.csv"
rm fhv_tripdata_2016-01.csv
hdfs dfs -put f_201601.csv /user/w205/fhv
rm f_201601.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_2016-02.csv
tail -n +2 "fhv_tripdata_2016-02.csv" > "f_201602.csv"
rm fhv_tripdata_2016-02.csv
hdfs dfs -put f_201602.csv /user/w205/fhv
rm f_201602.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_2016-03.csv
tail -n +2 "fhv_tripdata_2016-03.csv" > "f_201603.csv"
rm fhv_tripdata_2016-03.csv
hdfs dfs -put f_201603.csv /user/w205/fhv
rm f_201603.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_2016-04.csv
tail -n +2 "fhv_tripdata_2016-04.csv" > "f_201604.csv"
rm fhv_tripdata_2016-04.csv
hdfs dfs -put f_201604.csv /user/w205/fhv
rm f_201604.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_2016-05.csv
tail -n +2 "fhv_tripdata_2016-05.csv" > "f_201605.csv"
rm fhv_tripdata_2016-05.csv
hdfs dfs -put f_201605.csv /user/w205/fhv
rm f_201605.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_2016-06.csv
tail -n +2 "fhv_tripdata_2016-06.csv" > "f_201606.csv"
rm fhv_tripdata_2016-06.csv
hdfs dfs -put f_201606.csv /user/w205/fhv
rm f_201606.csv


# 2015

# *****
wget https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_2015-07.csv
tail -n +2 "fhv_tripdata_2015-07.csv" > "f_201507.csv"
rm fhv_tripdata_2015-07.csv
hdfs dfs -put f_201507.csv /user/w205/fhv
rm f_201507.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_2015-08.csv
tail -n +2 "fhv_tripdata_2015-08.csv" > "f_201508.csv"
rm fhv_tripdata_2015-08.csv
hdfs dfs -put f_201508.csv /user/w205/fhv
rm f_201508.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_2015-09.csv
tail -n +2 "fhv_tripdata_2015-09.csv" > "f_201509.csv"
rm fhv_tripdata_2015-09.csv
hdfs dfs -put f_201509.csv /user/w205/fhv
rm f_201509.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_2015-10.csv
tail -n +2 "fhv_tripdata_2015-10.csv" > "f_201510.csv"
rm fhv_tripdata_2015-10.csv
hdfs dfs -put f_201510.csv /user/w205/fhv
rm f_201510.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_2015-11.csv
tail -n +2 "fhv_tripdata_2015-11.csv" > "f_201511.csv"
rm fhv_tripdata_2015-11.csv
hdfs dfs -put f_201511.csv /user/w205/fhv
rm f_201511.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_2015-12.csv
tail -n +2 "fhv_tripdata_2015-12.csv" > "f_201512.csv"
rm fhv_tripdata_2015-12.csv
hdfs dfs -put f_201512.csv /user/w205/fhv
rm f_201512.csv

# early 2015
wget https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_2015-01.csv
tail -n +2 "fhv_tripdata_2015-01.csv" > "f_201501.csv"
rm fhv_tripdata_2015-01.csv
hdfs dfs -put f_201501.csv /user/w205/fhv
rm f_201501.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_2015-02.csv
tail -n +2 "fhv_tripdata_2015-02.csv" > "f_201502.csv"
rm fhv_tripdata_2015-02.csv
hdfs dfs -put f_201502.csv /user/w205/fhv
rm f_201502.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_2015-03.csv
tail -n +2 "fhv_tripdata_2015-03.csv" > "f_201503.csv"
rm fhv_tripdata_2015-03.csv
hdfs dfs -put f_201503.csv /user/w205/fhv
rm f_201503.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_2015-04.csv
tail -n +2 "fhv_tripdata_2015-04.csv" > "f_201504.csv"
rm fhv_tripdata_2015-04.csv
hdfs dfs -put f_201504.csv /user/w205/fhv
rm f_201504.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_2015-05.csv
tail -n +2 "fhv_tripdata_2015-05.csv" > "f_201505.csv"
rm fhv_tripdata_2015-05.csv
hdfs dfs -put f_201505.csv /user/w205/fhv
rm f_201505.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_2015-06.csv
tail -n +2 "fhv_tripdata_2015-06.csv" > "f_201506.csv"
rm fhv_tripdata_2015-06.csv
hdfs dfs -put f_201506.csv /user/w205/fhv
rm f_201506.csv
