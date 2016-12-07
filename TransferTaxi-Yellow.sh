# run from folder want these to transfer to

# putting into the /home/w205

# 2016
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-01.csv
tail -n +2 "yellow_tripdata_2016-01.csv" > "y_201601.csv"
rm yellow_tripdata_2016-01.csv
hdfs dfs -put y_201601.csv /user/w205/yellow
rm y_201601.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-02.csv
tail -n +2 "yellow_tripdata_2016-02.csv" > "y_201602.csv"
rm yellow_tripdata_2016-02.csv
hdfs dfs -put y_201602.csv /user/w205/yellow

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-03.csv
tail -n +2 "yellow_tripdata_2016-03.csv" > "y_201603.csv"
rm yellow_tripdata_2016-03.csv
hdfs dfs -put y_201603.csv /user/w205/yellow
rm y_201603.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-04.csv
tail -n +2 "yellow_tripdata_2016-04.csv" > "y_201604.csv"
rm yellow_tripdata_2016-04.csv
hdfs dfs -put y_201604.csv /user/w205/yellow
rm y_201604.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-05.csv
tail -n +2 "yellow_tripdata_2016-05.csv" > "y_201605.csv"
rm yellow_tripdata_2016-05.csv
hdfs dfs -put y_201605.csv /user/w205/yellow
rm y_201605.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-06.csv
tail -n +2 "yellow_tripdata_2016-06.csv" > "y_201606.csv"
rm yellow_tripdata_2016-06.csv
hdfs dfs -put y_201606.csv /user/w205/yellow
rm y_201606.csv


# 2015
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-01.csv
tail -n +2 "yellow_tripdata_2015-01.csv" > "y_201501.csv"
rm yellow_tripdata_2015-01.csv
hdfs dfs -put y_201501.csv /user/w205/yellow
rm y_201501.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-02.csv
tail -n +2 "yellow_tripdata_2015-02.csv" > "y_201502.csv"
rm yellow_tripdata_2015-02.csv
hdfs dfs -put y_201502.csv /user/w205/yellow
rm y_201502.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-03.csv
tail -n +2 "yellow_tripdata_2015-03.csv" > "y_201503.csv"
rm yellow_tripdata_2015-03.csv
hdfs dfs -put y_201503.csv /user/w205/yellow
rm y_201503.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-04.csv
tail -n +2 "yellow_tripdata_2015-04.csv" > "y_201504.csv"
rm yellow_tripdata_2015-04.csv
hdfs dfs -put y_201504.csv /user/w205/yellow
rm y_201504.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-05.csv
tail -n +2 "yellow_tripdata_2015-05.csv" > "y_201505.csv"
rm yellow_tripdata_2015-05.csv
hdfs dfs -put y_201505.csv /user/w205/yellow
rm y_201505.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-06.csv
tail -n +2 "yellow_tripdata_2015-06.csv" > "y_201506.csv"
rm yellow_tripdata_2015-06.csv
hdfs dfs -put y_201506.csv /user/w205/yellow
rm y_201506.csv

# *****
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-07.csv
tail -n +2 "yellow_tripdata_2015-07.csv" > "y_201507.csv"
rm yellow_tripdata_2015-07.csv
hdfs dfs -put y_201507.csv /user/w205/yellow
rm y_201507.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-08.csv
tail -n +2 "yellow_tripdata_2015-08.csv" > "y_201508.csv"
rm yellow_tripdata_2015-08.csv
hdfs dfs -put y_201508.csv /user/w205/yellow
rm y_201508.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-09.csv
tail -n +2 "yellow_tripdata_2015-09.csv" > "y_201509.csv"
rm yellow_tripdata_2015-09.csv
hdfs dfs -put y_201509.csv /user/w205/yellow
rm y_201509.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-10.csv
tail -n +2 "yellow_tripdata_2015-10.csv" > "y_201510.csv"
rm yellow_tripdata_2015-10.csv
hdfs dfs -put y_201510.csv /user/w205/yellow
rm y_201510.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-11.csv
tail -n +2 "yellow_tripdata_2015-11.csv" > "y_201511.csv"
rm yellow_tripdata_2015-11.csv
hdfs dfs -put y_201511.csv /user/w205/yellow
rm y_201511.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-12.csv
tail -n +2 "yellow_tripdata_2015-12.csv" > "y_201512.csv"
rm yellow_tripdata_2015-12.csv
hdfs dfs -put y_201512.csv /user/w205/yellow
rm y_201512.csv


# 2014
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-01.csv
tail -n +2 "yellow_tripdata_2014-01.csv" > "y_201401.csv"
rm yellow_tripdata_2014-01.csv
hdfs dfs -put y_201401.csv /user/w205/yellow
rm y_201401.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-02.csv
tail -n +2 "yellow_tripdata_2014-02.csv" > "y_201402.csv"
rm yellow_tripdata_2014-02.csv
hdfs dfs -put y_201402.csv /user/w205/yellow
rm y_201402.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-03.csv
tail -n +2 "yellow_tripdata_2014-03.csv" > "y_201403.csv"
rm yellow_tripdata_2014-03.csv
hdfs dfs -put y_201403.csv /user/w205/yellow
rm y_201403.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-04.csv
tail -n +2 "yellow_tripdata_2014-04.csv" > "y_201404.csv"
rm yellow_tripdata_2014-04.csv
hdfs dfs -put y_201404.csv /user/w205/yellow
rm y_201404.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-05.csv
tail -n +2 "yellow_tripdata_2014-05.csv" > "y_201405.csv"
rm yellow_tripdata_2014-05.csv
hdfs dfs -put y_201405.csv /user/w205/yellow
rm y_201405.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-06.csv
tail -n +2 "yellow_tripdata_2014-06.csv" > "y_201406.csv"
rm yellow_tripdata_2014-06.csv
hdfs dfs -put y_201406.csv /user/w205/yellow
rm y_201406.csv

#***
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-07.csv
tail -n +2 "yellow_tripdata_2014-07.csv" > "y_201407.csv"
rm yellow_tripdata_2014-07.csv
hdfs dfs -put y_201407.csv /user/w205/yellow
rm y_201407.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-08.csv
tail -n +2 "yellow_tripdata_2014-08.csv" > "y_201408.csv"
rm yellow_tripdata_2014-08.csv
hdfs dfs -put y_201408.csv /user/w205/yellow
rm y_201408.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-09.csv
tail -n +2 "yellow_tripdata_2014-09.csv" > "y_201409.csv"
rm yellow_tripdata_2014-09.csv
hdfs dfs -put y_201409.csv /user/w205/yellow
rm y_201409.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-10.csv
tail -n +2 "yellow_tripdata_2014-10.csv" > "y_201410.csv"
rm yellow_tripdata_2014-10.csv
hdfs dfs -put y_201410.csv /user/w205/yellow
rm y_201410.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-11.csv
tail -n +2 "yellow_tripdata_2014-11.csv" > "y_201411.csv"
rm yellow_tripdata_2014-11.csv
hdfs dfs -put y_201411.csv /user/w205/yellow
rm y_201411.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-12.csv
tail -n +2 "yellow_tripdata_2014-12.csv" > "y_201412.csv"
rm yellow_tripdata_2014-12.csv
hdfs dfs -put y_201412.csv /user/w205/yellow
rm y_201412.csv


# 2013
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2013-01.csv
tail -n +2 "yellow_tripdata_2013-01.csv" > "y_201301.csv"
rm yellow_tripdata_2013-01.csv
hdfs dfs -put y_201301.csv /user/w205/yellow
rm y_201301.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2013-02.csv
tail -n +2 "yellow_tripdata_2013-02.csv" > "y_201302.csv"
rm yellow_tripdata_2013-02.csv
hdfs dfs -put y_201302.csv /user/w205/yellow
rm y_201302.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2013-03.csv
tail -n +2 "yellow_tripdata_2013-03.csv" > "y_201303.csv"
rm yellow_tripdata_2013-03.csv
hdfs dfs -put y_201303.csv /user/w205/yellow
rm y_201303.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2013-04.csv
tail -n +2 "yellow_tripdata_2013-04.csv" > "y_201304.csv"
rm yellow_tripdata_2013-04.csv
hdfs dfs -put y_201304.csv /user/w205/yellow
rm y_201304.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2013-05.csv
tail -n +2 "yellow_tripdata_2013-05.csv" > "y_201305.csv"
rm yellow_tripdata_2013-05.csv
hdfs dfs -put y_201305.csv /user/w205/yellow
rm y_201305.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2013-06.csv
tail -n +2 "yellow_tripdata_2013-06.csv" > "y_201306.csv"
rm yellow_tripdata_2013-06.csv
hdfs dfs -put y_201306.csv /user/w205/yellow
rm y_201306.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2013-07.csv
tail -n +2 "yellow_tripdata_2013-07.csv" > "y_201307.csv"
rm yellow_tripdata_2013-07.csv
hdfs dfs -put y_201307.csv /user/w205/yellow
rm y_201307.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2013-08.csv
tail -n +2 "yellow_tripdata_2013-08.csv" > "y_201308.csv"
rm yellow_tripdata_2013-08.csv
hdfs dfs -put y_201308.csv /user/w205/yellow
rm y_201308.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2013-09.csv
tail -n +2 "yellow_tripdata_2013-09.csv" > "y_201309.csv"
rm yellow_tripdata_2013-09.csv
hdfs dfs -put y_201309.csv /user/w205/yellow
rm y_201309.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2013-10.csv
tail -n +2 "yellow_tripdata_2013-10.csv" > "y_201310.csv"
rm yellow_tripdata_2013-10.csv
hdfs dfs -put y_201310.csv /user/w205/yellow
rm y_201310.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2013-11.csv
tail -n +2 "yellow_tripdata_2013-11.csv" > "y_201311.csv"
rm yellow_tripdata_2013-11.csv
hdfs dfs -put y_201311.csv /user/w205/yellow
rm y_201311.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2013-12.csv
tail -n +2 "yellow_tripdata_2013-12.csv" > "y_201312.csv"
rm yellow_tripdata_2013-12.csv
hdfs dfs -put y_201312.csv /user/w205/yellow
rm y_201312.csv

# 2012
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2012-01.csv
tail -n +2 "yellow_tripdata_2012-01.csv" > "y_201201.csv"
rm yellow_tripdata_2012-01.csv
hdfs dfs -put y_201201.csv /user/w205/yellow
rm y_201201.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2012-02.csv
tail -n +2 "yellow_tripdata_2012-02.csv" > "y_201202.csv"
rm yellow_tripdata_2012-02.csv
hdfs dfs -put y_201202.csv /user/w205/yellow
rm y_201202.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2012-03.csv
tail -n +2 "yellow_tripdata_2012-03.csv" > "y_201203.csv"
rm yellow_tripdata_2012-03.csv
hdfs dfs -put y_201203.csv /user/w205/yellow
rm y_201203.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2012-04.csv
tail -n +2 "yellow_tripdata_2012-04.csv" > "y_201204.csv"
rm yellow_tripdata_2012-04.csv
hdfs dfs -put y_201204.csv /user/w205/yellow
rm y_201204.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2012-05.csv
tail -n +2 "yellow_tripdata_2012-05.csv" > "y_201205.csv"
rm yellow_tripdata_2012-05.csv
hdfs dfs -put y_201205.csv /user/w205/yellow
rm y_201205.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2012-06.csv
tail -n +2 "yellow_tripdata_2012-06.csv" > "y_201206.csv"
rm yellow_tripdata_2012-06.csv
hdfs dfs -put y_201206.csv /user/w205/yellow
rm y_201206.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2012-07.csv
tail -n +2 "yellow_tripdata_2012-07.csv" > "y_201207.csv"
rm yellow_tripdata_2012-07.csv
hdfs dfs -put y_201207.csv /user/w205/yellow
rm y_201207.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2012-08.csv
tail -n +2 "yellow_tripdata_2012-08.csv" > "y_201208.csv"
rm yellow_tripdata_2012-08.csv
hdfs dfs -put y_201208.csv /user/w205/yellow
rm y_201208.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2012-09.csv
tail -n +2 "yellow_tripdata_2012-09.csv" > "y_201209.csv"
rm yellow_tripdata_2012-09.csv
hdfs dfs -put y_201209.csv /user/w205/yellow
rm y_201209.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2012-10.csv
tail -n +2 "yellow_tripdata_2012-10.csv" > "y_201210.csv"
rm yellow_tripdata_2012-10.csv
hdfs dfs -put y_201210.csv /user/w205/yellow
rm y_201210.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2012-11.csv
tail -n +2 "yellow_tripdata_2012-11.csv" > "y_201211.csv"
rm yellow_tripdata_2012-11.csv
hdfs dfs -put y_201211.csv /user/w205/yellow
rm y_201211.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2012-12.csv
tail -n +2 "yellow_tripdata_2012-12.csv" > "y_201212.csv"
rm yellow_tripdata_2012-12.csv
hdfs dfs -put y_201212.csv /user/w205/yellow
rm y_201212.csv

# 2011
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2011-01.csv
tail -n +2 "yellow_tripdata_2011-01.csv" > "y_201101.csv"
rm yellow_tripdata_2011-01.csv
hdfs dfs -put y_201101.csv /user/w205/yellow
rm y_201101.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2011-02.csv
tail -n +2 "yellow_tripdata_2011-02.csv" > "y_201102.csv"
rm yellow_tripdata_2011-02.csv
hdfs dfs -put y_201102.csv /user/w205/yellow
rm y_201102.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2011-03.csv
tail -n +2 "yellow_tripdata_2011-03.csv" > "y_201103.csv"
rm yellow_tripdata_2011-03.csv
hdfs dfs -put y_201103.csv /user/w205/yellow
rm y_201103.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2011-04.csv
tail -n +2 "yellow_tripdata_2011-04.csv" > "y_201104.csv"
rm yellow_tripdata_2011-04.csv
hdfs dfs -put y_201104.csv /user/w205/yellow
rm y_201104.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2011-05.csv
tail -n +2 "yellow_tripdata_2011-05.csv" > "y_201105.csv"
rm yellow_tripdata_2011-05.csv
hdfs dfs -put y_201105.csv /user/w205/yellow
rm y_201105.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2011-06.csv
tail -n +2 "yellow_tripdata_2011-06.csv" > "y_201106.csv"
rm yellow_tripdata_2011-06.csv
hdfs dfs -put y_201106.csv /user/w205/yellow
rm y_201106.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2011-07.csv
tail -n +2 "yellow_tripdata_2011-07.csv" > "y_201107.csv"
rm yellow_tripdata_2011-07.csv
hdfs dfs -put y_201107.csv /user/w205/yellow
rm y_201107.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2011-08.csv
tail -n +2 "yellow_tripdata_2011-08.csv" > "y_201108.csv"
rm yellow_tripdata_2011-08.csv
hdfs dfs -put y_201108.csv /user/w205/yellow
rm y_201108.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2011-09.csv
tail -n +2 "yellow_tripdata_2011-09.csv" > "y_201109.csv"
rm yellow_tripdata_2011-09.csv
hdfs dfs -put y_201109.csv /user/w205/yellow
rm y_201109.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2011-10.csv
tail -n +2 "yellow_tripdata_2011-10.csv" > "y_201110.csv"
rm yellow_tripdata_2011-10.csv
hdfs dfs -put y_201110.csv /user/w205/yellow
rm y_201110.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2011-11.csv
tail -n +2 "yellow_tripdata_2011-11.csv" > "y_201111.csv"
rm yellow_tripdata_2011-11.csv
hdfs dfs -put y_201111.csv /user/w205/yellow
rm y_201111.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2011-12.csv
tail -n +2 "yellow_tripdata_2011-12.csv" > "y_201112.csv"
rm yellow_tripdata_2011-12.csv
hdfs dfs -put y_201112.csv /user/w205/yellow
rm y_201112.csv


# 2010
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2010-01.csv
tail -n +2 "yellow_tripdata_2010-01.csv" > "y_201001.csv"
rm yellow_tripdata_2010-01.csv
hdfs dfs -put y_201001.csv /user/w205/yellow
rm y_201001.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2010-02.csv
tail -n +2 "yellow_tripdata_2010-02.csv" > "y_201002.csv"
rm yellow_tripdata_2010-02.csv
hdfs dfs -put y_201002.csv /user/w205/yellow
rm y_201002.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2010-03.csv
tail -n +2 "yellow_tripdata_2010-03.csv" > "y_201003.csv"
rm yellow_tripdata_2010-03.csv
hdfs dfs -put y_201003.csv /user/w205/yellow
rm y_201003.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2010-04.csv
tail -n +2 "yellow_tripdata_2010-04.csv" > "y_201004.csv"
rm yellow_tripdata_2010-04.csv
hdfs dfs -put y_201004.csv /user/w205/yellow
rm y_201004.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2010-05.csv
tail -n +2 "yellow_tripdata_2010-05.csv" > "y_201005.csv"
rm yellow_tripdata_2010-05.csv
hdfs dfs -put y_201005.csv /user/w205/yellow
rm y_201005.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2010-06.csv
tail -n +2 "yellow_tripdata_2010-06.csv" > "y_201006.csv"
rm yellow_tripdata_2010-06.csv
hdfs dfs -put y_201006.csv /user/w205/yellow
rm y_201006.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2010-07.csv
tail -n +2 "yellow_tripdata_2010-07.csv" > "y_201007.csv"
rm yellow_tripdata_2010-07.csv
hdfs dfs -put y_201007.csv /user/w205/yellow
rm y_201007.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2010-08.csv
tail -n +2 "yellow_tripdata_2010-08.csv" > "y_201008.csv"
rm yellow_tripdata_2010-08.csv
hdfs dfs -put y_201008.csv /user/w205/yellow
rm y_201008.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2010-09.csv
tail -n +2 "yellow_tripdata_2010-09.csv" > "y_201009.csv"
rm yellow_tripdata_2010-09.csv
hdfs dfs -put y_201009.csv /user/w205/yellow
rm y_201009.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2010-10.csv
tail -n +2 "yellow_tripdata_2010-10.csv" > "y_201010.csv"
rm yellow_tripdata_2010-10.csv
hdfs dfs -put y_201010.csv /user/w205/yellow
rm y_201010.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2010-11.csv
tail -n +2 "yellow_tripdata_2010-11.csv" > "y_201011.csv"
rm yellow_tripdata_2010-11.csv
hdfs dfs -put y_201011.csv /user/w205/yellow
rm y_201011.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2010-12.csv
tail -n +2 "yellow_tripdata_2010-12.csv" > "y_201012.csv"
rm yellow_tripdata_2010-12.csv
hdfs dfs -put y_201012.csv /user/w205/yellow
rm y_201012.csv



# 2009
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2009-01.csv
tail -n +2 "yellow_tripdata_2009-01.csv" > "y_200901.csv"
rm yellow_tripdata_2009-01.csv
hdfs dfs -put y_200901.csv /user/w205/yellow
rm y_200901.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2009-02.csv
tail -n +2 "yellow_tripdata_2009-02.csv" > "y_200902.csv"
rm yellow_tripdata_2009-02.csv
hdfs dfs -put y_200902.csv /user/w205/yellow
rm y_200902.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2009-03.csv
tail -n +2 "yellow_tripdata_2009-03.csv" > "y_200903.csv"
rm yellow_tripdata_2009-03.csv
hdfs dfs -put y_200903.csv /user/w205/yellow
rm y_200903.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2009-04.csv
tail -n +2 "yellow_tripdata_2009-04.csv" > "y_200904.csv"
rm yellow_tripdata_2009-04.csv
hdfs dfs -put y_200904.csv /user/w205/yellow
rm y_200904.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2009-05.csv
tail -n +2 "yellow_tripdata_2009-05.csv" > "y_200905.csv"
rm yellow_tripdata_2009-05.csv
hdfs dfs -put y_200905.csv /user/w205/yellow
rm y_200905.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2009-06.csv
tail -n +2 "yellow_tripdata_2009-06.csv" > "y_200906.csv"
rm yellow_tripdata_2009-06.csv
hdfs dfs -put y_200906.csv /user/w205/yellow
rm y_200906.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2009-07.csv
tail -n +2 "yellow_tripdata_2009-07.csv" > "y_200907.csv"
rm yellow_tripdata_2009-07.csv
hdfs dfs -put y_200907.csv /user/w205/yellow
rm y_200907.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2009-08.csv
tail -n +2 "yellow_tripdata_2009-08.csv" > "y_200908.csv"
rm yellow_tripdata_2009-08.csv
hdfs dfs -put y_200908.csv /user/w205/yellow
rm y_200908.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2009-09.csv
tail -n +2 "yellow_tripdata_2009-09.csv" > "y_200909.csv"
rm yellow_tripdata_2009-09.csv
hdfs dfs -put y_200909.csv /user/w205/yellow
rm y_200909.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2009-10.csv
tail -n +2 "yellow_tripdata_2009-10.csv" > "y_200910.csv"
rm yellow_tripdata_2009-10.csv
hdfs dfs -put y_200910.csv /user/w205/yellow
rm y_200910.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2009-11.csv
tail -n +2 "yellow_tripdata_2009-11.csv" > "y_200911.csv"
rm yellow_tripdata_2009-11.csv
hdfs dfs -put y_200911.csv /user/w205/yellow
rm y_200911.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2009-12.csv
tail -n +2 "yellow_tripdata_2009-12.csv" > "y_200912.csv"
rm yellow_tripdata_2009-12.csv
hdfs dfs -put y_200912.csv /user/w205/yellow
rm y_200912.csv

