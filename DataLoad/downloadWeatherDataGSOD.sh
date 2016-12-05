# Make directory data
mkdir -p data
# download weather data from GSOD ftp
curl ftp://ftp.ncdc.noaa.gov/pub/data/gsod/2012/725053-94728-2012.op.gz | gunzip > data/nyGSODWeather.csv
curl ftp://ftp.ncdc.noaa.gov/pub/data/gsod/2013/725053-94728-2013.op.gz | gunzip | tail -n +2 >> data/nyGSODWeather.csv
curl ftp://ftp.ncdc.noaa.gov/pub/data/gsod/2014/725053-94728-2014.op.gz | gunzip | tail -n +2 >> data/nyGSODWeather.csv
curl ftp://ftp.ncdc.noaa.gov/pub/data/gsod/2015/725053-94728-2015.op.gz | gunzip | tail -n +2 >> data/nyGSODWeather.csv
curl ftp://ftp.ncdc.noaa.gov/pub/data/gsod/2016/725053-94728-2016.op.gz | gunzip | tail -n +2 >> data/nyGSODWeather.csv
# Parse weather to csv
python parseWeatherData.py
# Upload to HDFS
hdfs dfs -mkdir -p /user/w205/ny_weather
cat ./data/ny_weather.csv | tail -n +2 | hdfs dfs -put - /user/w205/ny_weather/ny_weather.csv 
