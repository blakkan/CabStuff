# run from folder want these to transfer through (doesn't make much difference, as original files get deleted)
import os
# putting into the /home/w205

for company in ["yellow", "green", "fhv"]:
    for year in ["2015", "2016"]:
        for month in ["07", "08"]:
        #for month in ["07", "08", "09", "10", "11", "12", "01", "02", "03", "04", "05", "06"]:
            s3_source_filename = "%s_tripdata_%s-%s.csv" % (company, year, month)
            local_filename = "%s_%s%s.csv" % (company, year, month)
            print("Starting transfer from s3 of %s to %s" % (s3_source_filename, local_filename))
            os.system("wget https://s3.amazonaws.com/nyc-tlc/trip+data/%s --no-check-certificate --no-verbose" % s3_source_filename)

            os.system("tail -n +2 %s | python decimate.py 0.001 > %s" % (s3_source_filename, local_filename[0]))
            os.remove(s3_source_filename)
            #hdfs dfs -put y_201601.csv /user/w205/yellow
            #rm y_201601.csv
