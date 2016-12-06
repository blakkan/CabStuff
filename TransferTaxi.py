# run from folder want these to transfer through (doesn't make much difference, as original files get deleted)
#
# must have hdfs directories /user/w205/yellow, green, and fhv existing
# and empty before running this.

import os
import sys
import boto3

# putting into the /home/w205

#yields a list of year and month (in string form)
def generate_year_months_from(initial_year, initial_month):
    year = initial_year
    month = initial_month
    while True:
        yield [ str(year), str(month).zfill(2) ]
        if month >= 12:
            month = 1
            year = year + 1
        else:
            month = month + 1



#
# silently remove any old data and set up new
# directories
#
os.system("hdfs dfs -rm /user/w205/yellow/*")
os.system("hdfs dfs -rmdir /user/w205/yellow")
os.system("hdfs dfs -mkdir /user/w205/yellow")
os.system("hdfs dfs -rm /user/w205/green/*")
os.system("hdfs dfs -rmdir /user/w205/green")
os.system("hdfs dfs -mkdir /user/w205/green")
os.system("hdfs dfs -rm /user/w205/fhv/*")
os.system("hdfs dfs -rmdir /user/w205/fhv")
os.system("hdfs dfs -mkdir /user/w205/fhv")

#
# Main loop
#
for company in ["yellow", "green", "fhv"]:
    for year_month in generate_year_months_from(2015, 7):
        if ( year_month[0] == "2016" and year_month[1] == "07"):
            break
        s3_source_filename = "%s_tripdata_%s-%s.csv" % (company, year_month[0], year_month[1])
        local_filename = "%s_%s%s.csv" % (company[0], year_month[0], year_month[1])
        print("Starting transfer from s3 of %s to %s" % (s3_source_filename, local_filename))

	# If we want a percentage, pull with wget and decimate
	if len(sys.argv) <= 1:
		os.system("wget https://s3.amazonaws.com/nyc-tlc/trip+data/%s --no-check-certificate --no-verbose" % s3_source_filename)
		os.system("tail -n +2 %s > %s" % ( s3_source_filename, local_filename))

	elif float(sys.argv[1]) <= 1.0:
        	os.system("wget https://s3.amazonaws.com/nyc-tlc/trip+data/%s --no-check-certificate --no-verbose" % s3_source_filename)
        	os.system("tail -n +2 %s | python decimate.py %s > %s" % (s3_source_filename, sys.argv[1], local_filename))
	# if we want a certain number of characters, pull them with curl, then trim off the top and (possibly fragmentary) bottom line
	else:
		character_limit = str(int(sys.argv[1]))
		os.system("curl -r 0-%s -o %s https://s3.amazonaws.com/nyc-tlc/trip+data/%s" % (character_limit, s3_source_filename, s3_source_filename))
		os.system("tail -n +2 %s | head -n -1 > %s" % (s3_source_filename, local_filename))


	# either way, push into hdfs and remove
        os.remove(s3_source_filename)
        os.system("hdfs dfs -put %s /user/w205/%s" % (local_filename, company) )
        os.remove(local_filename)
