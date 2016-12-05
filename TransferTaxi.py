# run from folder want these to transfer through (doesn't make much difference, as original files get deleted)
import os
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

for company in ["yellow", "green", "fhv"]:
    for year_month in generate_year_months_from(2015, 7):
        if ( year_month[0] == "2016" and year_month[1] == "07"):
            exit(0)
        s3_source_filename = "%s_tripdata_%s-%s.csv" % (company, year_month[0], year_month[1])
        local_filename = "%s_%s%s.csv" % (company[0], year_month[0], year_month[1])
        print("Starting transfer from s3 of %s to %s" % (s3_source_filename, local_filename))
        os.system("wget https://s3.amazonaws.com/nyc-tlc/trip+data/%s --no-check-certificate --no-verbose" % s3_source_filename)

        os.system("tail -n +2 %s | python decimate.py 0.001 > %s" % (s3_source_filename, local_filename))
        os.remove(s3_source_filename)
        os.system("hdfs dfs -put %s /user/w205/%s" % (local_filename, company) )
        os.remove(local_filename)
