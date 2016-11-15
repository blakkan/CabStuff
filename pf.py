import urllib2

uri = 'http://graphical.weather.gov/xml/sample_products/browser_interface/ndfdBrowserClientByDay.php'
args = "lat=40.77&lon=-73.98&format=24+hourly&numDays=7" #new york city
response = urllib2.urlopen(uri + '?' + args)
the_page = response.read()

#print the_page, for future reference
fh = open("output.txt", "w")
fh.write(the_page)
fh.close

import libxml2
doc = libxml2.parseDoc(the_page)

context = doc.xpathNewContext()


start_times_zone = context.xpathEval("//data/time-layout[@summarization='24hourly']/layout-key[.='k-p24h-n7-1']/../start-valid-time")


max_temps = context.xpathEval("//data/parameters/temperature[@type='maximum']/value")


min_temps = context.xpathEval("//data/parameters/temperature[@type='minimum']/value")

prob_precip_12_hour = context.xpathEval("//data/parameters/probability-of-precipitation[@type='12 hour']/value")

weathers = context.xpathEval("//data/parameters/weather/weather-conditions/@weather-summary")

#convert 12 hr max probability of precip to 24 hr by conslidating pairs

prob_precip_24_hr = []
for idx, val in enumerate(prob_precip_12_hour):
    localVal = val.getContent()
    if localVal.isdigit():  #Done at the end of list; have a nil value from NOAA there.
      if idx % 2 == 1: #if odd index, emit.  And yes, we lose the last one if it's an odd number.  This is OK.
          prob_precip_24_hr.append( str(max(int(val.getContent()), int(pairmax))) )
      else:
          pairmax = val.getContent()



#convert the ISO time/date format (starting at 6:00am local times) to a simple day format
start_times = map(lambda x: x.getContent().split('T')[0], start_times_zone[0:6])

#Build a consolidated list to put in the database
the_data = []
for idx, val in enumerate(start_times):
   the_data.append({"start":val, "max":max_temps[idx].getContent(), "min":min_temps[idx].getContent(), "weather":weathers[idx].getContent(),
      "prob_precip":prob_precip_24_hr[idx] })


#
# put in database
#
import psycopg2
try:
    conn = psycopg2.connect("dbname='john' user='john' host='localhost' password='john'")
except:
    print "didn't connect to database"
    exit()


cur = conn.cursor()
cur.execute("""TRUNCATE TABLE weather;""")
for idx, val in enumerate(start_times):
    print idx, val, max_temps[idx].getContent(), min_temps[idx].getContent(), weathers[idx].getContent(), prob_precip_24_hr[idx]
    try:
      cur.execute("""INSERT INTO weather (start,max,min,weather,prob_precip) VALUES ( %s, %s, %s, %s, %s );""",
        (val, max_temps[idx].getContent(), min_temps[idx].getContent(), weathers[idx].getContent(), prob_precip_24_hr[idx]))

    except:
      print "something went awry with the database write"
#cur.executemany("""INSERT INTO weather(start,max,min,weather) VALUES (%(start)s, %(max)s, %(min)s, %(weather)s)""", the_data)
conn.commit()
conn.close()
