#! /usr/bin/python


#
# put get the weather database
#
import psycopg2
import sys
try:
    conn = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password='pass'")
    conn.set_isolation_level(3)  #want to control our own committing
except:
    print "didn't connect to database"
    exit()


#
# Read in the weather forecast (expect it to be there)
#
cur_weather = conn.cursor()
cur_weather.execute("""SELECT * FROM weather_prediction;""")
weather_forecast = cur_weather.fetchall()

#
# Open, creating if necessary, the ride_prediction table
#

cur = conn.cursor()
cur.execute("""SELECT * FROM information_schema.tables WHERE table_name = 'ride_prediction';""")

res = cur.fetchone()

if ((res is None) or (res[0] == 0)):

    try:
        cur.execute("""CREATE TABLE ride_prediction (timestamp_of_prediction timestamp, neighborhood VARCHAR, date VARCHAR, hour_of_day INT, predicted_small_rides INT, predicted_large_rides INT );""")
    except:
        print "Error when attempting to create ride_prediction table"


# We're constantly trimming off the top of the table (but not until commit)

cur.execute("""DELETE FROM ride_prediction;""")  #using delete rather than truncate, because not sure if truncate
                                                    #plays nicely with transactions

#
# Write the prediction (this is just a stub, but it uses the weather prediction table
#
neighborhood = [ 'Brooklyn', 'Bronx', 'Manhattan', 'Queens', 'Staten Island' ]

def generate_prediction( place, weather_forecast, hour_of_day ):
  import datetime
  #Average ride rates (small and large cars) for each borough
  base_rides_data = { 'Brooklyn' : [ 100000, 40000 ], 'Bronx' : [120000, 30000], 
                      'Manhattan' : [180000, 60000], 'Queens' : [68000, 22000],
                      'Staten Island' : [ 70000, 15000 ] }
  base_rides = base_rides_data[place]

  #factor in hour of day
  if hour_of_day > 5 and hour_of_day < 9:
    base_rides[0] *= 1.5
    base_rides[1] *= 1.6
  elif hour_of_day > 16 and hour_of_day < 23:
    base_rides[0] *= 1.2
    base_rides[1] *= 1.4

  #factor in week of year (using iso8601 week).   More in winter
  year_month_day = [ int(x) for x in weather_forecast[1].split("-")]
  week_of_year = datetime.date(year_month_day[0], year_month_day[1], year_month_day[2]).isocalendar()[1]
  if week_of_year < 10:
    base_rides[0] *= 1.5
    base_rides[1] *= 1.6
  elif week_of_year > 40:
    base_rides[0] *= 1.2
    base_rides[1] *= 1.4

  #factor in extreme temperature
  if weather_forecast[2] > 94:
    base_rides[0] *= 1.1
    base_rides[1] *= 1.1
  elif weather_forecast[2] < 30:
    base_rides[0] *= 1.4
    base_rides[1] *= 1.4
  elif weather_forecast[2] < 38:
    base_rides[0] *= 1.1
    base_rides[1] *= 1.1

  #factor in impact of precipitation
  if weather_forecast[5] > 40:
    base_rides[0] *= 1.4
    base_rides[1] *= 1.4

  return [ int(round(base_rides[0])), int(round(base_rides[1])) ]


#
# put it in the database
#


for hood in neighborhood:
  for forecast in weather_forecast:
    for hour in range(0,24):
      prediction_list =  generate_prediction(hood, forecast, hour )

      try:
        cur.execute("""INSERT INTO ride_prediction (timestamp_of_prediction, neighborhood, date,  hour_of_day, predicted_small_rides, predicted_large_rides) VALUES ( %s, %s, %s, %s, %s, %s );""",
        (forecast[0], hood, forecast[1], hour, prediction_list[0], prediction_list[1]) )

      except:
       print "something went awry with the database write"
       print sys.exc_info()

conn.commit()
conn.close()
