# W205Project

UCB W205 Group Project by John Blakkan, Rohit Nair, Andrea Pope

# Working notes

There are Seven main pieces:

## Ride data ETL

This is consolidated for several taxi/ride-hailing services (in hive).  Fundamental fields are:   Date/Time of pickup, Lat/Long location of pickup,
and number of passengers (for our purposes, fare information, taxes, etc. are not retained)

## Historical Weather ETL and join to the Ride Data

This is joined to the ride data, giving historical ride data with weather (in hive, as measured at NY LaGuardia Airport).  We get this data from NOAA at https://www.ncdc.noaa.gov/cdo-web/. Since we use this only for model building (a batch process), we only need
to pull it when the model is updated (e.g. not when current weather is updated).    NOAA requires this data be accessed
by a request for data return by email.

The data (ingested in .csv form) is lightly transformed (in excel or equivalent, although this could be scripted.)   Snowfall is accounted as rainfall in a 10:1
ratio (i.e. 10mm of snow = 1mm of rain).   Fields used are: Date, Total Precipitation (in tenths of mm), Daily Max temp (F), and, and Daily Min temp (F)

We implement a hash-join to join weather information to the ride data with pyspark code because the daily weather data is very
small compared to the ride pickup table (i.e. there are many taxi pickups per day, but we only have one overall weather report (with 24-hr max/min temp and precipition) per day.  In-day variances are co-linear with hour-of-day, so will have input to our model.

Pyspark map/lambda is used to perform this hash-join.

## Borough Identification transformation of Ride data

Our customer use case is to plan allocation of taxi/ride-service vehicles to Boroughs, so the lat/long co-ordinates in the Ride Data must be converted to borough.

We obtain this data by using the online SAAS at https://www.itouchmap.com/latlong.html to create bounding polygons for each borough.  Boroughs can be bounded with approximately 20 vertices (with water borders requiring fewer points, and complex street boundaries - such as between Brooklyn and Queens requiring more).  Note:  The points are defined by the project, not by the website, so there are not IP or Terms-of-Use issues.  The New York City government website was used as a reference for the borough borders (http://maps.nyc.gov/doitt/nycitymap/).

These immutable polygon vertices are used with matplotlib's point-in-polygon library to provide a transform function for pyspark's map-lambda to add borough identifiers to the Ride data.


## Predictive model, with parameters

This is produced from the ride/weather data, and placed in a (TBD) table (in postgres).


## Current weather forecast for NYC

This will be pulled every 10 min from NOAA's XML service (http://graphical.weather.gov/xml/sample_products/browser_interface/ndfdBrowserClientByDay.phpusing). Script pf2.py parses the XML and creates a postgres table 6-day weather_prediction.  pf2.py can be run from the command line, but for production will be run via crontab (at 0, 10, 20, 30, 40, 50 min after the hour).  This script cleans the data by translating the mixed 12hr/24hr results into uniform 24 hr. results, and transforming pairs of 12 hr precipitation probabilities into single 24hr probabilities (i.e. the 24 hr. probability is the max of the day and night probabilities).  Timestamps of the XML downloads are also kept, so that "stale" weather data can be identified.  We pull and cache (and transform) the data in this manner because we have observed that the site has a high variance in response time (up to 10s of seconds) and also becomes unavailable for minutes at a time.  This is only a 6 line table ("weather_prediction") so we use postgres.  Attention is paid to using transactions when modifying this table, and per-line timestamps are retained.

## Prediction

Then script ride_prediction.py takes the forecast from postgres table "weather_prediction" and the model parameters from postgres (TBD) table, and produces postgres table ride_prediction.   ride_prediction.py can be run from the command line, but for production will be run via crontab (at 5, 15, 25, 35, 45, and 55 min after the hour).  The output of this table is: For each of 5 boroughs, for each of 6 days, for each hour in each day, a predicted number of large taxis (4 or more passengers) and small vehicles (3 or fewer passengers).  The timestamp of the weather_forecast used to generate the prediction is also passed through, so the user can verify the age of the forecast used for the predictions.

 This 720 row table (5 * 6 * 24) is kept in postgres.

## Tableau presentation

Tableau will serve data from the ride_prediction table.  The predicted number of pickups
 * for each borough
 * for each hour
 * of the next 6 days
will be presented (along with appropriate graphs).   Users will also see the UTC timestamp of the last successful fetch of weather forecasts, to be informed in the (unlikely but possible) event that the forecast is out of date.  Our User-story is of a planner deciding how many taxi company vehicles to allocate to each borough, or for a ride hailing service to use to incentivize their drivers to position their private vehicles appropriately.
