# W205Project

UCB W205 Group Project by John Blakkan, Rohit Nair, Andrea Pope

## Working notes

There are four main pieces:

# Ride data extraction

This is consolidated for several taxi/ride-hailing services (in hive)

# Historical Weather

This is joined to the ride data, giving historical ride data with weather (in hive).

# Predictive model, with parameters

This is produced from the ride/weather data, and placed in a (TBD) table (in postgres).

# Current weather forecast for NYC

This will be pulled every 10 min from NOAA using script pf2.py, which creates a postgres table weather_prediction.  pf2.py can be run from the command line, but for production will be run via crontab (at 0, 10, 20, 30, 40, 50 min after the hour).

Then script ride_prediction.py takes the forecast from postgres table weather_prediction and the model parameters from postgres (TBD) table, and produces postgres table ride_prediction.   ride_prediction.py can be run from the command line, but for production will be run via crontab (at 5, 15, 25, 35, 45, and 55 min after the hour).

# Tableau presentation

Tableau will access and present postgres table ride_prediction.
