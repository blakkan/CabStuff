#!/usr/bin/env python
import csv
import gzip
import datetime
from cStringIO import StringIO

def str_to_datetime(val):
    return val and datetime.datetime.strptime(val, '%Y%m%d')

def parse_weather_record(line):
    """
    Parse one line of the weather record
    The parsing is done according to
    ftp://ftp.ncdc.noaa.gov/pub/data/gsod/readme.txt
    """
    field_names = ('date',
                   'temp', '_',
                   'dew_point', '_',
                   'sea_level_pressure', '_',
                   'station_pressure', '_',
                   'visibility', '_',
                   'wind_speed', '_',
                   'max_wind_speed',
                   'max_wind_gust',
                   'max_temp',  # has *
                   'min_temp',  # has *
                   'precipitation', # has flags A-I
                   'snow_depth',
                   'indicators')
    # list of fields which values have to be converted to floats
    float_field_names = (
        'temp', 'dew_point', 'sea_level_pressure', 'station_pressure',
        'visibility', 'wind_speed', 'max_wind_speed', 'max_wind_gust',
        'max_temp', 'min_temp', 'precipitation', 'snow_depth',
    )

    # base conversion
    fields = line.strip().split()[2:]
    fields = [field.strip() or None for field in fields]
    obj = dict(zip(field_names, fields))
    obj['date'] = str_to_datetime(obj['date'])
    # float conversion
    for field_name in float_field_names:
        value = obj[field_name].rstrip('*ABCDEFGHI')
        if value in ('9999.9', '99.99', '999.9'):
            value = None
        else:
            value = float(value)
        obj[field_name] = value
    # measurement units conversion
    temp_field_names = ('temp', 'max_temp', 'min_temp')
    for field_name in temp_field_names:
        celsius_field_name = field_name + '_c'
        value = obj[field_name]
        if value is not None:
            obj[celsius_field_name] = (value - 32) * 5 / 9
        else:
            obj[celsius_field_name] = None
    # parsing indicator values
    indicator_names = ('fog', 'rain', 'snow', 'hail', 'thunder', 'tornado')
    indicator_values = [flag == '1' for flag in obj['indicators']]
    indicator_obj = dict(zip(indicator_names, indicator_values))
    obj['weather_ok'] = not any(indicator_values)
    obj.update(indicator_obj)
    # finalization
    del obj['_']
    return obj

weather_dat = []
with open("./data/nyGSODWeather.csv") as f:
        header = f.readline()
	for line in f:
        	weather_dat.append(parse_weather_record(line))

keys =  weather_dat[0].keys()
with open('./data/ny_weather.csv', 'wb') as output_file:
	dict_writer = csv.DictWriter(output_file, keys)
	dict_writer.writeheader()
	dict_writer.writerows(weather_dat)
