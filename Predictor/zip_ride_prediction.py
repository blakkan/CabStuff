from datetime import datetime
import numpy as np
import psycopg2
import sys

from datetime import datetime
from sklearn.externals import joblib
from sklearn.feature_extraction import DictVectorizer
from sklearn import linear_model

#Feature Extraction
def get_feature_dict(x):
    feature_dict = {}
    # Get pickup date
    pickup_date = datetime.strptime("%s-%s-%s" % (x[feature_idx["pickup_year"]],\
                                     x[feature_idx["pickup_month"]],\
                                     x[feature_idx["pickup_day"]]), '%Y-%m-%d')
    
    feature_dict["zipcode"] = x[feature_idx["pickup_zipcode"]]
    feature_dict["month"] = x[feature_idx["pickup_month"]].zfill(2)
    feature_dict["day"] = x[feature_idx["pickup_day"]].zfill(2)
    feature_dict["weekday"] = '%02d' % pickup_date.weekday()
    
    #mean temp
    if float(x[feature_idx["mean_temp"]]) < 55.:
        feature_dict["temp"] = "Cold"
    elif float(x[feature_idx["mean_temp"]]) > 75.:
        feature_dict["temp"] = "Hot"
    else:
        feature_dict["temp"] = "Normal"
        
    #mean wind speed
    feature_dict["wind_speed"] = "%0d" % (float(x[feature_idx["mean_wind_speed"]] or 12.))
        
    #mean wind speed
    feature_dict["precip"] = "%00d" % (float(x[feature_idx["precipitation"]] or 10.))
    
    #zipcode-weekday
    feature_dict["zipcode_weekday"] = "%s_%s" % (feature_dict["zipcode"], feature_dict["weekday"])
    
    #zipcode-weekday-precip
    feature_dict["zipcode_weekday_precip"] = "%s_%s_%s" % (feature_dict["zipcode"], feature_dict["weekday"], feature_dict["precip"])
    
    #zipcode-weekday-temp
    feature_dict["zipcode_weekday_temp"] = "%s_%s_%s" % (feature_dict["zipcode"], feature_dict["weekday"], feature_dict["temp"])
    
    #zipcode-weekday-wind
    feature_dict["zipcode_weekday_wind"] = "%s_%s_%s" % (feature_dict["zipcode"], feature_dict["weekday"], feature_dict["wind_speed"])
    
    #zipcode-weekday-wind-precip
    feature_dict["zipcode_weekday_temp_precip"] = "%s_%s_%s_%s" % (feature_dict["zipcode"], feature_dict["weekday"], feature_dict["temp"], feature_dict["precip"])
    
    #zipcode-weekday-wind-precip
    feature_dict["zipcode_weekday_temp_precip_wind"] = "%s_%s_%s_%s_%s" % (feature_dict["zipcode"], feature_dict["weekday"], feature_dict["temp"], feature_dict["precip"], feature_dict["wind_speed"])
    
    return feature_dict

feature_idx = {"pickup_zipcode":0, "pickup_year":1, "pickup_month":2, "pickup_day":3, "mean_temp":4, \
             "mean_wind_speed":5, "precipitation":6 }

def open_connection():    
    # Open connection
    try:
        conn = psycopg2.connect(database='postgres', user='muser', password='muser', host='ec2-52-207-211-243.compute-1.amazonaws.com', port='5432')
        conn.set_isolation_level(3)  #want to control our own committing
        print "connected"
        return conn
    except Exception, e:
        print "didn't connect to database", e
        exit()

conn = open_connection()        

cur = conn.cursor()
cur.execute("""SELECT 
zipcode, 
date_part('year', start::timestamp)::text as pickup_year, 
date_part('month', start::timestamp)::text as pickup_month, 
date_part('day', start::timestamp)::text as pickup_day, 
(min + max) / 2 as mean_temp, 
12 as mean_wind_speed, 
case 
    when prob_precip < 50 then 0 
    else prob_precip*5/50 
end as precipitation, 
neighborhood, 
borough
from zipcode_neighborhood_borough cross join weather_prediction; """)
zip_weather_forecast = cur.fetchall()

# Close connection
conn.close()

zip_weather_forecast = np.asarray(zip_weather_forecast)

input_dict = [get_feature_dict(x) for x in zip_weather_forecast]

y_forecaster = joblib.load('./model/nyc_yellow_taxi_predictor.pkl') 
y_vectorizer = joblib.load('./model/nyc_yellow_taxi_vectorizer.pkl')
g_forecaster = joblib.load('./model/nyc_green_taxi_predictor.pkl') 
g_vectorizer = joblib.load('./model/nyc_green_taxi_vectorizer.pkl')

y_input_vector = y_vectorizer.transform(input_dict)
g_input_vector = g_vectorizer.transform(input_dict)

y_predictions = y_forecaster.predict(y_input_vector)
g_predictions = g_forecaster.predict(g_input_vector)

np.random.seed(0)
y_predictions = [np.random.randint(1,50) if x < 0 else x for x in y_predictions]
g_predictions = [np.random.randint(1,50) if x < 0 else x for x in g_predictions]

y_zip_predictions = np.column_stack((zip_weather_forecast, y_predictions))
g_zip_predictions = np.column_stack((zip_weather_forecast, g_predictions))

y_to_save = y_zip_predictions[:,(8,7,0,1,2,3,9)]
y_to_save = np.column_stack((y_to_save, ['Yellow'] * len(y_to_save)))
y_to_save = np.column_stack((y_to_save, [str(datetime.now())] * len(y_to_save)))

g_to_save = g_zip_predictions[:,(8,7,0,1,2,3,9)]
g_to_save = np.column_stack((g_to_save, ['Yellow'] * len(g_to_save)))
g_to_save = np.column_stack((g_to_save, [str(datetime.now())] * len(g_to_save)))

conn = open_connection()

cur = conn.cursor()
cur.execute("""SELECT * FROM information_schema.tables WHERE table_name = 'zip_ride_prediction';""")

res = cur.fetchone()

if ((res is None) or (res[0] == 0)):

    try:
        cur.execute("""CREATE TABLE zip_ride_prediction 
( 
borough VARCHAR,
neighborhood VARCHAR, 
zipcode CHAR(5),
pickup_year varchar,
pickup_month varchar,
pickup_day varchar,
prediction INT,
ride_source VARCHAR,
timestamp_of_prediction VARCHAR);""")
        print "Created table."
    except:
        print "Error when attempting to create zip_ride_prediction table"

# We're constantly trimming off the top of the table (but not until commit)
cur.execute("""DELETE FROM zip_ride_prediction;""")  #using delete rather than truncate, because not sure if truncate
                                                    #plays nicely with transactions
    
# Save yellow cab predictions
for r in y_to_save:
    try:
        cur.execute("""INSERT INTO zip_ride_prediction 
(
borough,
neighborhood, 
zipcode,
pickup_year,
pickup_month,
pickup_day,
prediction,
ride_source,
timestamp_of_prediction) 
VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s );""",
        (r[0],r[1],r[2],r[3],r[4],r[5],int(float(r[6])),r[7],r[8]))

    except Exception, e:
        print "something went awry with the yellow cab write", e
        print sys.exc_info()
                    
                    
# Save green cab predictions
for r in g_to_save:
    try:
        cur.execute("""INSERT INTO zip_ride_prediction 
(
borough,
neighborhood, 
zipcode,
pickup_year,
pickup_month,
pickup_day,
prediction,
ride_source,
timestamp_of_prediction) 
VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s );""",
        (r[0],r[1],r[2],r[3],r[4],r[5],int(float(r[6])),r[7],r[8]))

    except Exception, e:
        print "something went awry with the green cab write", e
        print sys.exc_info()
                
conn.commit()                
conn.close()
print 'Update completed'
