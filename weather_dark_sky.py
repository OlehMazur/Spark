# Databricks notebook source
#Configuration
#3ec0bb65bbe4e020bef01d63d53c648c -free
#d4b20ef8a9ad8bcf2449be822fba03a4 -new 
storage_account_name = "staeeprodbigdataml2c"
storage_account_access_key = "EHYumrwso4XLSUHpvLptI33z7mumiZwZOErjrlP8FiW51Bb6NS2PaWJsqW9hsMttbZizgQjUexFZfZDBQJebYw=="
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

import sys
import os
import forecastio
import datetime
import calendar
import time
import pandas as pd
import geopy.geocoders
from geopy.geocoders import Nominatim

# COMMAND ----------

 #main params
city = dbutils.widgets.get('City')
year = int(dbutils.widgets.get('Year'))
    
#forecastio api_key
api_key = "3ec0bb65bbe4e020bef01d63d53c648c" if dbutils.widgets.get('API_Key') == '' else dbutils.widgets.get('API_Key')
   
#blob storage key
STORAGEACCOUNTNAME= 'staeeprodbigdataml2c'
STORAGEACCOUNTKEY= 'EHYumrwso4XLSUHpvLptI33z7mumiZwZOErjrlP8FiW51Bb6NS2PaWJsqW9hsMttbZizgQjUexFZfZDBQJebYw==' 
CONTAINERNAME= 'prod' if dbutils.widgets.get('CONTAINER_NAME') == '' else dbutils.widgets.get('CONTAINER_NAME')

geopy.geocoders.options.default_timeout = None
geolocator = Nominatim(user_agent="get lat and long")
location = geolocator.geocode(city)

lat = location.latitude
lng = location.longitude

days_in_year =  366 if calendar.isleap(year) else 365 
#days_in_year = 2 #TEST
data =[]

t1=time.time()

start = datetime.datetime(year, 1, 1)
for offset in range(0, days_in_year):
     date = start+datetime.timedelta(offset)
     forecast = forecastio.load_forecast(api_key, lat, lng, time=date).daily().data
     if len(forecast) > 0:
         data.append(forecast[0].d)
         
t2=time.time()  
print(("it takes %s seconds to get weather forecast ") % (t2 - t1)) 
      
df = pd.DataFrame(data)   
df_s = spark.createDataFrame(df)

#save to blob storage
readPath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/test_res.csv"
writePath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/Transformation/Weather/" + "weather_" + city + '_' + str(year)
fname = "weather_" + city + '_' + str(year)+ ".csv"
df_s.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").save(readPath) 

file_list = dbutils.fs.ls(readPath)
for i in file_list:
  if i[1].startswith("part-00000"):  
    read_name = i[1]
dbutils.fs.mv(readPath+"/"+read_name, writePath+"/"+fname)   
dbutils.fs.rm(readPath , recurse= True)

message = ('city ' + str(city)), (" it takes %s minutes to get weather forecast ") % str((t2 - t1)/60) , ( " count of rows " + str(df.shape[0]) ) , (  " count of apparentTemperatureMax " +  str(df['apparentTemperatureMax'].count())  ), (  " count of cloudCover " +  str(df['cloudCover'].count())  ), (  " count of humidity " +  str(df['humidity'].count())  ), (  " count of windSpeed " +  str(df['windSpeed'].count())  )

dbutils.notebook.exit( message )


# COMMAND ----------

# SearchPath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/Transformation/Weather/"
# file_list = dbutils.fs.ls(SearchPath)
# for i in file_list:
#    if i[1]. 
#      print(read_name)    