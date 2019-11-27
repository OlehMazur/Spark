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

file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/tmp/city.csv"
file_type = "csv"
city_df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
city_df.createOrReplaceTempView("City")

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
from pyspark.sql.functions import col,lit,expr 
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.types import StructType,StructField,StringType

# COMMAND ----------

def getLatLng (city):

  #geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
  geopy.geocoders.options.default_timeout = 7 #None
  geolocator = Nominatim(user_agent="get lat and long")
  location = geolocator.geocode(city)

  lat = location.latitude if location is not None else 'Nan'
  lng = location.longitude if location is not None else 'Nan'
  return ({'lat': lat, 'lon': lng})
                

# COMMAND ----------

from math import cos, asin, sqrt

def distance(lat1, lon1, lat2, lon2):
    p = 0.017453292519943295
    a = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p)*cos(lat2*p) * (1-cos((lon2-lon1)*p)) / 2
    return 12742 * asin(sqrt(a))

def closest(data, v):
    return min(data, key=lambda p: distance(v['lat'],v['lon'],p['lat'],p['lon']))


# COMMAND ----------

city_sql_df = spark.sql("select distinct city from City")

# COMMAND ----------

cities = city_sql_df.toPandas().values.tolist()

# COMMAND ----------

cities_format = []
for city in cities:
     cities_format.append(str(city).replace("[", "").replace("]", "").replace("'", "") )

# COMMAND ----------

city_dic_with_geo = {}
for cf in cities_format: 
  city_dic_with_geo[cf] = getLatLng(cf)

# COMMAND ----------

city_dic_with_geo_clean = {}
for i, v in city_dic_with_geo.items():
  if v == {'lat': 'Nan', 'lon': 'Nan'}:
    cities_format.remove(i)
    continue
  city_dic_with_geo_clean[i] = v

# COMMAND ----------

city_dic_with_geo_clean['Kostroma']['lat']
city_dic_with_geo_clean['Kostroma']['lon']

# COMMAND ----------

def get_city_by_geo (city_dic_with_geo_clean, closest_geo):
  for city, geo in city_dic_with_geo_clean.items():  
      if geo == closest_geo:
          return city

# COMMAND ----------

#57.7679158 40.9269141
def get_nearest_city (city_point, cities_format ):
  tempDataList = []
  for city in cities_format: 
     if (city == city_point):
        continue
     tempDataList.append(city_dic_with_geo_clean[city])
  point = city_dic_with_geo_clean[city_point]
  #(lat, lng)  
  #print(closest(tempDataList, point)) 
  #nearest city
  return get_city_by_geo(city_dic_with_geo_clean, closest(tempDataList, point) )
  

# COMMAND ----------

get_nearest_city('Kostroma', cities_format)

# COMMAND ----------

result = {}
nearests_city = list()

for city in cities_format: 
  nearests_city = list()
  #print('for city ' + city)
  cities_format_exclude = cities_format.copy()
  city_to_exclude_1 = get_nearest_city(city, cities_format)
  #print('first nearest city ' + city_to_exclude_1)
  #nearests_city.append(1)
  nearests_city.append(city_to_exclude_1)
  cities_format_exclude.remove(city_to_exclude_1)
  for city2 in cities_format_exclude: 
    if city2 != city:
      continue
    city_to_exclude_2 = get_nearest_city(city2,cities_format_exclude )
    #print ('second nearest city ' + city_to_exclude_2)
    #nearests_city.append(2)
    nearests_city.append(city_to_exclude_2)
    cities_format_exclude.remove(city_to_exclude_2)
    for city3 in cities_format_exclude: 
      if city3 != city:
        continue
      city_to_exclude_3 = get_nearest_city(city3,cities_format_exclude )
      #print ('third nearest city ' + city_to_exclude_3)
      #nearests_city.append(3)
      nearests_city.append(city_to_exclude_3)
      cities_format_exclude.remove(city_to_exclude_3)
      for city4 in cities_format_exclude: 
        if city4 != city:
          continue
        city_to_exclude_4 = get_nearest_city(city4,cities_format_exclude )
        #print ('fourth nearest city ' + city_to_exclude_4)
        #nearests_city.append(4)
        nearests_city.append(city_to_exclude_4)
        cities_format_exclude.remove(city_to_exclude_4)
        for city5 in cities_format_exclude: 
          if city5 != city:
            continue
          city_to_exclude_5 = get_nearest_city(city5,cities_format_exclude )
          #print ('fiveth nearest city ' + city_to_exclude_5)
          #nearests_city.append(5)
          nearests_city.append(city_to_exclude_5)
          cities_format_exclude.remove(city_to_exclude_5)
          for city6 in cities_format_exclude: 
            if city6 != city:
              continue
            city_to_exclude_6 = get_nearest_city(city6,cities_format_exclude )
            #print ('fiveth nearest city ' + city_to_exclude_5)
            #nearests_city.append(5)
            nearests_city.append(city_to_exclude_6)
            cities_format_exclude.remove(city_to_exclude_6)
            for city7 in cities_format_exclude: 
              if city7 != city:
                continue
              city_to_exclude_7 = get_nearest_city(city7,cities_format_exclude )
              #print ('fiveth nearest city ' + city_to_exclude_5)
              #nearests_city.append(5)
              nearests_city.append(city_to_exclude_7)
              cities_format_exclude.remove(city_to_exclude_7)
              for city8 in cities_format_exclude: 
                if city8 != city:
                  continue
                city_to_exclude_8 = get_nearest_city(city8,cities_format_exclude )
                #print ('fiveth nearest city ' + city_to_exclude_5)
                #nearests_city.append(5)
                nearests_city.append(city_to_exclude_8)
                cities_format_exclude.remove(city_to_exclude_8)
                for city9 in cities_format_exclude: 
                  if city9 != city:
                    continue
                  city_to_exclude_9 = get_nearest_city(city9,cities_format_exclude )
                  #print ('fiveth nearest city ' + city_to_exclude_5)
                  #nearests_city.append(5)
                  nearests_city.append(city_to_exclude_9)
                  cities_format_exclude.remove(city_to_exclude_9)
                  for city10 in cities_format_exclude: 
                    if city10 != city:
                      continue
                    city_to_exclude_10 = get_nearest_city(city10,cities_format_exclude )
                    #print ('fiveth nearest city ' + city_to_exclude_5)
                    #nearests_city.append(5)
                    nearests_city.append(city_to_exclude_10)
                    cities_format_exclude.remove(city_to_exclude_10)
  
  result[city] = nearests_city
    
    


# COMMAND ----------

 result['Kostroma']

# COMMAND ----------

ar = result['Kostroma']
for i in ar:
  print(i)

# COMMAND ----------



# COMMAND ----------

 #main params
message = ""  
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
     if (len(forecast) > 0) :
         data.append(forecast[0].d)
         
t2=time.time()  
#print(("it takes %s seconds to get weather forecast ") % (t2 - t1)) 

if (len(data) > 0) :     
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

  message = ('city ' + str(city)), (" it takes %s minutes to get weather forecast ") % str((t2 - t1)/60)
  #('city ' + str(city)), (" it takes %s minutes to get weather forecast ") % str((t2 - t1)/60) , ( " count of rows " + str(df.shape[0]) ) , (  " count of apparentTemperatureMax " +  str(df['apparentTemperatureMax'].count())  ), (  " count of cloudCover " +  str(df['cloudCover'].count())  ), (  " count of humidity " +  str(df['humidity'].count())  ), (  " count of windSpeed " +  str(df['windSpeed'].count())  )
  print(message)
else: 
  #message = str(city) + " there is no data for the city"
  print(str(city) + " there is no data for the city")
  
  nearest_city_list = result[city]
  
  schema_string = "time,apparentTemperatureMax,cloudCover,humidity,windSpeed"
  mySchema = StructType([StructField(c, StringType()) for c in schema_string.split(",")])
  df_full = spark.createDataFrame(data=[], schema=mySchema)
  
  for found_near_city in nearest_city_list:
    
    lat =city_dic_with_geo_clean[found_near_city]['lat']
    lng= city_dic_with_geo_clean[found_near_city]['lon']
    
    data =[]
    t1=time.time()

    start = datetime.datetime(year, 1, 1)
    for offset in range(0, days_in_year):
         date = start+datetime.timedelta(offset)
         forecast = forecastio.load_forecast(api_key, lat, lng, time=date).daily().data
         if (len(forecast) > 0) :
             data.append(forecast[0].d)

    t2=time.time()  
    print( ('city ' + str(found_near_city)), ("it takes %s minutes to get weather forecast ") % str((t2 - t1)/60)) 
    
    if (len(data) > 0) :     
      df = pd.DataFrame(data) 
      if ('time' not in df.columns):
        df = df.withColumn("time", expr("null") )
      if ('apparentTemperatureMax' not in df.columns):
        df = df.withColumn("apparentTemperatureMax", expr("null"))
      if ('cloudCover' not in df.columns ):
        df = df.withColumn("cloudCover", expr("null")) 
      if ('humidity' not in df.columns ):
        df = df.withColumn("humidity", expr("null")) 
      if ('windSpeed' not in df.columns ):
        df = df.withColumn("windSpeed", expr("null")) 
      
      df = df.select("time", "apparentTemperatureMax", "cloudCover", "humidity", "windSpeed") 
      
      filter_date = df.selectExpr("time").exceptAll(df_full.selectExpr("time"))
      
      list_of_date_clear= list()
      list_of_date = filter_date.toPandas().values.tolist()
      for date in list_of_date:
        list_of_date_clear.append(str(date).replace("[", "").replace("]", ""))
      
      df_new_data = df.where(col("time").isin(list_of_date_clear ))
    
      df_full = df_full.union(df_new_data)
   
    if (len(data) >=365):
        break

  #save to blob storage
  df_s = spark.createDataFrame(df_full)
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
  print("file has been saved to blob, " + str(len(df_full)) + " records" ) 

#dbutils.notebook.exit( message )


# COMMAND ----------



# COMMAND ----------

schema_string = "time,apparentTemperatureMax,cloudCover,humidity,windSpeed,city"
mySchema = StructType([StructField(c, StringType()) for c in schema_string.split(",")])
df = spark.createDataFrame(data=[], schema=mySchema)
df.show()

# COMMAND ----------

file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/Transformation/Weather/weather_Kostroma_2016/weather_Kostroma_Dzerzhinsk_2016.csv"
file_type = "csv"
city_df_dzer = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
city_df_dzer.createOrReplaceTempView("Dzerzhinsk")

# COMMAND ----------

file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/Transformation/Weather/weather_Kostroma_2016/weather_Kostroma_Yaroslavl_2016.csv"
file_type = "csv"
city_df_yar = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
city_df_yar.createOrReplaceTempView("Yaroslavl")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select distinct cast(from_unixtime(time) as date)
# MAGIC from Dzerzhinsk except (select distinct cast(from_unixtime(time) as date)
# MAGIC from Yaroslavl)

# COMMAND ----------

filter_date = city_df_dzer.selectExpr("time").exceptAll(city_df_yar.selectExpr("time"))

# COMMAND ----------

list_of_date_clear= list()
list_of_date = filter_date.toPandas().values.tolist()
for date in list_of_date:
  list_of_date_clear.append(str(date).replace("[", "").replace("]", ""))
  

# COMMAND ----------

city_df_dzer_new_data = city_df_dzer.select("time","apparentTemperatureMax","cloudCover","humidity","windSpeed").where(col("time").isin(list_of_date_clear ))

# COMMAND ----------

data_full= city_df_yar.select("time","apparentTemperatureMax","cloudCover","humidity","windSpeed").union(city_df_dzer_new_data)

# COMMAND ----------

data_full.count()

# COMMAND ----------

city_df_yar.selectExpr("min(cast(from_unixtime(time) as date))").show()

# COMMAND ----------

city_df_yar.selectExpr("max(cast(from_unixtime(time) as date))").show()

# COMMAND ----------

df.count()

# COMMAND ----------

city_df_dzer.selectExpr("cast(from_unixtime(time) as date)").exceptAll(city_df_yar.selectExpr("cast(from_unixtime(time) as date)")).show()

# COMMAND ----------

from pyspark.sql.functions import col,lit 

# COMMAND ----------

city_df_dzer.select("time","apparentTemperatureMax","cloudCover","humidity","windSpeed").where("time = 1451768400").show()

# COMMAND ----------

city_df_dzer.select("time","apparentTemperatureMax","cloudCover","humidity","windSpeed").where(col("time").isin(list_of_date_clear )).show()