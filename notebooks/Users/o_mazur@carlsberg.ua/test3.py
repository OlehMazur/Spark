# Databricks notebook source
storage_account_name = "staeeprodbigdataml2c"
storage_account_access_key = "EHYumrwso4XLSUHpvLptI33z7mumiZwZOErjrlP8FiW51Bb6NS2PaWJsqW9hsMttbZizgQjUexFZfZDBQJebYw=="
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/Plant_City.csv"
file_type = "csv"
df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Plant_City")

# COMMAND ----------

city_list = spark.sql("select distinct city from Plant_City")

# COMMAND ----------

cities = city_list.toPandas().values.tolist()

# COMMAND ----------

cities_format = []
for city in cities:
     cities_format.append(str(city).replace("[", "").replace("]", "").replace("'", "") )

# COMMAND ----------

import sys
import os
import forecastio
import datetime
import calendar
import time
import pandas as pd
import geopy
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# COMMAND ----------

from math import cos, asin, sqrt

def distance(lat1, lon1, lat2, lon2):
    p = 0.017453292519943295
    a = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p)*cos(lat2*p) * (1-cos((lon2-lon1)*p)) / 2
    return 12742 * asin(sqrt(a))

def closest(data, v):
    return min(data, key=lambda p: distance(v['lat'],v['lon'],p['lat'],p['lon']))


# COMMAND ----------

def getLatLng (city):

  #geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
  geopy.geocoders.options.default_timeout = 7 #None
  geolocator = Nominatim(user_agent="get lat and long")
  location = geolocator.geocode(city)

  lat = location.latitude
  lng = location.longitude
  return ({'lat': lat, 'lon': lng})
                

# COMMAND ----------

#57.7679158 40.9269141
tempDataList = []
for cf in cities_format: 
  if (cf == 'Kostroma' or cf == 'Vologda' or cf == 'Vladimir' or cf == 'Nizhny Novgorod' or cf == 'Moscow'):
     continue
  tempDataList.append(getLatLng(cf))


# COMMAND ----------

#info with city

# COMMAND ----------

#57.7679158 40.9269141
tempDataList_city_name = []
for cf in cities_format: 
  if (cf == 'Kostroma' or cf == 'Vologda' or cf == 'Vladimir' or cf == 'Nizhny Novgorod' or cf == 'Moscow'):
     continue
  tempDataList_city_name.append([getLatLng(cf), cf])


# COMMAND ----------

point = {'lat': 57.7679158, 'lon': 40.9269141}
print(closest(tempDataList, point))

# COMMAND ----------

for item in tempDataList_city_name: print (item)

# COMMAND ----------

geolocator = Nominatim(user_agent="get lat and long")
location = geolocator.geocode("Kostroma")

lat = location.latitude
lng = location.longitude

# COMMAND ----------

print(lat, lng) 
#{"latitude":57.7679158,"longitude":40.9269141

# COMMAND ----------



# COMMAND ----------

#Get Weather Data based on lat and lng

# COMMAND ----------

#dictionaty with all city and (lat, lng)

# COMMAND ----------

city_dic_with_geo = {}
for cf in cities_format: 
  city_dic_with_geo[cf] = getLatLng(cf)

# COMMAND ----------

def get_city_by_geo (city_dic_with_geo, closest_geo):
  for city, geo in city_dic_with_geo.items():  
      if geo == closest_geo:
          return city

# COMMAND ----------

city_dic_with_geo['Kostroma']

# COMMAND ----------

# #57.7679158 40.9269141

# params = ["Kostroma"]
# tempDataList = []
# for param in params: 
#   for city in cities_format: 
#      if (city in param):
#         continue
#      tempDataList.append(city_dic_with_geo[city])
#   point = city_dic_with_geo[param]
#   #(lat, lng)  
#   print(closest(tempDataList, point)) 
#   #nearest city
#   print(get_city_by_geo(city_dic_with_geo, closest(tempDataList, point) ))
    
  


# COMMAND ----------

#57.7679158 40.9269141
def get_nearest_city (city_point, cities_format ):
  tempDataList = []
  for city in cities_format: 
     if (city == city_point):
        continue
     tempDataList.append(city_dic_with_geo[city])
  point = city_dic_with_geo[city_point]
  #(lat, lng)  
  #print(closest(tempDataList, point)) 
  #nearest city
  return get_city_by_geo(city_dic_with_geo, closest(tempDataList, point) )
  

# COMMAND ----------

get_nearest_city('Kostroma', cities_format)

# COMMAND ----------

for city in cities_format: 
  print('for city ' + city)
  cities_format_exclude = cities_format.copy()
  city_to_exclude_1 = get_nearest_city(city, cities_format)
  print('first nearest city ' + city_to_exclude_1)
  cities_format_exclude.remove(city_to_exclude_1)
  for city2 in cities_format_exclude: 
    if city2 != city:
      continue
    city_to_exclude_2 = get_nearest_city(city2,cities_format_exclude )
    print ('second nearest city ' + city_to_exclude_2)
    cities_format_exclude.remove(city_to_exclude_2)
    for city3 in cities_format_exclude: 
      if city3 != city:
        continue
      city_to_exclude_3 = get_nearest_city(city3,cities_format_exclude )
      print ('third nearest city ' + city_to_exclude_3)
      cities_format_exclude.remove(city_to_exclude_3)
      for city4 in cities_format_exclude: 
        if city4 != city:
          continue
        city_to_exclude_4 = get_nearest_city(city4,cities_format_exclude )
        print ('fourth nearest city ' + city_to_exclude_4)
        cities_format_exclude.remove(city_to_exclude_4)
        for city5 in cities_format_exclude: 
          if city5 != city:
            continue
          city_to_exclude_5 = get_nearest_city(city5,cities_format_exclude )
          print ('fiveth nearest city ' + city_to_exclude_5)
    #break
      



# COMMAND ----------

# def test(city, city_list):
#   exclude = get_nearest_city(city, city_list)
#   print(exclude)
#   city_list.remove(exclude)
#   test(city, city_list)
  


# COMMAND ----------

result = {}
nearests_city = list()

for city in cities_format: 
  nearests_city = list()
  print('for city ' + city)
  cities_format_exclude = cities_format.copy()
  city_to_exclude_1 = get_nearest_city(city, cities_format)
  print('first nearest city ' + city_to_exclude_1)
  nearests_city.append(city_to_exclude_1)
  cities_format_exclude.remove(city_to_exclude_1)
  for city2 in cities_format_exclude: 
    if city2 != city:
      continue
    city_to_exclude_2 = get_nearest_city(city2,cities_format_exclude )
    print ('second nearest city ' + city_to_exclude_2)
    nearests_city.append(city_to_exclude_2)
    cities_format_exclude.remove(city_to_exclude_2)
    for city3 in cities_format_exclude: 
      if city3 != city:
        continue
      city_to_exclude_3 = get_nearest_city(city3,cities_format_exclude )
      print ('third nearest city ' + city_to_exclude_3)
      nearests_city.append(city_to_exclude_3)
      cities_format_exclude.remove(city_to_exclude_3)
      for city4 in cities_format_exclude: 
        if city4 != city:
          continue
        city_to_exclude_4 = get_nearest_city(city4,cities_format_exclude )
        print ('fourth nearest city ' + city_to_exclude_4)
        nearests_city.append(city_to_exclude_4)
        cities_format_exclude.remove(city_to_exclude_4)
        for city5 in cities_format_exclude: 
          if city5 != city:
            continue
          city_to_exclude_5 = get_nearest_city(city5,cities_format_exclude )
          print ('fiveth nearest city ' + city_to_exclude_5)
          nearests_city.append(city_to_exclude_5)
  
  result[city] = nearests_city
    
    


# COMMAND ----------

result['Kostroma']

# COMMAND ----------


df_result = pd.DataFrame.from_dict(result, columns=['first', 'second', 'third', 'fourth', 'fiveth'], orient='index')
df_result["city"] = df_result.index

# COMMAND ----------

df_result.head()

# COMMAND ----------

dfd = spark.createDataFrame(df_result)
dfd.createOrReplaceTempView("geo_info")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from geo_info
# MAGIC where city = 'Kostroma'