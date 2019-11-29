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

df = pd.DataFrame(city_dic_with_geo)
df_s = spark.createDataFrame(df)

readPath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/test_res.csv"
writePath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/tmp"
fname = "city_with_geo_info.csv"
df_s.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").save(readPath) 

file_list = dbutils.fs.ls(readPath)
for i in file_list:
  if i[1].startswith("part-00000"):  
    read_name = i[1]
dbutils.fs.mv(readPath+"/"+read_name, writePath+"/"+fname)   
dbutils.fs.rm(readPath , recurse= True)
 