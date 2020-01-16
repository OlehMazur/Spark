# Databricks notebook source
#Configuration
storage_account_name = "staeeprodbigdataml2c"
storage_account_access_key = "EHYumrwso4XLSUHpvLptI33z7mumiZwZOErjrlP8FiW51Bb6NS2PaWJsqW9hsMttbZizgQjUexFZfZDBQJebYw=="
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

#Nielsen data

# COMMAND ----------

file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/nielsen_wheader.csv"
file_type = "csv"
df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Nielsen")

# COMMAND ----------

#SKU_Baltika_SKU_Nilesen.csv

# COMMAND ----------

file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/SKU_Baltika_SKU_Nilesen.csv"
file_type = "csv"
df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df = df.withColumnRenamed("CODE" , "CODE").withColumnRenamed("Код Балтика" , "Bal_CODE").withColumnRenamed("Название Балтика" , "Name_Bal").withColumnRenamed("Обобщенный SKU Балтика" , "Lead_SKU_Bal")
df.createOrReplaceTempView("Mapping")

# COMMAND ----------

#SP_WH

# COMMAND ----------

file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/SP_WH.csv"
file_type = "csv"
df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("SP_WH")

# COMMAND ----------

#MD_Clients

# COMMAND ----------

file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/MD_Clients.csv"
file_type = "csv"
df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("MD_Clients")

# COMMAND ----------

#Divisions

# COMMAND ----------

file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/Divisions_UTF.csv"
file_type = "csv"
df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Divisions")

# COMMAND ----------

#MD_SKU

# COMMAND ----------

file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/MD_SKU.csv"
file_type = "csv"
df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df = df.withColumnRenamed("D-Size" , "D-Size").withColumnRenamed("IsClose" , "IsClose").withColumnRenamed("SKU" , "SKU").withColumnRenamed("Активность SKU" , "SKU_Activity").withColumnRenamed("Брэнд" , "Brand").withColumnRenamed("Категория", "Categoty").withColumnRenamed("Код SKU", "SKU_ID").withColumnRenamed("Код SKU Excel", "SKU_ID_Excel").withColumnRenamed("Код брэнда", "Brand_ID").withColumnRenamed("Код категории", "Category_ID").withColumnRenamed("Код обобщенного SKU", "SKU_Lead_ID").withColumnRenamed("Код обобщенного SKU без МП", "SKU_Lead_ID_Without_MP").withColumnRenamed("Код сорта", "Sort_ID").withColumnRenamed("Код суббренда", "Subbrand_ID").withColumnRenamed("Крепость", "Alcohol").withColumnRenamed("Мультипак", "Multypack").withColumnRenamed("Обобщенный SKU", "Lead_SKU").withColumnRenamed("Обобщенный SKU без МП", "Lead_SKU_Without_MP").withColumnRenamed("Объем тары", "Tare_volume").withColumnRenamed("Принадлежность продукции", "Product_Affiliation").withColumnRenamed("Продукция", "Product").withColumnRenamed("Промо", "Promo").withColumnRenamed("Региональность", "Regionality").withColumnRenamed("Содержание акоголя", "Alcohol content").withColumnRenamed("Сорт", "Sort").withColumnRenamed("Суббренд", "Subbrand").withColumnRenamed("Тип DRP", "DRP_Type").withColumnRenamed("Тип мультипака", "Multypack_Type").withColumnRenamed("Тип пробки", "Cork_Type").withColumnRenamed("Тип продукции", "Product_Type").withColumnRenamed("Тип сусла", "Word_Type").withColumnRenamed("Тип тары", "Tare_Type").withColumnRenamed("Цвет тары", "Color_Tare").withColumnRenamed("Ценовой сегмент", "Price_Segment")
df.createOrReplaceTempView("MD_SKU")

# COMMAND ----------

#Calendar

# COMMAND ----------

file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/Calendar.csv"
file_type = "csv"
df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Calendar")

# COMMAND ----------

#PlantID

# COMMAND ----------

file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/PlantID.csv"
file_type = "csv"
df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("plant_id_info")

# COMMAND ----------

#file bigdatadailyretail_wheader.csv (new source)

# COMMAND ----------

file_location = "wasbs://russia@staeeprodbigdataml2c.blob.core.windows.net/bigdatadailyretail_wheader.csv"
file_type = "csv"
df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Sell_out_NEW")

# COMMAND ----------

#Result sell_out_etl_new.csv (IMPORTANT => field  "cpg" should be changed)

# COMMAND ----------


sqldf = spark.sql(
  """
   select int(s.Date_Id/100) calendar_yearmonth, upper(s.NetworkName) network_name , s.store_no, /*m.Lead_SKU_Bal*/ MD_SKU.SKU_Lead_ID lead_sku, lon, lat , n.`Manufacturer (groups)` , sp_wh.dc_plant_code, cl.ActiveAdress plant_name, dev.division, customer_planning_group , sum(SalesValue) sales_value, sum(SalesVolume/10) sales_volume_hl 
from Sell_out_NEW s left join nielsen n on s.CODE = n.CODE left join mapping m on s.CODE = m.CODE left join sp_wh on s.Store_no = sp_wh.store_no
left join md_clients cl on cl.AdressCode = sp_wh.dc_plant_code  left join Divisions dev on  cl.ActiveAdress = dev.WH left join MD_SKU on cast(replace( m.Bal_code, left( m.Bal_code, 1), '') as int) = MD_SKU.SKU_ID 
cross join (select 'X5 Retail Group' as customer_planning_group) cpg
where n.`Manufacturer (groups)` = 'BALTIKA' 
group by int(s.Date_Id/100), upper(s.NetworkName), s.Store_no, MD_SKU.SKU_Lead_ID /*m.Lead_SKU_Bal*/, Lon, Lat , n.`Manufacturer (groups)` , sp_wh.dc_plant_code , cl.ActiveAdress , dev.Division , cpg.customer_planning_group
  """
  )
readPath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/test_res.csv"
writePath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/Result"
fname = 'sell_out_etl_new.csv'
sqldf.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").save(readPath)

file_list = dbutils.fs.ls(readPath)
for i in file_list:
  if i[1].startswith("part-00000"):  
    read_name = i[1]
dbutils.fs.mv(readPath+"/"+read_name, writePath+"/"+fname)   
dbutils.fs.rm(readPath , recurse= True)

# COMMAND ----------

#Result sell_out_etl_new_with_week.csv (IMPORTANT => field  "cpg" should be changed)

# COMMAND ----------


sqldf = spark.sql(
  """
   select calendar.MonthId calendar_yearmonth,  calendar.WeekId calendar_yearweek, upper(s.NetworkName) network_name , s.store_no, /*m.Lead_SKU_Bal*/ MD_SKU.SKU_Lead_ID lead_sku, lon, lat , n.`Manufacturer (groups)` , sp_wh.dc_plant_code, cl.ActiveAdress plant_name, dev.division, customer_planning_group , sum(SalesValue) sales_value, sum(SalesVolume/10) sales_volume_hl
from Sell_out_NEW s left join nielsen n on s.CODE = n.CODE left join mapping m on s.CODE = m.CODE left join sp_wh on s.Store_no = sp_wh.store_no
left join md_clients cl on cl.AdressCode = sp_wh.dc_plant_code  left join Divisions dev on  cl.ActiveAdress = dev.WH left join MD_SKU on cast(replace( m.Bal_code, left( m.Bal_code, 1), '') as int) = MD_SKU.SKU_ID left join calendar on s.Date_Id = calendar.DateKey
cross join (select 'X5 Retail Group' as customer_planning_group) cpg
where n.`Manufacturer (groups)` = 'BALTIKA' 
group by calendar.MonthId, calendar.WeekId,  upper(s.NetworkName), s.Store_no, MD_SKU.SKU_Lead_ID /*m.Lead_SKU_Bal*/, Lon, Lat , n.`Manufacturer (groups)` , sp_wh.dc_plant_code , cl.ActiveAdress , dev.Division , cpg.customer_planning_group
  """
  )
readPath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/test_res.csv"
writePath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/Result"
fname = 'sell_out_etl_new_with_week.csv'
sqldf.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").save(readPath)

file_list = dbutils.fs.ls(readPath)
for i in file_list:
  if i[1].startswith("part-00000"):  
    read_name = i[1]
dbutils.fs.mv(readPath+"/"+read_name, writePath+"/"+fname)   
dbutils.fs.rm(readPath , recurse= True)

# COMMAND ----------

#Result sell_out_etl_status_new.csv (IMPORTANT => field  "cpg" should be changed)

# COMMAND ----------


sqldf = spark.sql(
  """
   select int(s.Date_Id/100) calendar_yearmonth, upper(s.NetworkName) network_name , s.store_no, /*m.Lead_SKU_Bal*/ MD_SKU.SKU_Lead_ID lead_sku, lon, lat , n.`Manufacturer (groups)` , sp_wh.dc_plant_code, cl.ActiveAdress plant_name, dev.division, customer_planning_group , s.StatusDay status_day , s.DiscountRange
discount_range, sum(SalesValue) sales_value, sum(SalesVolume/10) sales_volume_hl 
from Sell_out_NEW s left join nielsen n on s.CODE = n.CODE left join mapping m on s.CODE = m.CODE left join sp_wh on s.Store_no = sp_wh.store_no
left join md_clients cl on cl.AdressCode = sp_wh.dc_plant_code  left join Divisions dev on  cl.ActiveAdress = dev.WH left join MD_SKU on cast(replace( m.Bal_code, left( m.Bal_code, 1), '') as int) = MD_SKU.SKU_ID 
cross join (select 'X5 Retail Group' as customer_planning_group) cpg
where n.`Manufacturer (groups)` = 'BALTIKA' and s.StatusDay = 1
group by int(s.Date_Id/100), upper(s.NetworkName), s.Store_no, MD_SKU.SKU_Lead_ID /*m.Lead_SKU_Bal*/, Lon, Lat , n.`Manufacturer (groups)` , sp_wh.dc_plant_code , cl.ActiveAdress , dev.Division , cpg.customer_planning_group, s.StatusDay, s.DiscountRange
  """
  )
readPath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/test_res.csv"
writePath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/Result"
fname = 'sell_out_etl_status_new.csv'
sqldf.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").save(readPath)

file_list = dbutils.fs.ls(readPath)
for i in file_list:
  if i[1].startswith("part-00000"):  
    read_name = i[1]
dbutils.fs.mv(readPath+"/"+read_name, writePath+"/"+fname)   
dbutils.fs.rm(readPath , recurse= True)

# COMMAND ----------

#Result sell_out_etl_status_new_with_week.csv ((IMPORTANT => field  "cpg" should be changed))

# COMMAND ----------


sqldf = spark.sql(
  """
   select calendar.MonthId calendar_yearmonth,  calendar.WeekId calendar_yearweek, upper(s.NetworkName) network_name , s.store_no, /*m.Lead_SKU_Bal*/ MD_SKU.SKU_Lead_ID lead_sku, lon, lat , n.`Manufacturer (groups)` , sp_wh.dc_plant_code, cl.ActiveAdress plant_name, dev.division, customer_planning_group , s.StatusDay status_day , s.DiscountRange discount_range, sum(SalesValue) sales_value, sum(SalesVolume/10) sales_volume_hl 
from Sell_out_NEW s left join nielsen n on s.CODE = n.CODE left join mapping m on s.CODE = m.CODE left join sp_wh on s.Store_no = sp_wh.store_no
left join md_clients cl on cl.AdressCode = sp_wh.dc_plant_code  left join Divisions dev on  cl.ActiveAdress = dev.WH left join MD_SKU on cast(replace( m.Bal_code, left( m.Bal_code, 1), '') as int) = MD_SKU.SKU_ID left join calendar on s.Date_Id = calendar.DateKey
cross join (select 'X5 Retail Group' as customer_planning_group) cpg
where n.`Manufacturer (groups)` = 'BALTIKA' and s.StatusDay = 1
group by calendar.MonthId, calendar.WeekId, upper(s.NetworkName), s.Store_no, MD_SKU.SKU_Lead_ID /*m.Lead_SKU_Bal*/, Lon, Lat , n.`Manufacturer (groups)` , sp_wh.dc_plant_code , cl.ActiveAdress , dev.Division , cpg.customer_planning_group, s.StatusDay, s.DiscountRange
  """
  )
readPath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/test_res.csv"
writePath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/Result"
fname = 'sell_out_etl_status_new_with_week.csv'
sqldf.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").save(readPath)

file_list = dbutils.fs.ls(readPath)
for i in file_list:
  if i[1].startswith("part-00000"):  
    read_name = i[1]
dbutils.fs.mv(readPath+"/"+read_name, writePath+"/"+fname)   
dbutils.fs.rm(readPath , recurse= True)

# COMMAND ----------

#Result store_attributes_etl.csv

# COMMAND ----------


sqldf = spark.sql(
  """ 
select 
keys_info.*,
median_total_info.total sales_volume_hl,
median_total_info.mediana median_sales, 
median_total_info.total / total_info_all.sales_total rate_of_Baltika, 
ps_info_ec.sales_ps / total_info_all.sales_total ECONOMY,
ps_info_ms.sales_ps / total_info_all.sales_total MAINSTREAM,
p_info_ms.sales_ps / total_info_all.sales_total PREMIUM,
sup_info_ms.sales_ps / total_info_all.sales_total `SUPER PREMIUM`,
sku_info.sku_count number_of_sku


from 
(
  select  upper(s.NetworkName) NetworkName, s.Store_no, Lon, Lat
  from Sell_out_NEW s 
  group by  upper(s.NetworkName), s.Store_no, Lon, Lat
)keys_info

left join 
(
  select tab.NetworkName, tab.Store_no, percentile(tab.SalesVolume, 0.5)  mediana, sum(tab.SalesVolume/10) total
  from (
    select  int(s.Date_Id/100) ym, upper(s.NetworkName) NetworkName, s.Store_no Store_no,  sum(SalesVolume) SalesVolume                      
    from Sell_out_NEW s 
    left join nielsen n on s.CODE = n.CODE 
    where  
    n.`Manufacturer (groups)` = 'BALTIKA'
    group by int(s.Date_Id/100), upper(s.NetworkName), s.Store_no
  ) tab
  group by tab.NetworkName, tab.Store_no  
) median_total_info
on 
keys_info.NetworkName = median_total_info.NetworkName and 
keys_info.Store_no = median_total_info.Store_no 

left join 
(
  select  upper(s.NetworkName) NetworkName, s.Store_no, sum(SalesVolume/10) sales_total
  from Sell_out_NEW s 
  group by  upper(s.NetworkName), s.Store_no
) total_info_all
on 

keys_info.NetworkName = total_info_all.NetworkName and 
keys_info.Store_no = total_info_all.Store_no 


left join 
(
  select  upper(s.NetworkName) NetworkName, s.Store_no, sum(SalesVolume/10) sales_ps
  from Sell_out_NEW s 
  left join nielsen n on s.CODE = n.CODE 
  where n.`price segment` = 'ECONOMY'
  group by  upper(s.NetworkName), s.Store_no
) ps_info_ec
on 
keys_info.NetworkName = ps_info_ec.NetworkName and 
keys_info.Store_no = ps_info_ec.Store_no 

left join 
(
  select  upper(s.NetworkName) NetworkName, s.Store_no, sum(SalesVolume/10) sales_ps
  from Sell_out_NEW s 
  left join nielsen n on s.CODE = n.CODE 
  where n.`price segment` = 'MAINSTREAM'
  group by  upper(s.NetworkName), s.Store_no
) ps_info_ms
on 
keys_info.NetworkName = ps_info_ms.NetworkName and 
keys_info.Store_no = ps_info_ms.Store_no 

left join 
(
  select  upper(s.NetworkName) NetworkName, s.Store_no, sum(SalesVolume/10) sales_ps
  from Sell_out_NEW s 
  left join nielsen n on s.CODE = n.CODE 
  where n.`price segment` = 'PREMIUM'
  group by  upper(s.NetworkName), s.Store_no
) p_info_ms
on 
keys_info.NetworkName = p_info_ms.NetworkName and 
keys_info.Store_no = p_info_ms.Store_no 

left join 
(
  select  upper(s.NetworkName) NetworkName, s.Store_no, sum(SalesVolume/10) sales_ps
  from Sell_out_NEW s  
  left join nielsen n on s.CODE = n.CODE 
  where n.`price segment` = 'SUPER PREMIUM'
  group by  upper(s.NetworkName), s.Store_no
) sup_info_ms
on 
keys_info.NetworkName = sup_info_ms.NetworkName and 
keys_info.Store_no = sup_info_ms.Store_no 

left join 
(
  select upper(s.NetworkName) NetworkName, s.Store_no, count(distinct `SKU (FULL SIZE & DOWN SIZE)`) sku_count
  from Sell_out_NEW s 
  left join nielsen n on s.CODE = n.CODE 
  where int(s.Date_Id/100) = 201712 and  n.`Manufacturer (groups)` = 'BALTIKA'
  group by  upper(s.NetworkName), s.Store_no 
) sku_info
on 
keys_info.NetworkName = sku_info.NetworkName and 
keys_info.Store_no = sku_info.Store_no 
 
  """
  )
readPath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/test_res.csv"
writePath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/Result"
fname = 'store_attributes_etl.csv'
sqldf.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").save(readPath)

file_list = dbutils.fs.ls(readPath)
for i in file_list:
  if i[1].startswith("part-00000"):  
    read_name = i[1]
dbutils.fs.mv(readPath+"/"+read_name, writePath+"/"+fname)   
dbutils.fs.rm(readPath , recurse= True)