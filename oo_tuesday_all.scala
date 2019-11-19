// Databricks notebook source
//Configuration
val storage_account_name = "staeeprodbigdataml2c"
val storage_account_access_key = "EHYumrwso4XLSUHpvLptI33z7mumiZwZOErjrlP8FiW51Bb6NS2PaWJsqW9hsMttbZizgQjUexFZfZDBQJebYw=="
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

// COMMAND ----------

//constants

// COMMAND ----------

val readPath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/test_res.csv"
val writePath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/Result" //ETL/Result //etl_fbkp
val fname = "open_orders_etl_tuesday_with_format_All.csv"

// COMMAND ----------

//MD_Clients

// COMMAND ----------

val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/MD_Clients.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("MD_Clients")

// COMMAND ----------

//Divisions

// COMMAND ----------

val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/Division.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Divisions")

// COMMAND ----------

//MD_SKU

// COMMAND ----------

val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/MD_SKU.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("MD_SKU")

// COMMAND ----------

//Calendar

// COMMAND ----------

val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/Calendar.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Calendar")

// COMMAND ----------

//orders_wpromo_v4.csv/Sell_in

// COMMAND ----------

val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/Sell_in_All.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("orders_wpromo")

// COMMAND ----------

//PlantID

// COMMAND ----------

val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/PlantID.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("plant_id_info")

// COMMAND ----------

//CPG_Formats

// COMMAND ----------

val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/CPG_Formats.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("CPG_Formats")

// COMMAND ----------

//Result open_orders_etl.csv with formats Tuesday

// COMMAND ----------

val sqldf = spark.sql(
  """
select lead_sku, customer_planning_group, plant_name, plant, load_week, calendar_yearweek, calendar_yearmonth, division, format, concat(customer_planning_group, '_',format) as cpg_format, open_orders_promo, open_orders as open_orders_total, open_orders - open_orders_promo as open_orders 
from 
(
select 
replace (replace (md_sku.SKU_Lead_ID ,51070074, 510700 ) ,38070074, 380700 ) lead_sku , ow.Client customer_planning_group ,  cl.ActiveAdress plant_name, plant_id_info.Plant_ID plant,   tuesday_info.weekid load_week,  calendar.WeekId calendar_yearweek, calendar.MonthId calendar_yearmonth, dev.division,
sum (if ( ow.Promo_BL = "\\N" or Promo_BL is null, 0, Order_Volume/10 ) ) open_orders_promo, sum (Order_Volume/10) open_orders,
if(tuesday_info.weekid = calendar.WeekId, 1, 0) exclude_week, 

--if(cl.New2 = 'СМ/ГМ', 'Супермаркет', cl.New2) format
if (
ow.Client = 'X5 Retail Group' or ow.Client = 'Магнит (Тандер)' ,  
  if(cl.New2 = 'СМ/ГМ', 'Супермаркет', if(isnull(cl.New2) or cl.New2 = "\\N", 'Минимаркет', cl.New2 )) , 
  if(isnull(cpgf.format) or cpgf.format = "\\N" or cpgf.format = 'Локальные клиенты' , 'LKA', if (right(cpgf.format,1) = 'ы', replace(cpgf.format,right(cpgf.format,1), '' ), cpgf.format ) )
  ) format

from orders_wpromo ow left join md_clients cl on ow.AdressID = cl.AdressCode left join plant_id_info on cl.ActiveAdress = plant_id_info.ActiveAdress 
left join md_sku on if ( isnull(cast( left(ow.SKUID,1) as int)), cast(replace(ow.SKUID, left( ow.SKUID, 1), '') as int), cast(ow.SKUID as int) ) = md_sku.SKU_ID
left join Divisions dev on  cl.ActiveAdress = dev.WH left join calendar on year(ow.Date)*10000 +month(ow.Date)*100 + day(ow.Date)  = calendar.DateKey
left join (
  select datekey tuesday, monthid, weekid
  from calendar
  where 
  dayofweek(concat( left(DateKey, 4),  '-',  left(replace(DateKey, left(DateKey, 4), ''), 2) , '-', left(replace(DateKey, left(DateKey, 6), ''), 2))) -1  = 2 --Tuesday/Friday
  and 
   cast (concat( left(DateKey, 4),  '-',  left(replace(DateKey, left(DateKey, 4), ''), 2) , '-', 
            left(replace(DateKey, left(DateKey, 6), ''), 2)) as date) <= (select max(Date) from  orders_wpromo)

) tuesday_info
on 
year(ow.OrderDate)*10000 +month(ow.OrderDate)*100 + day(ow.OrderDate) <= tuesday_info.tuesday 
and tuesday_info.tuesday  < year(ow.Date)*10000 +month(ow.Date)*100 + day(ow.Date)

left join CPG_Formats cpgf on ow.Client = cpgf.CPG

where 
plant_id_info.Plant_ID is not null 
and md_sku.SKU_Lead_ID is not null
--and dev.division in ('C', 'NW')
and if(year(ow.OrderDate)*10000 +month(ow.OrderDate)*100 + day(ow.OrderDate) <= tuesday_info.tuesday and tuesday_info.tuesday < year(ow.Date)*10000 +month(ow.Date)*100 + day(ow.Date), 1, 0) = 1 
and calendar.WeekId >= 201601 
and tuesday_info.weekid >= 201601
group by 
md_sku.SKU_Lead_ID, ow.Client, cl.ActiveAdress, plant_id_info.Plant_ID, calendar.WeekId , tuesday_info.weekid, calendar.MonthId, dev.division, cl.New2, cpgf.format 
)tab
where tab.exclude_week = 0
  """
  )
sqldf.createOrReplaceTempView("result_source_table")

// COMMAND ----------

//result export

// COMMAND ----------

import com.databricks.WorkflowException
import java.io.FileNotFoundException

var Result = "Failure"   

try {
val sqldf = spark.sql(
  """
  select distinct * from result_source_table 
  """
  )
sqldf.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").save(readPath)

val name : String = "part-00000"  
val file_list : Seq[String] = dbutils.fs.ls(readPath).map(_.path).filter(_.contains(name))
val read_name = if (file_list.length >= 1 ) file_list(0).replace(readPath + "/", "")
val row_count = spark.read.format("csv").option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_list(0)).count   
dbutils.fs.mv(readPath+"/"+read_name , writePath+"/"+fname)   
dbutils.fs.rm(readPath , recurse = true) 
if (row_count > 0) Result = "Success" else println("The file " +writePath+"/"+fname + " is empty !" )
} 
catch {
  case e:FileNotFoundException => println("Error, " + e)
  case e:WorkflowException  => println("Error, " + e)
}

dbutils.notebook.exit(Result)

