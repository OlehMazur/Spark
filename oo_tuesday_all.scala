// Databricks notebook source
//Type of ETL: 0 (only Baltika ) 1 (only CAP) 2 (Both)

// COMMAND ----------

val type_of_ETL: Int = 2

// COMMAND ----------

//Type of data extraction: 0 (full) , 1 (incremental )

// COMMAND ----------

val type_of_data_extract: Int = 1

// COMMAND ----------

// The range of month in case of incremental loading (only if type_of_data_extract = 1 !!! )

// COMMAND ----------

val num_of_days_before_current_date: Int = 30 //number of day before current date
val num_of_days_after_current_date: Int = 30  //number of day after current date

// COMMAND ----------

//Configuration (Baltika)
val storage_account_name = "staeeprodbigdataml2c"
val storage_account_access_key = "EHYumrwso4XLSUHpvLptI33z7mumiZwZOErjrlP8FiW51Bb6NS2PaWJsqW9hsMttbZizgQjUexFZfZDBQJebYw=="
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

// COMMAND ----------

//Configuration (CAP)
spark.conf.set(
  "fs.azure.sas.dcd.prdcbwesa01.blob.core.windows.net",
  "https://prdcbwesa01.blob.core.windows.net/dcd?st=2019-09-13T15%3A01%3A24Z&se=2020-03-14T14%3A01%3A00Z&sp=rwdl&sv=2018-03-28&sr=c&sig=aErgDFXTRr3Lj519B4ZtjDHTp%2F3xsXchFqVuS2IAnGc%3D")

// COMMAND ----------

//constants

// COMMAND ----------

val readPath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/test_res.csv"
val writePath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/Result" //ETL/Result //etl_fbkp
val writePath_СAP = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/export_to_CAP"
val fname = "open_orders_etl_tuesday_with_format_All.csv"

// COMMAND ----------

//constants (CAP)

// COMMAND ----------

val writePath_GBS = "wasbs://dcd@prdcbwesa01.blob.core.windows.net/RU" 
val readPath_GBS = "wasbs://dcd@prdcbwesa01.blob.core.windows.net/RU/ru_tmp" 

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

//result export to ETl\Result

// COMMAND ----------

val query_bal =  """
  select distinct * from result_source_table 
  """

// COMMAND ----------

val sqldf_bal = spark.sql(query_bal)

// COMMAND ----------

def exportToBlobStorage_Baltika: String = { 

import com.databricks.WorkflowException
import java.io.FileNotFoundException

var Result = "Failure"   

try {
sqldf_bal.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").save(readPath)

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
  
  Result

}
//dbutils.notebook.exit(Result)



// COMMAND ----------

val query = s"""
select distinct calendar_yearmonth as partition_name, * from result_source_table 
""" +
{if (type_of_data_extract == 1) 
 s""" 
 where calendar_yearmonth between 
 year(date_add(current_date(),-1 * $num_of_days_before_current_date))*100 + month(date_add(current_date(),-1 * $num_of_days_before_current_date))
 and 
 year(date_add(current_date(),$num_of_days_after_current_date))*100 + month(date_add(current_date(),$num_of_days_after_current_date))
 """ 
 else ""}

// COMMAND ----------

val sqldf = spark.sql(query)

// COMMAND ----------

//export to CAP

// COMMAND ----------

def exportToBlobStorage (type_of_ETL:Int): String = { 

import com.databricks.WorkflowException
import java.io.FileNotFoundException

var Result = "Failure" 
val partition_field = "partition_name"
val export_format = "com.databricks.spark.csv"
val export_delimiter = Character.toString(7.toChar)

var readPath_ETL = if (type_of_ETL == 0) readPath else if (type_of_ETL == 1) readPath_GBS else null
var writePath_ETL= if (type_of_ETL == 0) writePath_СAP else if (type_of_ETL == 1) writePath_GBS else null

try {
  sqldf
  .coalesce(1)
  .write.mode("overwrite")
  .format(export_format)
  .option("header", "true")
  .option("inferSchema", "true")
  .option("delimiter", export_delimiter)
  .partitionBy(partition_field)
  .save(readPath_ETL)

  val name : String = "part-00000"   
  val path_list : Seq[String] = dbutils.fs.ls(readPath_ETL).map(_.path).filter(_.contains(partition_field))

  for (path <- path_list) {
   var partition_name = path.replace(readPath_ETL + "/" + partition_field + "=", "").replace("/", "")
   var file_list : Seq[String] = dbutils.fs.ls(path).map(_.path).filter(_.contains(name)) 
   var read_name =  if (file_list.length >= 1 ) file_list(0).replace(path + "/", "") 
   var fname = "OPENORDERSTUE_" + partition_name + "_RU_DCD"+ ".csv" 
   dbutils.fs.mv(read_name.toString , writePath_ETL+"/"+fname) 
    }
  dbutils.fs.rm(readPath_ETL , recurse = true) 
  Result = "Success" 
  } 
catch {
    case e:FileNotFoundException => println("Error, " + e)
    case e:WorkflowException  => println("Error, " + e)
  }

  Result
}



// COMMAND ----------

val Result = 
if (type_of_ETL == 0) { if (exportToBlobStorage_Baltika == "Success" &&  exportToBlobStorage(0) == "Success") "Success" else "Failure" }
else if (type_of_ETL == 1) exportToBlobStorage(1)
else if (type_of_ETL == 2) { if (exportToBlobStorage_Baltika == "Success" && exportToBlobStorage(0) == "Success" && exportToBlobStorage(1) == "Success") "Success" else "Failure" }
else "Unexpected parameter"

dbutils.notebook.exit(Result)