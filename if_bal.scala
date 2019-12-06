// Databricks notebook source
//Type of ETL: 0 (only Baltika ) 1 (only CAP) 2 (Both)

// COMMAND ----------

val type_of_ETL: Int = 1

// COMMAND ----------

//Type of data extraction: 0 (full) , 1 (incremental )

// COMMAND ----------

val type_of_data_extract: Int = 1

// COMMAND ----------

// The range of month in case of incremental loading (only if type_of_data_extract = 1 !!! )

// COMMAND ----------

val num_of_days_before_current_date: Int = 30
//val num_of_days_after_current_date: Int = 30  

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
val file_location_path = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/"

// COMMAND ----------

//constants (CAP)

// COMMAND ----------

val writePath_GBS = "wasbs://dcd@prdcbwesa01.blob.core.windows.net/RU" 
val readPath_GBS = "wasbs://dcd@prdcbwesa01.blob.core.windows.net/RU/ru_tmp" 

// COMMAND ----------

//Indirect_Forecast.csv

// COMMAND ----------

val export_delimiter = Character.toString(7.toChar)
val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/Indirect_Forecast.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", export_delimiter).option("header", "true").load(file_location)
df.createOrReplaceTempView("Indirect_Forecast")

// COMMAND ----------

// %sql select max(cast(date as date)) from Indirect_Forecast limit 1

// COMMAND ----------

//FC_KPI

// COMMAND ----------

val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/FC_KPI.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("FC_KPI")

// COMMAND ----------

//Calendar

// COMMAND ----------

val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/Calendar.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Calendar")

// COMMAND ----------

//MD_SKU

// COMMAND ----------

val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/MD_SKU.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("MD_SKU")

// COMMAND ----------

//IndDirect, InDirect_Product

// COMMAND ----------

//InDirect_Product2 with BEL delimiter

// COMMAND ----------

val export_delimiter = Character.toString(7.toChar)
val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/InDirect_Product.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", export_delimiter).option("header", "true").load(file_location)
df.createOrReplaceTempView("IndirectSales")

// COMMAND ----------

//Sell-in

// COMMAND ----------

// val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/Sell_in_All.csv"
// val file_type = "csv"
// val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
// df.createOrReplaceTempView("Orders_Main")

// COMMAND ----------

//MD_SKU

// COMMAND ----------

val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/MD_SKU.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("MD_SKU")

// COMMAND ----------

//MD_SKU_RU

// COMMAND ----------

// val file_location = writePath + "/"+ "MD_SKU_RU.csv"
// val file_type = "csv"
// val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
// df.createOrReplaceTempView("MD_SKU_RU")

// COMMAND ----------

//seas_sku

// COMMAND ----------

// val file_location = file_location_path + "/"+ "seas_sku.csv"
// val file_type = "csv"
// val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
// df.createOrReplaceTempView("seas_sku")

// COMMAND ----------

//Active SKU

// COMMAND ----------

// val sqldf = spark.sql("""

// --	Все SKU для которых существует история продаж не менее чем в 12-ти последних месяцах (подряд, на уровне SKU по всей стране)

// select SKU_Lead_ID, sum(diff) NumOfSkippedMonth
// from 
// (
// select 
//   tab.SKU_Lead_ID 
//   ,
//   if (
//   isnull(months_between( to_date(concat(left(MonthID,4), '.', right(MonthID,2), '.', '01'), 'yyyy.MM.dd'),to_date(concat(left(MonthID_prev,4), '.', right(MonthID_prev,2), '.', '01'), 'yyyy.MM.dd'))) , 1, 
//   months_between( to_date(concat(left(MonthID,4), '.', right(MonthID,2), '.', '01'), 'yyyy.MM.dd'),to_date(concat(left(MonthID_prev,4), '.', right(MonthID_prev,2), '.', '01'), 'yyyy.MM.dd'))
//   )-1 diff

// from (
//   select   
//   s.SKU_Lead_ID ,calendar.MonthID , sum (o.Actual_Volume) as Actual_Volume , LAG(calendar.MonthID, 1,0) OVER (partition by s.SKU_Lead_ID ORDER BY calendar.MonthID) MonthID_prev
//   from orders_main o left join md_sku s on cast(replace(o.SKUID, left(o.SKUID, 1), '') as int) = cast(s.SKU_ID as int)
//   left join calendar on year(o.Date)*10000 +month(o.Date)*100 + day(o.Date)  = calendar.DateKey
//   where 
//   calendar.MonthID >= year(date_add(current_date(),-365))*100 + month(date_add(current_date(),-365)) 
//   group by   s.SKU_Lead_ID , calendar.MonthID
//   having (Actual_Volume > 0)     
//   ) tab  
// ) tab
// group by SKU_Lead_ID
// having (NumOfSkippedMonth = 0)

// union 

// --Кроме этого активными SKU будут считаться все сезонные Subbrand. Их список в файле seas_sku.csv

// select distinct md_sku_ru.lead_sku SKU_Lead_ID,  0 as NumOfSkippedMonth
// from md_sku_ru 
// inner join seas_sku 
// on  md_sku_ru.subbrand_name = seas_sku.Subbrand
// """)
// sqldf.createOrReplaceTempView("Active_SKU_Only")



// COMMAND ----------

// %sql 
// select * from IndirectSales limit 1

// COMMAND ----------

// %sql 
// select * from Indirect_Forecast limit 1

// COMMAND ----------

//step 1

// COMMAND ----------

//Indirect_RU

// COMMAND ----------

val query = s"""

select *
from (
select  
cast( if(left(s.Lead_SKUID, 1) = "=", replace(s.Lead_SKUID, left(s.Lead_SKUID, 1), ''), s.Lead_SKUID) as double) lead_sku  , 
s.AD_ID, 
s.PGO,
cl.WeekId calendar_yearweek, 
cl.MonthId calendar_yearmonth, 
sum(s.Volume_dal)/10 volume_hl

from IndirectSales s 
left join Calendar cl on s.Date = cl.DayName 

where cl.WeekId >= 201601 
--and int(if(left(s.Lead_SKUID, 1) = "=", replace(s.Lead_SKUID, left(s.Lead_SKUID, 1), ''), s.Lead_SKUID)) in (select int(SKU_Lead_ID) from  Active_SKU_Only)

group by s.Lead_SKUID, cl.WeekId, cl.MonthId, s.AD_ID, s.PGO
) tab

""" 

// COMMAND ----------

val sqldf = spark.sql(query)
sqldf.createOrReplaceTempView("InActuals")

// COMMAND ----------

// %sql select * from IndirectSales limit 1

// COMMAND ----------

// %sql select * from Indirect_Forecast  limit 1

// COMMAND ----------

val sqldf = spark.sql("""
select cast (fc.`Period of Plan` as timestamp) as version, year(cast (fc.`Period of Plan` as timestamp))*10000 + month(cast (fc.`Period of Plan` as timestamp))*100 + day(cast (fc.`Period of Plan` as timestamp)) as version_key , fc.SKU_ID, int(if(left(fc.SKU_ID,1) = "=", replace(fc.SKU_ID,left(fc.SKU_ID,1), ''), fc.SKU_ID )) sku_code_int, fc.`Active address` plant_name, fc.`Code of Active address` plant,
fc.Date, year(fc.Date)*10000 + month(fc.Date)*100 + day(fc.Date) as week_date_key,  date_format(fc.Date, 'dd.MM.yy') week_date_format, kpi.Horizon, sum (fc.Forecast)/10 vol_hl 
from Indirect_Forecast fc left join FC_KPI kpi on 
year(cast (fc.`Period of Plan` as timestamp) )*10000 + month(cast (fc.`Period of Plan` as timestamp))*100 + day(cast (fc.`Period of Plan` as timestamp)) = int(right(kpi.Version,4))* 10000 + int(left(replace(kpi.Version, left(kpi.Version,3), ''),2)) *100 + int(left(kpi.Version,2)) and
year(fc.Date)*10000 + month(fc.Date)*100 + day(fc.Date) = int(right(kpi.Week_Forecast,4))* 10000 + int(left(replace(kpi.Week_Forecast, left(kpi.Week_Forecast,3), ''),2)) *100 + int(left(kpi.Week_Forecast,2)) 
where (kpi.Horizon is not null) or (kpi.Horizon <> "\\N")
group by fc.`Period of Plan`, fc.SKU_ID, fc.Date , kpi.Horizon, fc.`Active address`, fc.`Code of Active address`
""")
sqldf.createOrReplaceTempView("FC_input")

// COMMAND ----------

//step 2

// COMMAND ----------

val sqldf = spark.sql("""
select * 
from (
select version_key,  sku_code_int, week_date_key,week_date_format,  Horizon, plant_name, plant, vol_hl
from FC_input
)
pivot 
(
  sum(vol_hl)
  for Horizon in ('w1', 'w4', 'm3')
)
""") 
sqldf.createOrReplaceTempView("FC_input_pivot")

// COMMAND ----------

//step 3

// COMMAND ----------

val sqldf = spark.sql("""
select p.* , c.WeekId, c.MonthId, int(if(left(s.SKU_Lead_ID ,1) = "=", replace(s.SKU_Lead_ID ,left(s.SKU_Lead_ID ,1), ''), s.SKU_Lead_ID  )) lead_sku_code_int
from FC_input_pivot p 
left join (select distinct Week, WeekId,MonthId from Calendar) c on p.week_date_format = c.Week
left join MD_SKU s on p.sku_code_int = int(if(left(s.SKU_ID,1) = "=", replace(s.SKU_ID,left(s.SKU_ID,1), ''), s.SKU_ID ))
""")
sqldf.createOrReplaceTempView("FC_result")

// COMMAND ----------

//step 4

// COMMAND ----------

val sqldf = spark.sql(""" 
select MonthId calendar_yearmonth, WeekId calendar_yearweek , lead_sku_code_int as lead_sku ,  plant, plant_name, sum(w1) `sales_forecast_volume_w-1`, sum(w4) `sales_forecast_volume_w-4`
from FC_result 
group by MonthId, WeekId, lead_sku_code_int, plant, plant_name
""")
sqldf.createOrReplaceTempView("FC_result_W1_W4")

// COMMAND ----------

//step 5

// COMMAND ----------

val sqldf = spark.sql(""" 
select MonthId calendar_yearmonth,  WeekId calendar_yearweek , lead_sku_code_int lead_sku,  plant, plant_name, sum(m3) `forecast_volume_m-3`
from FC_result 
group by MonthId, WeekId, lead_sku_code_int,  plant, plant_name
""" )
sqldf.createOrReplaceTempView("FC_result_M3")

// COMMAND ----------

//fc_indirect_weekly

// COMMAND ----------

val query_full_week = s"""
select *
from (
  select   
  main.* ,  fc.`sales_forecast_volume_w-1`, fc.`sales_forecast_volume_w-4`,   ac.volume_hl as actual_sales_volume
  from 
  (
  select distinct calendar_yearmonth,calendar_yearweek, lead_sku, plant, replace(replace(plant_name,'"' , ''), ';', '') plant_name from   FC_result_W1_W4
  union
  select distinct calendar_yearmonth,calendar_yearweek, lead_sku, PGO plant, replace(replace(AD_ID,'"' , ''), ';', '')  plant_name  from InActuals
  ) main
  left join FC_result_W1_W4 fc  on 
  main.calendar_yearmonth = fc.calendar_yearmonth and
  main.calendar_yearweek = fc.calendar_yearweek and 
  main.lead_sku = fc.lead_sku and
  main.plant = fc.plant 
  
  left join InActuals ac on 
  main.calendar_yearmonth = ac.calendar_yearmonth and
  main.calendar_yearweek = ac.calendar_yearweek and 
  main.lead_sku = ac.lead_sku and
  main.plant = ac.PGO 
) tab

"""

// COMMAND ----------

val sqldf_full_week = spark.sql(query_full_week)

// COMMAND ----------

//Export to ETL/Reult

// COMMAND ----------

def exportToBlobStorage_Baltika_week: String = { 

import com.databricks.WorkflowException
import java.io.FileNotFoundException

val fname = "fc_indirect_weekly.csv" 
var Result = "Failure"   

try {
sqldf_full_week.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").save(readPath)

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

val query_incremental_week = s"""
select *
from (
  select   
  main.calendar_yearmonth as partition_name , main.* ,  fc.`sales_forecast_volume_w-1`, fc.`sales_forecast_volume_w-4`,   ac.volume_hl as actual_sales_volume
  from 
  (
  select distinct calendar_yearmonth,calendar_yearweek, lead_sku, plant, replace(replace(plant_name,'"' , ''), ';', '') plant_name from   FC_result_W1_W4
  union
  select distinct calendar_yearmonth,calendar_yearweek, lead_sku, PGO plant, replace(replace(AD_ID,'"' , ''), ';', '')  plant_name  from InActuals
  ) main
  left join FC_result_W1_W4 fc  on 
  main.calendar_yearmonth = fc.calendar_yearmonth and
  main.calendar_yearweek = fc.calendar_yearweek and 
  main.lead_sku = fc.lead_sku and
  main.plant = fc.plant 
  
  left join InActuals ac on 
  main.calendar_yearmonth = ac.calendar_yearmonth and
  main.calendar_yearweek = ac.calendar_yearweek and 
  main.lead_sku = ac.lead_sku and
  main.plant = ac.PGO 
) tab

""" +
{if (type_of_data_extract == 1) 
 s""" 
 where calendar_yearmonth >= year(date_add(current_date(),-1 * $num_of_days_before_current_date))*100 + month(date_add(current_date(),-1 * $num_of_days_before_current_date))
 
 """ 
 else ""}

// COMMAND ----------

val sqldf_incremental_week = spark.sql(query_incremental_week)

// COMMAND ----------

def exportToBlobStorage_week (type_of_ETL:Int): String = { 

import com.databricks.WorkflowException
import java.io.FileNotFoundException

var Result = "Failure" 
val partition_field = "partition_name"
val export_format = "com.databricks.spark.csv"
val export_delimiter = Character.toString(7.toChar)

var readPath_ETL = if (type_of_ETL == 0) readPath else if (type_of_ETL == 1) readPath_GBS else null
var writePath_ETL= if (type_of_ETL == 0) writePath_СAP else if (type_of_ETL == 1) writePath_GBS else null

try {
  sqldf_incremental_week
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
   var fname = "FCACCWEEKINDIRECT_" + partition_name + "_RU_DCD"+ ".csv" 
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

//fc_indirect_monthly

// COMMAND ----------

val query_full_month = s"""
select *
From (
select  main.* ,fc.`forecast_volume_m-3`, ac.volume_hl as actual_sales_volume
from 
  (
  select distinct calendar_yearmonth,calendar_yearweek, lead_sku, plant, replace(replace(plant_name,'"' , ''), ';', '') plant_name  from  FC_result_M3
  union
  select distinct calendar_yearmonth,calendar_yearweek, lead_sku, PGO plant, replace(replace(AD_ID,'"' , ''), ';', '')  plant_name   from InActuals
  ) main

left join FC_result_M3 fc  on 
  main.calendar_yearmonth = fc.calendar_yearmonth and
  main.calendar_yearweek = fc.calendar_yearweek and 
  main.lead_sku = fc.lead_sku and
  main.plant = fc.plant
  
left join InActuals ac on 
  main.calendar_yearmonth = ac.calendar_yearmonth and
  main.calendar_yearweek = ac.calendar_yearweek and 
  main.lead_sku = ac.lead_sku and
  main.plant = ac.PGO 
  
  ) tab
""" 

// COMMAND ----------

val sqldf_full_month = spark.sql(query_full_month)

// COMMAND ----------

def exportToBlobStorage_Baltika_month: String = { 

import com.databricks.WorkflowException
import java.io.FileNotFoundException

val fname = "fc_indirect_monthly.csv" 
var Result = "Failure"   

try {
sqldf_full_month.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").save(readPath)

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

val query_incremental_month = s"""
select *
From (
select  main.calendar_yearmonth as partition_name,  main.* ,fc.`forecast_volume_m-3`, ac.volume_hl as actual_sales_volume
from 
  (
  select distinct calendar_yearmonth,calendar_yearweek, lead_sku, plant, replace(replace(plant_name,'"' , ''), ';', '') plant_name  from  FC_result_M3
  union
  select distinct calendar_yearmonth,calendar_yearweek, lead_sku, PGO plant, replace(replace(AD_ID,'"' , ''), ';', '')  plant_name   from InActuals
  ) main

left join FC_result_M3 fc  on 
  main.calendar_yearmonth = fc.calendar_yearmonth and
  main.calendar_yearweek = fc.calendar_yearweek and 
  main.lead_sku = fc.lead_sku and
  main.plant = fc.plant
  
left join InActuals ac on 
  main.calendar_yearmonth = ac.calendar_yearmonth and
  main.calendar_yearweek = ac.calendar_yearweek and 
  main.lead_sku = ac.lead_sku and
  main.plant = ac.PGO 
  
  ) tab
""" +
{if (type_of_data_extract == 1) 
 s""" 
 where calendar_yearmonth >= year(date_add(current_date(),-1 * $num_of_days_before_current_date))*100 + month(date_add(current_date(),-1 * $num_of_days_before_current_date))
 
 """ 
 else ""}

// COMMAND ----------

val sqldf_incremental_month = spark.sql(query_incremental_month)

// COMMAND ----------

def exportToBlobStorage_month (type_of_ETL:Int): String = { 

import com.databricks.WorkflowException
import java.io.FileNotFoundException

var Result = "Failure" 
val partition_field = "partition_name"
val export_format = "com.databricks.spark.csv"
val export_delimiter = Character.toString(7.toChar)

var readPath_ETL = if (type_of_ETL == 0) readPath else if (type_of_ETL == 1) readPath_GBS else null
var writePath_ETL= if (type_of_ETL == 0) writePath_СAP else if (type_of_ETL == 1) writePath_GBS else null

try {
  sqldf_incremental_month
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
   var fname = "FCACCMONTHINDIRECT_" + partition_name + "_RU_DCD"+ ".csv" 
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
if (type_of_ETL == 0) { if (exportToBlobStorage_Baltika_week == "Success" && exportToBlobStorage_Baltika_month == "Success" && exportToBlobStorage_week(0) == "Success"  && exportToBlobStorage_month(0) == "Success" ) "Success" else  "Failure" }
else if (type_of_ETL == 1) { if (exportToBlobStorage_week(1) == "Success"  && exportToBlobStorage_month(1) == "Success" ) "Success" else "Failure"  }
else if (type_of_ETL == 2) { if (exportToBlobStorage_Baltika_week == "Success" && exportToBlobStorage_Baltika_month == "Success" && exportToBlobStorage_week(0) == "Success"  && exportToBlobStorage_month(0) == "Success" && exportToBlobStorage_week(1) == "Success"  && exportToBlobStorage_month(1) == "Success" ) "Success" else "Failure" }
else "Unexpected parameter"

dbutils.notebook.exit(Result)