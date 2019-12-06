// Databricks notebook source
//Type of ETL: 0 (only Baltika ) 1 (only CAP) 2 (Both)

// COMMAND ----------

val type_of_ETL: Int = 2

// COMMAND ----------

//Type of data extraction: 0 (full) , 1 (incremental )

// COMMAND ----------

val type_of_data_extract: Int = 0

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

//Direct_Forecast.csv

// COMMAND ----------

val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/Fc_Hist.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Fc_Hist")

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

//PlantID Info

// COMMAND ----------

val file_location = file_location_path + "PlantID.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("plant_id_info")

// COMMAND ----------

val file_location = writePath  +"/" +  "Sell_in_RU_p2_with_formats_All.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Sell_in_RU_p2_with_formats")

// COMMAND ----------

val file_location = file_location_path + "Sell_in_All.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("orders_wpromo")

// COMMAND ----------

val file_location = writePath  +"/" +"MD_SKU_RU.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("MD_SKU_RU")

// COMMAND ----------

val file_location = file_location_path + "/"+ "seas_sku.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("seas_sku")

// COMMAND ----------

val file_location = file_location_path + "MD_SKU.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("MD_SKU")

// COMMAND ----------

val file_location = file_location_path + "Calendar.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Calendar")

// COMMAND ----------

val file_location = file_location_path + "MD_Clients.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("MD_Clients")

// COMMAND ----------

val file_location = file_location_path + "PlantID.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("plant_id_info")

// COMMAND ----------

//Active SKU

// COMMAND ----------

val sqldf = spark.sql("""

--	Все SKU для которых существует история продаж не менее чем в 12-ти последних месяцах (подряд, на уровне SKU по всей стране)

select SKU_Lead_ID, sum(diff) NumOfSkippedMonth
from 
(
select 
  tab.SKU_Lead_ID 
  ,
  if (
  isnull(months_between( to_date(concat(left(MonthID,4), '.', right(MonthID,2), '.', '01'), 'yyyy.MM.dd'),to_date(concat(left(MonthID_prev,4), '.', right(MonthID_prev,2), '.', '01'), 'yyyy.MM.dd'))) , 1, 
  months_between( to_date(concat(left(MonthID,4), '.', right(MonthID,2), '.', '01'), 'yyyy.MM.dd'),to_date(concat(left(MonthID_prev,4), '.', right(MonthID_prev,2), '.', '01'), 'yyyy.MM.dd'))
  )-1 diff

from (
  select   
  s.SKU_Lead_ID ,calendar.MonthID , sum (o.Actual_Volume) as Actual_Volume , LAG(calendar.MonthID, 1,0) OVER (partition by s.SKU_Lead_ID ORDER BY calendar.MonthID) MonthID_prev
  from orders_wpromo o left join md_sku s on cast(replace(o.SKUID, left(o.SKUID, 1), '') as int) = cast(s.SKU_ID as int)
  left join calendar on year(o.Date)*10000 +month(o.Date)*100 + day(o.Date)  = calendar.DateKey
  where 
  calendar.MonthID >= year(date_add(current_date(),-365))*100 + month(date_add(current_date(),-365)) 
  group by   s.SKU_Lead_ID , calendar.MonthID
  having (Actual_Volume > 0)     
  ) tab  
) tab
group by SKU_Lead_ID
having (NumOfSkippedMonth = 0)

union 

--Кроме этого активными SKU будут считаться все сезонные Subbrand. Их список в файле seas_sku.csv

select distinct md_sku_ru.lead_sku SKU_Lead_ID,  0 as NumOfSkippedMonth
from md_sku_ru 
inner join seas_sku 
on  md_sku_ru.subbrand_name = seas_sku.Subbrand
""")
sqldf.createOrReplaceTempView("Active_SKU_Only")



// COMMAND ----------

//Active Plant

// COMMAND ----------


val sqldf = spark.sql("""

--	Все Plant, которые присутствуют не меньше чем на протяжении последних 3 месяцев без перерыва

select Plant_ID, sum(diff) NumOfSkippedMonth
from 
(
select 
  tab.Plant_ID 
  ,
  if (
  isnull(months_between( to_date(concat(left(MonthID,4), '.', right(MonthID,2), '.', '01'), 'yyyy.MM.dd'),to_date(concat(left(MonthID_prev,4), '.', right(MonthID_prev,2), '.', '01'), 'yyyy.MM.dd'))) , 1, 
  months_between( to_date(concat(left(MonthID,4), '.', right(MonthID,2), '.', '01'), 'yyyy.MM.dd'),to_date(concat(left(MonthID_prev,4), '.', right(MonthID_prev,2), '.', '01'), 'yyyy.MM.dd'))
  )-1 diff

from (
    select   
  plant_id_info.Plant_ID ,calendar.MonthID , sum (o.Actual_Volume) as Actual_Volume , LAG(calendar.MonthID, 1,0) OVER (partition by plant_id_info.Plant_ID ORDER BY calendar.MonthID) MonthID_prev
  from orders_wpromo o 
  left join calendar on year(o.Date)*10000 +month(o.Date)*100 + day(o.Date)  = calendar.DateKey
  left join md_clients cl on o.AdressID =cl.AdressCode 
  left join plant_id_info on cl.ActiveAdress = plant_id_info.ActiveAdress
  where 
  calendar.MonthID >= year(date_add(current_date(),-90))*100 + month(date_add(current_date(),-90)) 
  group by   plant_id_info.Plant_ID , calendar.MonthID
  having (Actual_Volume > 0)   
  ) tab  
) tab
group by Plant_ID
having (NumOfSkippedMonth = 0)

""")
sqldf.createOrReplaceTempView("Active_Plant_Only")



// COMMAND ----------

//Active CPG

// COMMAND ----------


val sqldf = spark.sql("""

-- Все CPG, которые присутствуют не меньше чем на протяжении последних 3 месяцев без перерыва

select Client, sum(diff) NumOfSkippedMonth
from 
(
select 
  tab.Client 
  ,
  if (
  isnull(months_between( to_date(concat(left(MonthID,4), '.', right(MonthID,2), '.', '01'), 'yyyy.MM.dd'),to_date(concat(left(MonthID_prev,4), '.', right(MonthID_prev,2), '.', '01'), 'yyyy.MM.dd'))) , 1, 
  months_between( to_date(concat(left(MonthID,4), '.', right(MonthID,2), '.', '01'), 'yyyy.MM.dd'),to_date(concat(left(MonthID_prev,4), '.', right(MonthID_prev,2), '.', '01'), 'yyyy.MM.dd'))
  )-1 diff

from (
  select   
  o.Client ,calendar.MonthID , sum (o.Actual_Volume) as Actual_Volume , LAG(calendar.MonthID, 1,0) OVER (partition by o.Client ORDER BY calendar.MonthID) MonthID_prev
  from orders_wpromo o 
  left join calendar on year(o.Date)*10000 +month(o.Date)*100 + day(o.Date)  = calendar.DateKey
  where 
  calendar.MonthID >= year(date_add(current_date(),-90))*100 + month(date_add(current_date(),-90)) 
  group by   o.Client , calendar.MonthID
  having (Actual_Volume > 0)   
  ) tab  
) tab
group by Client
having (NumOfSkippedMonth = 0)

""")
sqldf.createOrReplaceTempView("Active_CPG_Only")



// COMMAND ----------

//step 1

// COMMAND ----------

val sqldf = spark.sql("""
select fc.version, year(fc.version)*10000 + month(fc.version)*100 + day(fc.version) as version_key , fc.client, fc.address, 
fc.sku_code, int(if(left(fc.sku_code,1) = "=", replace(fc.sku_code,left(fc.sku_code,1), ''), fc.sku_code )) sku_code_int,
fc.week_date, year(fc.week_date)*10000 + month(fc.week_date)*100 + day(fc.week_date) as week_date_key,  date_format(fc.week_date, 'dd.MM.yy') week_date_format,
fc.week_num,  kpi.Horizon, sum (fc.vol_dal)/10 vol_hl 
from Fc_Hist fc left join FC_KPI kpi on 
year(fc.version)*10000 + month(fc.version)*100 + day(fc.version) = int(right(kpi.Version,4))* 10000 + int(left(replace(kpi.Version, left(kpi.Version,3), ''),2)) *100 + int(left(kpi.Version,2)) and
year(fc.week_date)*10000 + month(fc.week_date)*100 + day(fc.week_date) = int(right(kpi.Week_Forecast,4))* 10000 + int(left(replace(kpi.Week_Forecast, left(kpi.Week_Forecast,3), ''),2)) *100 + int(left(kpi.Week_Forecast,2)) 
where (kpi.Horizon is not null) or (kpi.Horizon <> "\\N")
group by fc.version, fc.client, fc.address, fc.sku_code, fc.week_date, fc.week_num , kpi.Horizon
""")
sqldf.createOrReplaceTempView("FC_input")

// COMMAND ----------

//step 2

// COMMAND ----------

val sqldf = spark.sql("""
select * 
from (
select version_key, client, address,  sku_code_int, week_date_key,week_date_format, week_num, Horizon, vol_hl
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
select p.*, int(if(left(s.SKU_Lead_ID ,1) = "=", replace(s.SKU_Lead_ID ,left(s.SKU_Lead_ID ,1), ''), s.SKU_Lead_ID  )) lead_sku_code_int , c.WeekId, c.MonthId , plant.Plant_ID
from FC_input_pivot p 
left join MD_SKU s on p.sku_code_int = int(if(left(s.SKU_ID,1) = "=", replace(s.SKU_ID,left(s.SKU_ID,1), ''), s.SKU_ID ))
left join (select distinct Week, WeekId,MonthId from Calendar) c on p.week_date_format = c.Week
left join plant_id_info plant on trim(BOTH '		' from p.address) = trim(BOTH '		' from plant.ActiveAdress) --trim unicode symbols from  PlantID.csv
 """)
sqldf.createOrReplaceTempView("FC_result")

// COMMAND ----------

//step 4

// COMMAND ----------

val sqldf = spark.sql(""" 
select WeekId calendar_yearweek , MonthId calendar_yearmonth, lead_sku_code_int lead_sku , Plant_ID plant , client customer_planning_group, sum(w1) `sales_forecast_volume_w-1`, sum(w4) `sales_forecast_volume_w-4`
from FC_result 
group by WeekId, MonthId, lead_sku_code_int, Plant_ID, client
""")
sqldf.createOrReplaceTempView("FC_result_W1_W4")

// COMMAND ----------

//step 5

// COMMAND ----------

val sqldf = spark.sql(""" 
select MonthId calendar_yearmonth,  WeekId calendar_yearweek , lead_sku_code_int lead_sku , Plant_ID plant , client customer_planning_group, sum(m3) `forecast_volume_m-3`
from FC_result 
group by MonthId, WeekId, lead_sku_code_int, Plant_ID, client
""" )
sqldf.createOrReplaceTempView("FC_result_M3")

// COMMAND ----------

//fc_direct_weekly

// COMMAND ----------

val query_full_week = s"""
select *
from (
select  fc.* , sell.total_shipments_volume

from FC_result_W1_W4 fc 

left join 
(
select calendar_yearweek, lead_sku, plant, customer_planning_group, sum (total_shipments_volume_hl) total_shipments_volume
from Sell_in_RU_p2_with_formats 
group by calendar_yearweek, lead_sku, plant, customer_planning_group
) sell
on 
fc.calendar_yearweek = sell.calendar_yearweek and fc.lead_sku = sell.lead_sku and fc.plant = sell.plant and fc.customer_planning_group = sell.customer_planning_group

/*
where
(int(fc.lead_sku) in (select distinct int(SKU_Lead_ID) from Active_SKU_Only)) and
(fc.plant in (select distinct Plant_ID from Active_Plant_Only where Plant_ID is not null )) and 
(fc.customer_planning_group in (select distinct Client from Active_CPG_Only where Client is not null))
*/

) tab
""" 

// COMMAND ----------

val sqldf_full_week = spark.sql(query_full_week)

// COMMAND ----------

//Export to ETL/Result

// COMMAND ----------

def exportToBlobStorage_Baltika_week: String = { 

import com.databricks.WorkflowException
import java.io.FileNotFoundException

val fname = "fc_acc_week.csv" 
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

//Export to CAP

// COMMAND ----------

val query_incremental_week = s"""
select *
from (
select fc.calendar_yearmonth partition_name,  fc.* , sell.total_shipments_volume

from FC_result_W1_W4 fc 

left join 
(
select calendar_yearweek, lead_sku, plant, customer_planning_group, sum (total_shipments_volume_hl) total_shipments_volume
from Sell_in_RU_p2_with_formats 
group by calendar_yearweek, lead_sku, plant, customer_planning_group
) sell
on 
fc.calendar_yearweek = sell.calendar_yearweek and fc.lead_sku = sell.lead_sku and fc.plant = sell.plant and fc.customer_planning_group = sell.customer_planning_group

/*
where
(int(fc.lead_sku) in (select distinct int(SKU_Lead_ID) from Active_SKU_Only)) and
(fc.plant in (select distinct Plant_ID from Active_Plant_Only where Plant_ID is not null )) and 
(fc.customer_planning_group in (select distinct Client from Active_CPG_Only where Client is not null))
*/

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
   var fname = "FCACCWEEKDIRECT_" + partition_name + "_RU_DCD"+ ".csv" 
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

//fc_direct_monthly

// COMMAND ----------

val query_full_month = s"""
select * 
from( 
select  fc.*, sell.actual_sales_volume 

from FC_result_M3 fc 

left join 
(
select calendar_yearmonth,calendar_yearweek, lead_sku, plant, customer_planning_group, sum (total_shipments_volume_hl) actual_sales_volume
from Sell_in_RU_p2_with_formats 
group by calendar_yearmonth,calendar_yearweek, lead_sku, plant, customer_planning_group
) sell 
on 
fc.calendar_yearmonth = sell.calendar_yearmonth and fc.calendar_yearweek = sell.calendar_yearweek and fc.lead_sku = sell.lead_sku and fc.plant = sell.plant and fc.customer_planning_group = sell.customer_planning_group

/*
where
  (int(fc.lead_sku) in (select distinct int(SKU_Lead_ID) from Active_SKU_Only)) and
  (fc.plant in (select distinct Plant_ID from Active_Plant_Only where Plant_ID is not null )) and 
  (fc.customer_planning_group in (select distinct Client from Active_CPG_Only where Client is not null))
*/  

) tab 

""" 

// COMMAND ----------

val sqldf_full_month = spark.sql(query_full_month)

// COMMAND ----------

//Export to ETL/Result

// COMMAND ----------

def exportToBlobStorage_Baltika_month: String = { 

import com.databricks.WorkflowException
import java.io.FileNotFoundException

val fname = "fc_acc_month.csv" 
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

val query_incremental_month  = s"""
select * 
from( 
select fc.calendar_yearmonth partition_name,  fc.*, sell.actual_sales_volume 

from FC_result_M3 fc 

left join 
(
select calendar_yearmonth,calendar_yearweek, lead_sku, plant, customer_planning_group, sum (total_shipments_volume_hl) actual_sales_volume
from Sell_in_RU_p2_with_formats 
group by calendar_yearmonth,calendar_yearweek, lead_sku, plant, customer_planning_group
) sell 
on 
fc.calendar_yearmonth = sell.calendar_yearmonth and fc.calendar_yearweek = sell.calendar_yearweek and fc.lead_sku = sell.lead_sku and fc.plant = sell.plant and fc.customer_planning_group = sell.customer_planning_group

/*
where
  (int(fc.lead_sku) in (select distinct int(SKU_Lead_ID) from Active_SKU_Only)) and
  (fc.plant in (select distinct Plant_ID from Active_Plant_Only where Plant_ID is not null )) and 
  (fc.customer_planning_group in (select distinct Client from Active_CPG_Only where Client is not null))
*/  

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
   var fname = "FCACCMONTHDIRECT_" + partition_name + "_RU_DCD"+ ".csv" 
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
if (type_of_ETL == 0) { if ( exportToBlobStorage_Baltika_week == "Success"  && exportToBlobStorage_Baltika_month == "Success" && exportToBlobStorage_week(0) == "Success"  && exportToBlobStorage_month(0) == "Success") "Success" else "Failure"  }
else if (type_of_ETL == 1) { if (exportToBlobStorage_week(1) == "Success"  && exportToBlobStorage_month(1) == "Success" ) "Success" else  "Failure"}
else if (type_of_ETL == 2) { if ( exportToBlobStorage_Baltika_week == "Success"  && exportToBlobStorage_Baltika_month == "Success" && exportToBlobStorage_week(0) == "Success"  && exportToBlobStorage_month(0) == "Success" && exportToBlobStorage_week(1) == "Success"  && exportToBlobStorage_month(1) == "Success") "Success" else "Failure" }
else "Unexpected parameter"

dbutils.notebook.exit(Result)