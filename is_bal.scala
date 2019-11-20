// Databricks notebook source
//Type of ETL: 0 (only Baltika ) 1 (only CAP) 2 (Both)

// COMMAND ----------

val type_of_ETL: Int = 0

// COMMAND ----------

//Type of data extraction: 0 (full) , 1 (incremental )

// COMMAND ----------

val type_of_data_extract: Int = 0

// COMMAND ----------

// The range of month in case of incremental loading (only if type_of_data_extract = 1 !!! )

// COMMAND ----------

val num_of_days_before_current_date: Int = 90 // 30 number of day before current date  !!!!!INDIRECT DOEST HAVE DF UPDATES !!!
val num_of_days_after_current_date: Int = 30  // 30 number of day after current date   !!!!!INDIRECT DOEST HAVE DF UPDATES !!!

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
val fname = "Indirect_RU.csv"

// COMMAND ----------

//constants (CAP)

// COMMAND ----------

val writePath_GBS = "wasbs://dcd@prdcbwesa01.blob.core.windows.net/RU" 
val readPath_GBS = "wasbs://dcd@prdcbwesa01.blob.core.windows.net/RU/ru_tmp" 

// COMMAND ----------

//Calendar

// COMMAND ----------

val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/Calendar.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Calendar")

// COMMAND ----------

//InDirect_Product2 with BEL delimiter

// COMMAND ----------

val export_delimiter = Character.toString(7.toChar)
val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/InDirect_Product.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", export_delimiter).option("header", "true").load(file_location)
df.createOrReplaceTempView("IndirectSales")

// COMMAND ----------

//numbers of week back from current date, if "0" - current week

// COMMAND ----------

// %sql select max(Date) max_date from IndirectSales where Date is not null or Date <> "\\N" ----2019-09-29T00:00:00.000+0000


// COMMAND ----------

// %sql 
// select 
// (select distinct Weekid from Calendar where DayName = cast(current_date() as timestamp) ) - 
// (select distinct Weekid from Calendar where DayName = (select max(Date) max_date from IndirectSales where Date is not null or Date <> "\\N" ) ) as numbers_of_weeks_back


// COMMAND ----------

val numbers_of_weeks_back: Int = 0

// COMMAND ----------

//numbers of week ahead from current date

// COMMAND ----------

val numbers_of_weeks_ahead: Int = 78

// COMMAND ----------

//numbers of week ahead from current date

// COMMAND ----------

import java.time.LocalDateTime

val mv  =LocalDateTime.now.plusWeeks(-1 * numbers_of_weeks_back)
val current_minus_n_weeks = mv.getYear * 10000 + mv.getMonthValue *100 +mv.getDayOfMonth
val query_mv = s"select DayName as report_date, WeekId from Calendar where datekey = $current_minus_n_weeks" 
val sqldf_mv = spark.sql(query_mv)
sqldf_mv.createOrReplaceTempView("report_week_info")

// val pp  =LocalDateTime.now.plusWeeks(numbers_of_weeks_ahead)
// val current_plus_n_weeks = pp.getYear * 10000 + pp.getMonthValue *100 +pp.getDayOfMonth
// val query = s"select WeekId from Calendar where datekey = $current_plus_n_weeks" 
// val sqldf = spark.sql(query)
// sqldf.createOrReplaceTempView("till_week_info")

val pp  = mv.plusWeeks(numbers_of_weeks_ahead)
val current_plus_n_weeks = pp.getYear * 10000 + pp.getMonthValue *100 +pp.getDayOfMonth
val query = s"select WeekId from Calendar where datekey = $current_plus_n_weeks" 
val sqldf = spark.sql(query)
sqldf.createOrReplaceTempView("till_week_info")


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
//      select   int(if(left(s.SKU_Lead_ID,1) = "=", replace(s.SKU_Lead_ID,left(s.SKU_Lead_ID,1), ''), s.SKU_Lead_ID )) SKU_Lead_ID , sum (o.Actual_Volume) as Actual_Volume 
//      from orders_main o left join md_sku s on cast(replace(o.SKUID, left(o.SKUID, 1), '') as int) = int(if(left(s.SKU_ID,1) = "=", replace(s.SKU_ID,left(s.SKU_ID,1), ''), s.SKU_ID ))
//      left join calendar on year(o.Date)*10000 +month(o.Date)*100 + day(o.Date)  = calendar.DateKey
//      where 
//      --int(MonthID/100) = 2018
//      calendar.MonthID <= year(date_add(current_date(),-365))*100 + month(date_add(current_date(),-365)) 
//      group by   s.SKU_Lead_ID
//      having (Actual_Volume > 0)
//      """) 
// sqldf.createOrReplaceTempView("Active_SKU")

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

//Indirect_Active_SKU

// COMMAND ----------

// val file_location = writePath  +"/" +  "Indirect_Active_SKU.csv"
// val file_type = "csv"
// val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
// df.createOrReplaceTempView("Indirect_Active_SKU")

// COMMAND ----------

//Indirect_Active_PGO_Distr

// COMMAND ----------

// val file_location = writePath  +"/" +  "Indirect_Active_PGO_Distr.csv"
// val file_type = "csv"
// val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
// df.createOrReplaceTempView("Indirect_Active_PGO_Distr")

// COMMAND ----------

// val query = s"""
// select main.* , if (files.Lead_SKUID is not null and files.Distributor is not null  and files.PGO is not null , 1, 0 ) Active_files,
// if (summ.Lead_SKUID is not null and summ.Distributor is not null  and summ.PGO is not null , 1, 0 ) Active_sales, 
// summ.volume_hl  Vol_3m_hl 

// from (
// select distinct s.Lead_SKUID, replace(replace(concat(s.OP, s.Distributor, s.Sub_channel), '"',''), ' ', '') Distributor , s.PGO from IndirectSales s

// union 
//   (
//   select tab1.active_lead_SKU Lead_SKUID, replace(replace(tab2.Distr, '"', ''), ' ', '' ) Distributor, tab2.PGO
//   from 
//   (select distinct active_lead_SKU from Indirect_Active_SKU) tab1
//   cross join 
//   (select distinct Distr, PGO from Indirect_Active_PGO_Distr) tab2
//   )
// ) main

// left join 

//  (
//   select tab1.active_lead_SKU Lead_SKUID, replace(replace(tab2.Distr, '"', ''), ' ', '' )  Distributor, tab2.PGO
//   from 
//   (select distinct active_lead_SKU from Indirect_Active_SKU) tab1
//   cross join 
//   (select distinct Distr, PGO from Indirect_Active_PGO_Distr) tab2
//   ) files
  
//   on main.Lead_SKUID = files.Lead_SKUID and main.Distributor = files.Distributor and main.PGO = files.PGO
  
// left join 

// (
// select 
// s.Lead_SKUID,  replace(replace(concat(s.OP, s.Distributor, s.Sub_channel), '"',''), ' ', '') Distributor, s.PGO , sum(s.Volume_dal)/10 volume_hl
// from IndirectSales s left join Calendar cl on s.Date = cl.DayName
// where cl.MonthId in (201909,201908,201907)
// group by s.Lead_SKUID,  s.OP, s.Distributor, s.Sub_channel, s.PGO
// having (volume_hl > 0)
// ) summ
// on 
//  main.Lead_SKUID = summ.Lead_SKUID and main.Distributor = summ.Distributor and main.PGO = summ.PGO

// """

// COMMAND ----------

val sqldf = spark.sql("""
select max(Date) max_date from IndirectSales where Date is not null or Date <> "\\N"
""")
sqldf.createOrReplaceTempView("current_date_info")

// COMMAND ----------

val sqldf = spark.sql("""
select 
s.Lead_SKUID,  s.OP, s.Distributor, s.Sub_channel, s.PGO, s.Distr_Client, sum(s.Volume_dal)/10 volume_hl 
from IndirectSales s left join Calendar cl on s.Date = cl.DayName
where
cl.MonthId  >= year(date_add((select max_date from current_date_info),-60))*100 + month(date_add((select max_date from current_date_info),-60)) and
cl.MonthId  <= year((select max_date from current_date_info))*100 + month((select max_date from current_date_info))
group by s.Lead_SKUID,  s.OP, s.Distributor, s.Sub_channel, s.PGO, s.Distr_Client
having (volume_hl > 0)
""")
sqldf.createOrReplaceTempView("indirect_active_objects")


// COMMAND ----------

val sqldf = spark.sql(
"""
select distinct 
Lead_SKUID , OP, distributor, sub_channel, PGO, Distr_Client
from Indirect_active_objects 
"""
)
sqldf.createOrReplaceTempView("key_info")

// COMMAND ----------

val sqldf = spark.sql("""
select key_info.*, week.WeekId calendar_yearweek, week.MonthId calendar_yearmonth
from key_info cross join 
(select distinct WeekId, MonthId
from Calendar 
where DateKey >= year((select report_date from report_week_info))*10000 + month((select report_date from report_week_info))*100 + day((select report_date from report_week_info))
and WeekId <= (select distinct WeekId from till_week_info)
)week
 """)
sqldf.createOrReplaceTempView("key_info_with_week")                  

// COMMAND ----------

//Indirect_RU

// COMMAND ----------

val query = s"""

select *
from (
select  
int(cl.MonthId/100) as partition_name, 
replace (replace(s.Distr_Client, '  ', ' '), '"' , '') distr_client , 
--replace (concat(s.OP,' ', s.Distributor,' ',  s.Sub_channel ), '"' , '') distr_client,
replace (s.OP, '"' , '') OP,  
replace (s.Distributor, '"' , '') distributor, 
--replace (s.Client, '"' , '') client,
s.Sub_channel sub_channel, 
s.PGO , 
cast( if(left(s.Lead_SKUID, 1) = "=", replace(s.Lead_SKUID, left(s.Lead_SKUID, 1), ''), s.Lead_SKUID) as double) lead_sku  , 
cl.WeekId calendar_yearweek, 
cl.MonthId calendar_yearmonth, 
sum(s.Volume_dal)/10 volume_hl

from IndirectSales s 
left join Calendar cl on s.Date = cl.DayName 

where cl.WeekId >= 201601 
--and int(if(left(s.Lead_SKUID, 1) = "=", replace(s.Lead_SKUID, left(s.Lead_SKUID, 1), ''), s.Lead_SKUID)) in (select int(SKU_Lead_ID) from  Active_SKU_Only)

group by  s.Distr_Client,  s.PGO, s.Lead_SKUID, cl.WeekId, cl.MonthId, s.OP, s.Distributor, s.Sub_channel/*, s.Client*/ 
) tab

union 

select 
int(calendar_yearmonth/100) as partition_name, 
replace (replace(Distr_Client, '  ', ' '), '"' , '') distr_client , 
--replace (concat(OP,' ', distributor,' ',  sub_channel ), '"' , '') distr_client,
replace (OP, '"' , '') OP, 
replace (distributor, '"' , '') distributor, 
sub_channel,
PGO,
cast( if(left(Lead_SKUID, 1) = "=", replace(Lead_SKUID, left(Lead_SKUID, 1), ''), Lead_SKUID) as double) lead_sku  , 
calendar_yearweek,
calendar_yearmonth,
null volume_hl
from 
key_info_with_week

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

//result export

// COMMAND ----------

val sqldf = spark.sql(query)

// COMMAND ----------

def exportToBlobStorage (type_of_ETL:Int): String = { 

import com.databricks.WorkflowException
import java.io.FileNotFoundException

var Result = "Failure" 
val partition_field = "partition_name"
val export_format = "com.databricks.spark.csv"
val export_delimiter = ";"

var readPath_ETL = if (type_of_ETL == 0) readPath else if (type_of_ETL == 1) readPath_GBS else null
var writePath_ETL= if (type_of_ETL == 0) writePath else if (type_of_ETL == 1) writePath_GBS else null

try {
  sqldf
  .coalesce(1)
  .write.mode("overwrite")
  .format(export_format)
  .option("header", "true")
  .option("inferSchema", "true")
  .option("delimiter", export_delimiter)
  //.option("encoding", "cp1251")
  .partitionBy(partition_field)
  .save(readPath_ETL)

  val name : String = "part-00000"   
  val path_list : Seq[String] = dbutils.fs.ls(readPath_ETL).map(_.path).filter(_.contains(partition_field))

  for (path <- path_list) {
   var partition_name = path.replace(readPath_ETL + "/" + partition_field + "=", "").replace("/", "")
   var file_list : Seq[String] = dbutils.fs.ls(path).map(_.path).filter(_.contains(name)) 
   var read_name =  if (file_list.length >= 1 ) file_list(0).replace(path + "/", "") 
  // var fname = "Indirect_RU_cp1251_" + partition_name + ".csv" 
   var fname = "Indirect_RU_" + partition_name + ".csv"
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

//hole file 

// COMMAND ----------

def exportToBlobStorage_hole_file (type_of_ETL:Int): String = { 

import com.databricks.WorkflowException
import java.io.FileNotFoundException

var Result = "Failure" 
val partition_field = "partition_name"
val export_format = "com.databricks.spark.csv"
val export_delimiter = ";"

var readPath_ETL = if (type_of_ETL == 0) readPath else if (type_of_ETL == 1) readPath_GBS else null
var writePath_ETL= if (type_of_ETL == 0) writePath else if (type_of_ETL == 1) writePath_GBS else null

try {
  sqldf
  .coalesce(1)
  .write.mode("overwrite")
  .format(export_format)
  .option("header", "true")
  .option("inferSchema", "true")
  .option("delimiter", export_delimiter)
  //.option("encoding", "cp1251")
  .save(readPath_ETL)

  val name : String = "part-00000"   
  val file_list : Seq[String] = dbutils.fs.ls(readPath_ETL).map(_.path).filter(_.contains(name))
  val read_name = if (file_list.length >= 1 ) file_list(0).replace(readPath_ETL + "/", "")
  var fname = "Indirect_RU" +  ".csv" 
  //var fname = "Indirect_RU_cp1251" +  ".csv" 
  dbutils.fs.mv(readPath_ETL+"/"+ read_name , writePath_ETL+"/"+fname)     
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

// val Result = 
// if (type_of_ETL == 0) exportToBlobStorage(0) 
// else if (type_of_ETL == 1) exportToBlobStorage(1)
// else if (type_of_ETL == 2) {exportToBlobStorage(0); exportToBlobStorage(1) }
// else "Unexpected parameter"

// //dbutils.notebook.exit(Result)

// COMMAND ----------

val Result = 
if (type_of_ETL == 0) exportToBlobStorage_hole_file(0) 
else if (type_of_ETL == 1) exportToBlobStorage_hole_file(1)
else if (type_of_ETL == 2) {exportToBlobStorage_hole_file(0); exportToBlobStorage_hole_file(1) }
else "Unexpected parameter"

//dbutils.notebook.exit(Result)

// COMMAND ----------



// COMMAND ----------

// val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/Result/Indirect_RU.csv"
// val file_type = "csv"
// val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
// df.createOrReplaceTempView("Indirect_RU")

// COMMAND ----------

// %sql 
// select *
// from (
// select OP, distributor, sub_channel, distr_client, row_number() over (partition by OP, distributor, sub_channel order by distr_client) row_num
// from Indirect_RU 
// group by OP, distributor, sub_channel, distr_client
// ) tab
// where row_num > 1