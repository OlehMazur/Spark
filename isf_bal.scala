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

//IndDirect, InDirect_Product

// COMMAND ----------

val export_delimiter = Character.toString(7.toChar)
val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/InDirect_Product.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", export_delimiter).option("header", "true").load(file_location)
df.createOrReplaceTempView("IndirectSales")

// COMMAND ----------

val export_delimiter = Character.toString(7.toChar)
val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/InDirect_Product2.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", export_delimiter).option("header", "true").load(file_location)
df.createOrReplaceTempView("IndirectSales2")

// COMMAND ----------

val file_location = writePath + "/Template.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("template")

// COMMAND ----------

// MAGIC %sql select * from template where pgo = '*5515101'

// COMMAND ----------

// MAGIC %sql 
// MAGIC select t.PGO pgo_template , s.PGO pgo_file, t.Volume Volume_dal_Template ,  sum(s.Volume_dal) Volume_dal_Indirect_File , int(t.Volume  -  sum(s.Volume_dal)) diff
// MAGIC from template t inner join  InDirect_Product s on t.PGO = s.PGO
// MAGIC --where t.PGO = '*5515101'
// MAGIC group by t.PGO , t.Volume, s.PGO

// COMMAND ----------

//Indirect_Forecast.csv

// COMMAND ----------

val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/Indirect_Forecast.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Indirect_Forecast")

// COMMAND ----------

// MAGIC  %sql select * from Indirect_Forecast where `Code of Active address` = '*5515101' 

// COMMAND ----------

// MAGIC  %sql select max(cast (date as date)) from IndirectSales 

// COMMAND ----------

// MAGIC %sql select max(cast (date as date)) from Indirect_Forecast limit 1

// COMMAND ----------

// MAGIC %sql 
// MAGIC select distinct cl.MonthId
// MAGIC from IndirectSales s left join Calendar cl on s.Date = cl.DayName 
// MAGIC 
// MAGIC where Lead_SKUID in
// MAGIC (
// MAGIC 29270014,
// MAGIC 51070014,
// MAGIC 38070014,
// MAGIC 51070016,
// MAGIC 38070016,
// MAGIC 29270016
// MAGIC )

// COMMAND ----------

// MAGIC %sql 
// MAGIC select * from IndirectSales where Lead_SKUID = 1.35 limit 1

// COMMAND ----------

val file_location = writePath  +"/" +  "Indirect_RU.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Indirect_RU")

// COMMAND ----------

// MAGIC %sql 
// MAGIC select distinct lead_sku  from Indirect_RU where lead_sku in
// MAGIC (
// MAGIC 29270014,
// MAGIC 51070014,
// MAGIC 38070014,
// MAGIC 51070016,
// MAGIC 38070016,
// MAGIC 29270016
// MAGIC )

// COMMAND ----------

// MAGIC %sql 
// MAGIC select * from Indirect_RU where lead_sku = 384110 limit 1

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct lead_sku
// MAGIC from Indirect_RU 
// MAGIC where lead_sku not in (select distinct Lead_SKUID from IndirectSales where Lead_SKUID is not null )

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct Lead_SKUID 
// MAGIC from IndirectSales 
// MAGIC where Lead_SKUID not in (select distinct lead_sku from Indirect_RU where lead_sku is not null )

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct lead_sku
// MAGIC from (
// MAGIC select  
// MAGIC int(cl.MonthId/100) as partition_name, 
// MAGIC replace (s.Distr_Client, '"' , '') distr_client , 
// MAGIC replace (s.OP, '"' , '') OP,  
// MAGIC replace (s.Distributor, '"' , '') distributor, 
// MAGIC replace (s.Client, '"' , '') client,
// MAGIC s.Sub_channel sub_channel, 
// MAGIC s.PGO , 
// MAGIC cast(if(left(s.Lead_SKUID, 1) = "=", replace(s.Lead_SKUID, left(s.Lead_SKUID, 1), ''), s.Lead_SKUID) as double) lead_sku , 
// MAGIC cl.WeekId calendar_yearweek, 
// MAGIC cl.MonthId calendar_yearmonth, 
// MAGIC sum(s.Volume_dal)/10 volume_hl,
// MAGIC s.Lead_SKUID
// MAGIC 
// MAGIC from IndirectSales s 
// MAGIC left join Calendar cl on s.Date = cl.DayName 
// MAGIC 
// MAGIC where cl.WeekId >= 201601 
// MAGIC --and int(if(left(s.Lead_SKUID, 1) = "=", replace(s.Lead_SKUID, left(s.Lead_SKUID, 1), ''), s.Lead_SKUID)) in (select int(SKU_Lead_ID) from  Active_SKU_Only)
// MAGIC 
// MAGIC group by  s.Distr_Client,  s.PGO, s.Lead_SKUID, cl.WeekId, cl.MonthId, s.OP, s.Distributor, s.Sub_channel, s.Client 
// MAGIC ) tab
// MAGIC where tab.lead_sku in 
// MAGIC (
// MAGIC 29270014,
// MAGIC 51070014,
// MAGIC 38070014,
// MAGIC 51070016,
// MAGIC 38070016,
// MAGIC 29270016
// MAGIC 
// MAGIC )
// MAGIC --tab.Lead_SKUID = '2 9 270 014'

// COMMAND ----------

// MAGIC %sql 
// MAGIC select cast('29270014' as int)

// COMMAND ----------

// MAGIC %sql select distinct PGO  from IndirectSales where PGO like '*5515101' 

// COMMAND ----------

// MAGIC %sql select distinct PGO  from Indirect_RU where PGO = '*5515101' 

// COMMAND ----------

// MAGIC %sql 
// MAGIC select sum(Volume_dal) volume_dal , sum(Volume_dal/10) volume_hl
// MAGIC from IndirectSales

// COMMAND ----------

// MAGIC %sql 
// MAGIC select sum(volume_hl)
// MAGIC from Indirect_RU

// COMMAND ----------

// MAGIC %sql
// MAGIC select sum(s.Volume_dal/10) volume_hl
// MAGIC from InDirect_Product s left join Calendar cl on s.Date = cl.DayName 
// MAGIC where cl.WeekId >= 201601 

// COMMAND ----------

// MAGIC %sql select * from Indirect_Forecast where `Code of Active address` = '*55104801' limit 1

// COMMAND ----------

// MAGIC %sql select count (distinct Lead_SKUID ) from IndirectSales --InDirect_Product

// COMMAND ----------

// MAGIC %sql select distinct Distr_Client  from IndirectSales 

// COMMAND ----------

// MAGIC %sql 
// MAGIC select distinct SKU_ID, PGO, Distributor from IndirectSales
// MAGIC union 
// MAGIC select distinct SKU_ID, 'Code of Active address' PGO, Distr Distributor  from Indirect_Forecast Distributor

// COMMAND ----------

/*

int(cl.MonthId/100) as partition_name, 
replace (s.Distr_Client, '"' , '') distr_client , 
replace (s.OP, '"' , '') OP,  
replace (s.Distributor, '"' , '') distributor, 
replace (s.Client, '"' , '') client,
s.Sub_channel sub_channel, 
s.PGO , 
cast( if(left(s.Lead_SKUID, 1) = "=", replace(s.Lead_SKUID, left(s.Lead_SKUID, 1), ''), s.Lead_SKUID) as int) lead_sku  , 
cl.WeekId calendar_yearweek, 
cl.MonthId calendar_yearmonth, 
sum(s.Volume_dal)/10 volume_hl
*/

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

//Indirect_RU

// COMMAND ----------

val query = s"""

select *
from (
select  
int(cl.MonthId/100) as partition_name, 
replace (s.Distr_Client, '"' , '') distr_client , 
replace (s.OP, '"' , '') OP,  
replace (s.Distributor, '"' , '') distributor, 
replace (s.Client, '"' , '') client,
s.Sub_channel sub_channel, 
s.PGO , 
cast( if(left(s.Lead_SKUID, 1) = "=", replace(s.Lead_SKUID, left(s.Lead_SKUID, 1), ''), s.Lead_SKUID) as int) lead_sku  , 
cl.WeekId calendar_yearweek, 
cl.MonthId calendar_yearmonth, 
sum(s.Volume_dal)/10 volume_hl

from IndirectSales s 
left join Calendar cl on s.Date = cl.DayName 

where cl.WeekId >= 201601 
--and int(if(left(s.Lead_SKUID, 1) = "=", replace(s.Lead_SKUID, left(s.Lead_SKUID, 1), ''), s.Lead_SKUID)) in (select int(SKU_Lead_ID) from  Active_SKU_Only)

group by  s.Distr_Client,  s.PGO, s.Lead_SKUID, cl.WeekId, cl.MonthId, s.OP, s.Distributor, s.Sub_channel, s.Client 
) tab

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
  .option("encoding", "cp1251")
  .partitionBy(partition_field)
  .save(readPath_ETL)

  val name : String = "part-00000"   
  val path_list : Seq[String] = dbutils.fs.ls(readPath_ETL).map(_.path).filter(_.contains(partition_field))

  for (path <- path_list) {
   var partition_name = path.replace(readPath_ETL + "/" + partition_field + "=", "").replace("/", "")
   var file_list : Seq[String] = dbutils.fs.ls(path).map(_.path).filter(_.contains(name)) 
   var read_name =  if (file_list.length >= 1 ) file_list(0).replace(path + "/", "") 
   var fname = "Indirect_RU_cp1251_" + partition_name + ".csv" 
   //var fname = "Indirect_RU_" + partition_name + ".csv"
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
  .option("encoding", "cp1251")
  .save(readPath_ETL)

  val name : String = "part-00000"   
  val file_list : Seq[String] = dbutils.fs.ls(readPath_ETL).map(_.path).filter(_.contains(name))
  val read_name = if (file_list.length >= 1 ) file_list(0).replace(readPath_ETL + "/", "")
  //var fname = "Indirect_RU" +  ".csv" 
  var fname = "Indirect_RU_cp1251" +  ".csv" 
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

val Result = 
if (type_of_ETL == 0) exportToBlobStorage(0) 
else if (type_of_ETL == 1) exportToBlobStorage(1)
else if (type_of_ETL == 2) {exportToBlobStorage(0); exportToBlobStorage(1) }
else "Unexpected parameter"

//dbutils.notebook.exit(Result)

// COMMAND ----------

val Result = 
if (type_of_ETL == 0) exportToBlobStorage_hole_file(0) 
else if (type_of_ETL == 1) exportToBlobStorage_hole_file(1)
else if (type_of_ETL == 2) {exportToBlobStorage_hole_file(0); exportToBlobStorage_hole_file(1) }
else "Unexpected parameter"

//dbutils.notebook.exit(Result)

// COMMAND ----------

val file_location = writePath  +"/" +  "fc_indirect_weekly.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("fc_indirect_weekly")

// COMMAND ----------

val file_location = writePath  +"/" +  "fc_indirect_monthly.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("fc_indirect_monthly")

// COMMAND ----------

val file_location = writePath  +"/" +  "Indirect_RU.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Indirect_RU")

// COMMAND ----------

// MAGIC %sql 
// MAGIC select count(distinct lead_sku ) from Indirect_RU

// COMMAND ----------

// MAGIC %sql select * from Indirect_RU where lead_sku =  119100

// COMMAND ----------

// MAGIC %sql select * from IndirectSales where sku_id =  119100 --lead_sku =  119100

// COMMAND ----------

// MAGIC %sql select * from fc_indirect_weekly where lead_sku = 242850 and plant = '*800058'

// COMMAND ----------

// MAGIC %sql select count(*) from (select distinct * from fc_indirect_weekly) --where lead_sku = 242850 --and plant = '*800058' --3350943

// COMMAND ----------

// MAGIC %sql select plant, sum (actual_sales_volume) from fc_indirect_monthly where lead_sku = 242850 and plant = '*800058' group by  plant

// COMMAND ----------

// MAGIC %sql select plant, sum (actual_sales_volume) from fc_indirect_weekly where lead_sku = 242850 and plant = '*800058' group by  plant

// COMMAND ----------

// MAGIC %sql select PGO, sum (volume_hl) from Indirect_RU where lead_sku = 242850 and PGO = '*800058' group by  PGO

// COMMAND ----------

// MAGIC %sql select distinct AD_ID, PGO, sum(volume_dal) from IndirectSales where PGO = '*800058' and Lead_SKUID = 242850 group by AD_ID, PGO

// COMMAND ----------

// MAGIC %sql 
// MAGIC select * from Indirect_Forecast limit 1 --where SKU_ID = '242850' and 'Code of Active address' = '*800058'

// COMMAND ----------



// COMMAND ----------

val file_location = writePath  +"/" +  "Indirect_Active_SKU.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Indirect_Active_SKU")

// COMMAND ----------

val file_location = writePath  +"/" +  "Indirect_Active_PGO_Distr.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Indirect_Active_PGO_Distr")

// COMMAND ----------

// MAGIC %sql select * from Indirect_Active_SKU limit 1

// COMMAND ----------

// MAGIC %sql select * from Indirect_Active_PGO_Distr limit 1

// COMMAND ----------

// MAGIC %sql 
// MAGIC select 
// MAGIC --pgo, Distributor, sum(Volume_dal/10) 
// MAGIC /*cl.MonthId,*/ pgo,  OP, Distributor, Sub_channel, sum(Volume_dal)/10 
// MAGIC from IndirectSales s left join  Calendar cl on s.Date = cl.DayName 
// MAGIC where pgo = '*55136800' 
// MAGIC and Distributor = 'ООО "БИРТАЙМ"' 
// MAGIC and MonthId in (201907,201908,201909)
// MAGIC and OP = 'СБП off-trade Барнаул' 
// MAGIC and Sub_channel = 'BB OT keg'
// MAGIC group by /*cl.MonthId ,*/ pgo,  OP, Distributor, Sub_channel
// MAGIC --group by Distributor, pgo
// MAGIC --order by MonthId desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct replace(Distr, '"', '')
// MAGIC from 
// MAGIC (
// MAGIC   (select distinct active_lead_SKU from Indirect_Active_SKU) tab1
// MAGIC   cross join 
// MAGIC   (select distinct Distr, PGO from Indirect_Active_PGO_Distr) tab2
// MAGIC ) tab
// MAGIC where PGO = '*55136800'

// COMMAND ----------

// MAGIC %sql select distinct replace(concat(s.OP,' ', s.Distributor, ' ', s.Sub_channel), '"','') from IndirectSales s where pgo = '*55136800'

// COMMAND ----------

// MAGIC %sql select * from IndirectSales limit 1

// COMMAND ----------

// MAGIC %sql 
// MAGIC select PGO, Distributor, sum(Vol_3m_hl)
// MAGIC from (
// MAGIC select main.* , if (files.Lead_SKUID is not null and files.Distributor is not null  and files.PGO is not null , 1, 0 ) Active_files,
// MAGIC if (summ.Lead_SKUID is not null and summ.Distributor is not null  and summ.PGO is not null , 1, 0 ) Active_sales, 
// MAGIC summ.volume_hl  Vol_3m_hl 
// MAGIC 
// MAGIC from (
// MAGIC select distinct s.Lead_SKUID,  s.Distributor, s.PGO from IndirectSales s
// MAGIC 
// MAGIC union 
// MAGIC   (
// MAGIC   select tab1.active_lead_SKU Lead_SKUID, tab2.Distr Distributor, tab2.PGO
// MAGIC   from 
// MAGIC   (select distinct active_lead_SKU from Indirect_Active_SKU) tab1
// MAGIC   cross join 
// MAGIC   (select distinct Distr, PGO from Indirect_Active_PGO_Distr) tab2
// MAGIC   )
// MAGIC ) main
// MAGIC 
// MAGIC left join 
// MAGIC 
// MAGIC  (
// MAGIC   select tab1.active_lead_SKU Lead_SKUID, tab2.Distr Distributor, tab2.PGO
// MAGIC   from 
// MAGIC   (select distinct active_lead_SKU from Indirect_Active_SKU) tab1
// MAGIC   cross join 
// MAGIC   (select distinct Distr, PGO from Indirect_Active_PGO_Distr) tab2
// MAGIC   ) files
// MAGIC   
// MAGIC   on main.Lead_SKUID = files.Lead_SKUID and main.Distributor = files.Distributor and main.PGO = files.PGO
// MAGIC   
// MAGIC left join 
// MAGIC 
// MAGIC (
// MAGIC select 
// MAGIC s.Lead_SKUID,  s.Distributor, s.PGO , sum(s.Volume_dal)/10 volume_hl
// MAGIC from IndirectSales s left join Calendar cl on s.Date = cl.DayName
// MAGIC where cl.MonthId in (201909,201908,201907)
// MAGIC group by s.Lead_SKUID,  s.Distributor, s.PGO
// MAGIC having (volume_hl > 0)
// MAGIC ) summ
// MAGIC on 
// MAGIC  main.Lead_SKUID = summ.Lead_SKUID and main.Distributor = summ.Distributor and main.PGO = summ.PGO
// MAGIC 
// MAGIC where 
// MAGIC --main.Lead_SKUID = 100 and 
// MAGIC main.PGO = '*55136800' --and main.Distributor = "СБП off-trade Барнаул  ООО ""БИРТАЙМ"" BB OT keg"
// MAGIC and 
// MAGIC main.Distributor = 'ООО "БИРТАЙМ"' --and 
// MAGIC ) tab
// MAGIC group by PGO, Distributor

// COMMAND ----------

//v2

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC select main.* , if (files.Lead_SKUID is not null and files.Distributor is not null  and files.PGO is not null , 1, 0 ) Active_files,
// MAGIC if (summ.Lead_SKUID is not null and summ.Distributor is not null  and summ.PGO is not null , 1, 0 ) Active_sales, 
// MAGIC summ.volume_hl  Vol_3m_hl 
// MAGIC 
// MAGIC from (
// MAGIC select distinct s.Lead_SKUID, replace(replace(concat(s.OP, s.Distributor, s.Sub_channel), '"',''), ' ', '') Distributor , s.PGO from IndirectSales s
// MAGIC 
// MAGIC union 
// MAGIC   (
// MAGIC   select tab1.active_lead_SKU Lead_SKUID, replace(replace(tab2.Distr, '"', ''), ' ', '' ) Distributor, tab2.PGO
// MAGIC   from 
// MAGIC   (select distinct active_lead_SKU from Indirect_Active_SKU) tab1
// MAGIC   cross join 
// MAGIC   (select distinct Distr, PGO from Indirect_Active_PGO_Distr) tab2
// MAGIC   )
// MAGIC ) main
// MAGIC 
// MAGIC left join 
// MAGIC 
// MAGIC  (
// MAGIC   select tab1.active_lead_SKU Lead_SKUID, replace(replace(tab2.Distr, '"', ''), ' ', '' )  Distributor, tab2.PGO
// MAGIC   from 
// MAGIC   (select distinct active_lead_SKU from Indirect_Active_SKU) tab1
// MAGIC   cross join 
// MAGIC   (select distinct Distr, PGO from Indirect_Active_PGO_Distr) tab2
// MAGIC   ) files
// MAGIC   
// MAGIC   on main.Lead_SKUID = files.Lead_SKUID and main.Distributor = files.Distributor and main.PGO = files.PGO
// MAGIC   
// MAGIC left join 
// MAGIC 
// MAGIC (
// MAGIC select 
// MAGIC s.Lead_SKUID,  replace(replace(concat(s.OP, s.Distributor, s.Sub_channel), '"',''), ' ', '') Distributor, s.PGO , sum(s.Volume_dal)/10 volume_hl
// MAGIC from IndirectSales s left join Calendar cl on s.Date = cl.DayName
// MAGIC where cl.MonthId in (201909,201908,201907)
// MAGIC group by s.Lead_SKUID,  s.OP, s.Distributor, s.Sub_channel, s.PGO
// MAGIC having (volume_hl > 0)
// MAGIC ) summ
// MAGIC on 
// MAGIC  main.Lead_SKUID = summ.Lead_SKUID and main.Distributor = summ.Distributor and main.PGO = summ.PGO
// MAGIC 
// MAGIC where 
// MAGIC --main.Lead_SKUID = 100 and 
// MAGIC main.PGO = '*55136800' and main.Distributor = 'СБПoff-tradeБарнаулОООБИРТАЙМBBOTkeg'
// MAGIC --and 
// MAGIC --main.Distributor = 'ООО "БИРТАЙМ"' --and 

// COMMAND ----------

// val query = s"""

// select main.* , if (files.Lead_SKUID is not null and files.Distributor is not null  and files.PGO is not null , 1, 0 ) Active_files,
// if (summ.Lead_SKUID is not null and summ.Distributor is not null  and summ.PGO is not null , 1, 0 ) Active_sales, 
// summ.volume_hl  Vol_3m_hl

// from (
// select distinct s.Lead_SKUID,  s.Distributor, s.PGO from IndirectSales s

// union 
//   (
//   select tab1.active_lead_SKU Lead_SKUID, tab2.Distr Distributor, tab2.PGO
//   from 
//   (select distinct active_lead_SKU from Indirect_Active_SKU) tab1
//   cross join 
//   (select distinct Distr, PGO from Indirect_Active_PGO_Distr) tab2
//   )
// ) main

// left join 

//  (
//   select tab1.active_lead_SKU Lead_SKUID, tab2.Distr Distributor, tab2.PGO
//   from 
//   (select distinct active_lead_SKU from Indirect_Active_SKU) tab1
//   cross join 
//   (select distinct Distr, PGO from Indirect_Active_PGO_Distr) tab2
//   ) files
  
//   on main.Lead_SKUID = files.Lead_SKUID and main.Distributor = files.Distributor and main.PGO = files.PGO
  
// left join 

// (
// select 
// s.Lead_SKUID,  s.Distributor, s.PGO , sum(s.Volume_dal)/10 volume_hl
// from IndirectSales s left join Calendar cl on s.Date = cl.DayName
// where cl.MonthId in (201909,201908,201907)
// group by s.Lead_SKUID,  s.Distributor, s.PGO
// having (volume_hl > 0)
// ) summ
// on 
//  main.Lead_SKUID = summ.Lead_SKUID and main.Distributor = summ.Distributor and main.PGO = summ.PGO

// """



// COMMAND ----------

val query = s"""
select main.* , if (files.Lead_SKUID is not null and files.Distributor is not null  and files.PGO is not null , 1, 0 ) Active_files,
if (summ.Lead_SKUID is not null and summ.Distributor is not null  and summ.PGO is not null , 1, 0 ) Active_sales, 
summ.volume_hl  Vol_3m_hl 

from (
select distinct s.Lead_SKUID, replace(replace(concat(s.OP, s.Distributor, s.Sub_channel), '"',''), ' ', '') Distributor , s.PGO from IndirectSales s

union 
  (
  select tab1.active_lead_SKU Lead_SKUID, replace(replace(tab2.Distr, '"', ''), ' ', '' ) Distributor, tab2.PGO
  from 
  (select distinct active_lead_SKU from Indirect_Active_SKU) tab1
  cross join 
  (select distinct Distr, PGO from Indirect_Active_PGO_Distr) tab2
  )
) main

left join 

 (
  select tab1.active_lead_SKU Lead_SKUID, replace(replace(tab2.Distr, '"', ''), ' ', '' )  Distributor, tab2.PGO
  from 
  (select distinct active_lead_SKU from Indirect_Active_SKU) tab1
  cross join 
  (select distinct Distr, PGO from Indirect_Active_PGO_Distr) tab2
  ) files
  
  on main.Lead_SKUID = files.Lead_SKUID and main.Distributor = files.Distributor and main.PGO = files.PGO
  
left join 

(
select 
s.Lead_SKUID,  replace(replace(concat(s.OP, s.Distributor, s.Sub_channel), '"',''), ' ', '') Distributor, s.PGO , sum(s.Volume_dal)/10 volume_hl
from IndirectSales s left join Calendar cl on s.Date = cl.DayName
where cl.MonthId in (201909,201908,201907)
group by s.Lead_SKUID,  s.OP, s.Distributor, s.Sub_channel, s.PGO
having (volume_hl > 0)
) summ
on 
 main.Lead_SKUID = summ.Lead_SKUID and main.Distributor = summ.Distributor and main.PGO = summ.PGO

"""

// COMMAND ----------

val sqldf = spark.sql(query)

// COMMAND ----------

val fname = "indirect_test_table_cp1251.csv" 
sqldf.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").option("encoding", "cp1251").save(readPath)

val name : String = "part-00000"  
val file_list : Seq[String] = dbutils.fs.ls(readPath).map(_.path).filter(_.contains(name))
val read_name = if (file_list.length >= 1 ) file_list(0).replace(readPath + "/", "")
val row_count = spark.read.format("csv").option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_list(0)).count   
dbutils.fs.mv(readPath+"/"+read_name , writePath+"/"+fname)   
dbutils.fs.rm(readPath , recurse = true)

// COMMAND ----------

val fname = "indirect_test_table.csv" 
sqldf.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").save(readPath)

val name : String = "part-00000"  
val file_list : Seq[String] = dbutils.fs.ls(readPath).map(_.path).filter(_.contains(name))
val read_name = if (file_list.length >= 1 ) file_list(0).replace(readPath + "/", "")
val row_count = spark.read.format("csv").option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_list(0)).count   
dbutils.fs.mv(readPath+"/"+read_name , writePath+"/"+fname)   
dbutils.fs.rm(readPath , recurse = true)

// COMMAND ----------

val file_location = writePath  +"/" +  "indirect_test_table.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("indirect_test_table")

// COMMAND ----------

// MAGIC %sql
// MAGIC select PGO, Distributor, sum(Vol_3m_hl) 
// MAGIC from indirect_test_table 
// MAGIC where PGO = '*55136800' and Distributor = 'СБПoff-tradeБарнаулОООБИРТАЙМBBOTkeg'
// MAGIC group by PGO, Distributor

// COMMAND ----------

// MAGIC %sql select * from indirect_test_table limit 1

// COMMAND ----------

// MAGIC %sql select * from indirect_test_table where Lead_SKUID = 378120

// COMMAND ----------

// MAGIC %sql 
// MAGIC select * from IndirectSales where Lead_SKUID = 678220