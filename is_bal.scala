// Databricks notebook source
// MAGIC %run /Users/o_mazur@carlsberg.ua/dcd_etl_functions

// COMMAND ----------

//util variables

// COMMAND ----------

val fname = "Indirect_RU.csv"
val job = "dcd_notebook_workflow_indirect"
val notebook = "is_bal"
val notebook_start_time  = LocalDateTime.now
val readPath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/tmp/" + job
val readPath_GBS = "wasbs://dcd@prdcbwesa01.blob.core.windows.net/RU/ru_tmp/" +job

// COMMAND ----------

//Init log DB  

// COMMAND ----------

// MAGIC %sql use etl_info

// COMMAND ----------

//start logging

// COMMAND ----------

save_to_log_first_step(job,notebook)

// COMMAND ----------

//log for parameters 

// COMMAND ----------

save_to_log_parameters_value (job,notebook, "type_of_ETL", type_of_ETL)
save_to_log_parameters_value (job,notebook, "type_of_data_extract", type_of_data_extract)

// COMMAND ----------

//log for source files

// COMMAND ----------

//Calendar

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = true
val source_file_name = "Calendar.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Calendar")

save_to_log_file_processing(job,notebook,start_time, df, source_file_name )

//number of rows in the file > 1
if (df.count == 0) {
  save_to_log_if_source_is_empty(job,notebook,start_time, source_file_name)
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
  //throw new Exception("the source file is empty")
}

//if the source file is up todate

if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0  && is_regularly_updated_table ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, source_file_name )
  save_to_log_last_step(job,notebook,"Skipped",notebook_start_time)
  dbutils.notebook.exit("Skipped")  
  //throw new Exception("the source file is not up to date")
}

// COMMAND ----------

//InDirect_Product2 with BEL delimiter

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = true
val source_file_name = "InDirect_Product.csv"
val file_location = source_file_location + source_file_name
val export_delimiter = Character.toString(7.toChar)
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", export_delimiter).option("header", "true").load(file_location)
df.createOrReplaceTempView("IndirectSales")

save_to_log_file_processing(job,notebook,start_time, df, source_file_name )

//number of rows in the file > 1
if (df.count == 0) {
  save_to_log_if_source_is_empty(job,notebook,start_time, source_file_name)
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
  //throw new Exception("the source file is empty")
}

//if the source file is up todate

if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0  && is_regularly_updated_table ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, source_file_name )
  save_to_log_last_step(job,notebook,"Skipped",notebook_start_time)
  dbutils.notebook.exit("Skipped")  
  //throw new Exception("the source file is not up to date")
}

// COMMAND ----------

//numbers of week back from current date, if "0" - current week

// COMMAND ----------

// %sql select max(Date) max_date from IndirectSales where Date is not null or Date <> "\\N" ----2019-11-19T00:00:00.000+0000


// COMMAND ----------

// %sql 
// select 
// (select distinct Weekid from Calendar where DayName = cast(current_date() as timestamp) ) - 
// (select distinct Weekid from Calendar where DayName = (select max(Date) max_date from IndirectSales where Date is not null or Date <> "\\N" ) ) as numbers_of_weeks_back


// COMMAND ----------

val query_get_numbers_of_weeks_back = """
select 
int(
(select distinct Weekid from Calendar where DayName = cast(current_date() as timestamp) ) - 
(select distinct Weekid from Calendar where DayName = (select max(Date) max_date from IndirectSales where Date is not null or Date <> "\\N" ) ) ) as numbers_of_weeks_back
"""

// COMMAND ----------

val df_gnwb = spark.sql(query_get_numbers_of_weeks_back)

// COMMAND ----------

val numbers_of_weeks_back = df_gnwb.first.getInt(0)
//val res_df_gnwb_res = res_df_gnwb(0).toString().replace("[", "").replace("]", "").toInt

// COMMAND ----------

//val numbers_of_weeks_back: Int = 0

// COMMAND ----------

//numbers of week ahead from current date

// COMMAND ----------

val numbers_of_weeks_ahead: Int = 78

// COMMAND ----------

//numbers of week ahead from current date

// COMMAND ----------

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

val start_time = LocalDateTime.now

val is_regularly_updated_table = true
val source_file_name = "MD_SKU.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("MD_SKU")

save_to_log_file_processing(job,notebook,start_time, df, source_file_name )

//number of rows in the file > 1
if (df.count == 0) {
  save_to_log_if_source_is_empty(job,notebook,start_time, source_file_name)
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
  //throw new Exception("the source file is empty")
}

//if the source file is up todate

if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0  && is_regularly_updated_table ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, source_file_name )
  save_to_log_last_step(job,notebook,"Skipped",notebook_start_time)
  dbutils.notebook.exit("Skipped")  
  //throw new Exception("the source file is not up to date")
}

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

val query_full = s"""

select tab.* , load_week_info.load_week
from (
select * , '' as client
from (
select  
int(cl.MonthId/100) as partition_name, 
s.Distr_Client as distr_client,
--replace (replace(s.Distr_Client, '  ', ' '), '"' , '') distr_client , 
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
Distr_Client as distr_client,
--replace (replace(Distr_Client, '  ', ' '), '"' , '') distr_client , 
--replace (concat(OP,' ', distributor,' ',  sub_channel ), '"' , '') distr_client,
replace (OP, '"' , '') OP, 
replace (distributor, '"' , '') distributor, 
sub_channel,
PGO,
cast( if(left(Lead_SKUID, 1) = "=", replace(Lead_SKUID, left(Lead_SKUID, 1), ''), Lead_SKUID) as double) lead_sku  , 
calendar_yearweek,
calendar_yearmonth,
null volume_hl, 
'' as client
from 
key_info_with_week

) tab 
cross join 
(select max(WeekId) load_week  from Calendar where WeekId  <= ( select distinct WeekId from Calendar where DateKey =  year((select report_date from report_week_info))*10000 + month((select report_date from report_week_info))*100 + day((select report_date from report_week_info)) )
) as load_week_info

""" 

// COMMAND ----------

val sqldf_full = spark.sql(query_full)

// COMMAND ----------

val query_incremental = s"""

select tab.* , '' as client, load_week_info.load_week
from (
select *
from (
select  
cl.MonthId as partition_name, 
s.Distr_Client as distr_client,
--replace (replace(s.Distr_Client, '  ', ' '), '"' , '') distr_client , 
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
calendar_yearmonth as partition_name, 
Distr_Client as distr_client,
--replace (replace(Distr_Client, '  ', ' '), '"' , '') distr_client , 
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
) tab
cross join 
(select max(WeekId) load_week  from Calendar where WeekId  <= ( select distinct WeekId from Calendar where DateKey =  year((select report_date from report_week_info))*10000 + month((select report_date from report_week_info))*100 + day((select report_date from report_week_info)) )
) as load_week_info
""" +
{if (type_of_data_extract == 1) 
 s""" 
 where calendar_yearmonth >=  year(date_add(current_date(),-1 * $num_of_days_before_current_date))*100 + month(date_add(current_date(),-1 * $num_of_days_before_current_date))
  
 """ 
 else ""}

// COMMAND ----------

val sqldf_incremental = spark.sql(query_incremental)

// COMMAND ----------

//Final testing before result export

// COMMAND ----------

exportToBlobStorage_Baltika_Result_Before_Testing(job,notebook,fname, sqldf_full, readPath)

// COMMAND ----------

val sqldf_result = load_preliminary_result(fname)

// COMMAND ----------

save_to_log_start_result_testing(job,notebook)

// COMMAND ----------

if (Test_Number_of_Rows(job,notebook, sqldf_result, fname) == "FAILED") {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")    
}

// COMMAND ----------

if (Test_Is_incomplete_period(job,notebook, sqldf_result, fname) == "FAILED") {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")    
}

// COMMAND ----------

if (Test_Missing_Filed(job, notebook, sqldf_result, "lead_sku", fname ) == "FAILED" ) {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
}

// COMMAND ----------

if (Test_Missing_Filed(job, notebook, sqldf_result, "distr_client", fname ) == "FAILED" ) {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
}

// COMMAND ----------

if (Test_Missing_Filed(job, notebook, sqldf_result, "PGO" , fname) == "FAILED" ) {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
}

// COMMAND ----------

Test_Values_In_Correct_Range(job, notebook, sqldf_result, "monthly", "volume_hl", 353840, 1786466, fname)
  

// COMMAND ----------

//end Final testing before result export

// COMMAND ----------

val Result = 
if (type_of_ETL == 0) { 
  if (exportToBlobStorage_Baltika (job,notebook,fname, sqldf_full, readPath ) == "Success" &&      
      exportToBlobStorage_CAP (job,notebook,0, sqldf_incremental, "HFAINDIRECT_" , readPath, readPath_GBS) == "Success" 
      )  "Success" else "Failure"  }
else if (type_of_ETL == 1) { 
  if (exportToBlobStorage_CAP (job,notebook,1, sqldf_incremental, "HFAINDIRECT_" , readPath, readPath_GBS) == "Success" 
     ) "Success" else  "Failure"  }
else if (type_of_ETL == 2) { 
  if (exportToBlobStorage_Baltika (job,notebook,fname, sqldf_full, readPath ) == "Success" &&
      exportToBlobStorage_CAP (job,notebook,1, sqldf_incremental, "HFAINDIRECT_" , readPath, readPath_GBS) == "Success" 
     ) "Success" else "Failure" }
else "Unexpected parameter"


//save to log
save_to_log_last_step(job,notebook,Result,notebook_start_time)

dbutils.notebook.exit(Result)

// COMMAND ----------



// COMMAND ----------

//   val source_file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/Result/"
//   val source_file_name = "Indirect_RU.csv"
//   val file_location = source_file_location + source_file_name
//   val export_delimiter = ";"
//   val file_type = "csv"
//   val sqldf = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", export_delimiter).option("header", "true").load(file_location)
// sqldf.createOrReplaceTempView("Indirect_RU")

// COMMAND ----------

//   val source_file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/"
//   val source_file_name = "InDirect_Product.csv"
//   val file_location = source_file_location + source_file_name
//   val export_delimiter = Character.toString(7.toChar)
//   val file_type = "csv"
//   val sqldf = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", export_delimiter).option("header", "true").load(file_location)
// sqldf.createOrReplaceTempView("InDirect_Product")

// COMMAND ----------

// val source_file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/"
// val source_file_name = "Calendar.csv"
// val file_location = source_file_location + source_file_name
// val file_type = "csv"
// val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
// df.createOrReplaceTempView("Calendar")

// COMMAND ----------

// %sql
// select calendar_yearweek, sum(volume_hl)
// from Indirect_RU
// group by calendar_yearweek

// COMMAND ----------

// %sql
// select  sum(volume_hl)
// from Indirect_RU
// --where calendar_yearweek = 201950


// COMMAND ----------

// %sql
// select sum(Volume_dal)/10
// from InDirect_Product p left join Calendar cl on p.date = cl.dayname
// where cl.weekid >= 201601