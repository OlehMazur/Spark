// Databricks notebook source
// MAGIC %run /Users/o_mazur@carlsberg.ua/dcd_etl_functions

// COMMAND ----------

//util variables

// COMMAND ----------

val fc_indirect_weekly = "fc_indirect_weekly.csv" 
val fc_indirect_monthly = "fc_indirect_monthly.csv"
val job = "dcd_notebook_workflow_indirect"
val notebook = "if_bal"
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

//Indirect_Forecast.csv

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = true
val source_file_name = "Indirect_Forecast.csv"
val file_location = source_file_location + source_file_name
val export_delimiter = Character.toString(7.toChar)
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", export_delimiter).option("header", "true").load(file_location)
df.createOrReplaceTempView("Indirect_Forecast")

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

// %sql select max(cast(date as date)) from Indirect_Forecast limit 1

// COMMAND ----------

//FC_KPI

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = false
val source_file_name = "FC_KPI.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("FC_KPI")

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

//some tables are not updating on a regular base
if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0 && !is_regularly_updated_table  ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, source_file_name )
}


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

//IndDirect, InDirect_Product

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
year(cast (fc.`Period of Plan` as timestamp) )*10000 + month(cast (fc.`Period of Plan` as timestamp))*100 + day(cast (fc.`Period of Plan` as timestamp)) = int(right(kpi.Version,4))* 10000 + int(right(replace(kpi.Version, right(kpi.Version,5), ''),2)) *100 + int(left(kpi.Version,2)) and
year(fc.Date)*10000 + month(fc.Date)*100 + day(fc.Date) = int(right(kpi.Week_Forecast,4))* 10000 + int(right(replace(kpi.Week_Forecast, right(kpi.Week_Forecast,5), ''),2)) *100 + int(left(kpi.Week_Forecast,2))
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

//Final testing before result export

// COMMAND ----------

exportToBlobStorage_Baltika_Result_Before_Testing(job,notebook,fc_indirect_weekly, sqldf_full_week, readPath)

// COMMAND ----------

exportToBlobStorage_Baltika_Result_Before_Testing(job,notebook,fc_indirect_monthly, sqldf_full_month, readPath)

// COMMAND ----------

val sqldf_result_week = load_preliminary_result(fc_indirect_weekly)

// COMMAND ----------

val sqldf_result_month = load_preliminary_result(fc_indirect_monthly)

// COMMAND ----------

save_to_log_start_result_testing(job,notebook)

// COMMAND ----------

if (Test_Number_of_Rows(job,notebook, sqldf_result_week, fc_indirect_weekly) == "FAILED") {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")    
}

// COMMAND ----------

if (Test_Number_of_Rows(job,notebook, sqldf_result_month, fc_indirect_monthly) == "FAILED") {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")    
}

// COMMAND ----------

if (Test_Is_incomplete_period(job,notebook, sqldf_result_week, fc_indirect_weekly) == "FAILED") {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")    
}

// COMMAND ----------

if (Test_Is_incomplete_period(job,notebook, sqldf_result_month, fc_indirect_monthly) == "FAILED") {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")    
}

// COMMAND ----------

if (Test_Missing_Filed(job, notebook, sqldf_result_week, "lead_sku", fc_indirect_weekly ) == "FAILED" ) {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
}

// COMMAND ----------

if (Test_Missing_Filed(job, notebook, sqldf_result_month, "lead_sku", fc_indirect_monthly ) == "FAILED" ) {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
}

// COMMAND ----------

//end Final testing before result export

// COMMAND ----------

val Result = 
if (type_of_ETL == 0) { 
  if (exportToBlobStorage_Baltika (job,notebook,fc_indirect_weekly, sqldf_full_week, readPath ) == "Success" &&  
      exportToBlobStorage_Baltika (job,notebook,fc_indirect_monthly, sqldf_full_month, readPath ) == "Success"  &&    
      exportToBlobStorage_CAP (job,notebook,0, sqldf_incremental_week, "FCACCWEEKINDIRECT_" , readPath, readPath_GBS ) == "Success" && 
      exportToBlobStorage_CAP (job,notebook,0, sqldf_incremental_month, "FCACCMONTHINDIRECT_" , readPath, readPath_GBS ) == "Success" 
      )  "Success" else "Failure"  }
else if (type_of_ETL == 1) { 
  if (exportToBlobStorage_CAP (job,notebook,1, sqldf_incremental_week, "FCACCWEEKINDIRECT_" , readPath, readPath_GBS ) == "Success" && 
     exportToBlobStorage_CAP (job,notebook,1, sqldf_incremental_month, "FCACCMONTHINDIRECT_" , readPath, readPath_GBS ) == "Success"
     ) "Success" else  "Failure"  }
else if (type_of_ETL == 2) { 
  if (exportToBlobStorage_Baltika (job,notebook,fc_indirect_weekly, sqldf_full_week, readPath ) == "Success" &&  
      exportToBlobStorage_Baltika (job,notebook,fc_indirect_monthly, sqldf_full_month, readPath ) == "Success"  &&    
      exportToBlobStorage_CAP (job,notebook,1, sqldf_incremental_week, "FCACCWEEKINDIRECT_" , readPath, readPath_GBS ) == "Success" && 
      exportToBlobStorage_CAP (job,notebook,1, sqldf_incremental_month, "FCACCMONTHINDIRECT_" , readPath, readPath_GBS) == "Success" 
     ) "Success" else "Failure" }
else "Unexpected parameter"


//save to log
save_to_log_last_step(job,notebook,Result,notebook_start_time)

dbutils.notebook.exit(Result)