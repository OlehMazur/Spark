// Databricks notebook source
// MAGIC %run /Users/o_mazur@carlsberg.ua/dcd_etl_functions

// COMMAND ----------

//util variables

// COMMAND ----------

val fname = "open_orders_etl_tuesday_with_format_All.csv"
val job = "dcd_notebook_workflow_open_orders"
val notebook = "oo_tuesday_all.scala"
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

//MD_Clients

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = true
val source_file_name = "MD_Clients.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("MD_Clients")

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

//Divisions

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = true
val source_file_name = "Division.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Divisions")

save_to_log_file_processing(job,notebook,start_time, df, source_file_name )

//number of rows in the file > 1
if (df.count == 0) {
  save_to_log_if_source_is_empty(job,notebook,start_time, source_file_name)
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
  //throw new Exception("the source file is empty")
}

//if the source file is up todate

if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0 && is_regularly_updated_table ) {
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

if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0 && is_regularly_updated_table ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, source_file_name )
  save_to_log_last_step(job,notebook,"Skipped",notebook_start_time)
  dbutils.notebook.exit("Skipped")  
  //throw new Exception("the source file is not up to date")
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

if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0 && is_regularly_updated_table ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, source_file_name )
  save_to_log_last_step(job,notebook,"Skipped",notebook_start_time)
  dbutils.notebook.exit("Skipped")  
  //throw new Exception("the source file is not up to date")
}

// COMMAND ----------

//orders_wpromo_v4.csv/Sell_in

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = true
val source_file_name = "Sell_in_All.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("orders_wpromo")

save_to_log_file_processing(job,notebook,start_time, df, source_file_name )

//number of rows in the file > 1
if (df.count == 0) {
  save_to_log_if_source_is_empty(job,notebook,start_time, source_file_name)
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
  //throw new Exception("the source file is empty")
}

//if the source file is up todate

if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0 && is_regularly_updated_table ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, source_file_name )
  save_to_log_last_step(job,notebook,"Skipped",notebook_start_time)
  dbutils.notebook.exit("Skipped")  
  //throw new Exception("the source file is not up to date")
}


// COMMAND ----------

//PlantID

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = true
val source_file_name = "PlantID.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("plant_id_info")

save_to_log_file_processing(job,notebook,start_time, df, source_file_name )

//number of rows in the file > 1
if (df.count == 0) {
  save_to_log_if_source_is_empty(job,notebook,start_time, source_file_name)
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
  //throw new Exception("the source file is empty")
}

//if the source file is up todate

if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0 && is_regularly_updated_table ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, source_file_name )
  save_to_log_last_step(job,notebook,"Skipped",notebook_start_time)
  dbutils.notebook.exit("Skipped")  
  //throw new Exception("the source file is not up to date")
}

// COMMAND ----------

//CPG_Formats

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = false
val source_file_name = "CPG_Formats.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("CPG_Formats")

save_to_log_file_processing(job,notebook,start_time, df, source_file_name )

//number of rows in the file > 1
if (df.count == 0) {
  save_to_log_if_source_is_empty(job,notebook,start_time, source_file_name)
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
  //throw new Exception("the source file is empty")
}

//if the source file is up todate

if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0 && is_regularly_updated_table  ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, source_file_name )
  save_to_log_last_step(job,notebook,"Skipped",notebook_start_time)
  dbutils.notebook.exit("Skipped")  
  //throw new Exception("the source file is not up to date")
}

//some tables are not updating on a regular base
if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0 && !is_regularly_updated_table  ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, source_file_name )
  
  //throw new Exception("the source file is not up to date")
}

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

val query = s"""
select distinct calendar_yearmonth as partition_name, * from result_source_table 
""" +
{if (type_of_data_extract == 1) 
 s""" 
 where calendar_yearmonth >= year(date_add(current_date(),-1 * $num_of_days_before_current_date))*100 + month(date_add(current_date(),-1 * $num_of_days_before_current_date))
 """ 
 else ""}

// COMMAND ----------

val sqldf = spark.sql(query)

// COMMAND ----------

//Final testing before result export

// COMMAND ----------

exportToBlobStorage_Baltika_Result_Before_Testing(job,notebook,fname, sqldf_bal, readPath)

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

// if (Test_Is_incomplete_period(job,notebook, sqldf_result, fname) == "FAILED") {
//   save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
//   dbutils.notebook.exit("Failure")    
// }

// COMMAND ----------

if (Test_Missing_Filed(job, notebook, sqldf_result, "lead_sku", fname ) == "FAILED" ) {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
}

// COMMAND ----------

Test_Values_In_Correct_Range(job, notebook, sqldf_result, "monthly", "open_orders_promo", 0, 209331, fname)

// COMMAND ----------

Test_Values_In_Correct_Range(job, notebook, sqldf_result, "monthly", "open_orders", 0, 92318, fname)

// COMMAND ----------

if (Test_Missing_Filed(job, notebook, sqldf_result, "open_orders_promo", fname ) == "FAILED" ) {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
}

// COMMAND ----------

if (Test_Missing_Filed(job, notebook, sqldf_result, "open_orders", fname ) == "FAILED" ) {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
}

// COMMAND ----------

//end Final testing before result export

// COMMAND ----------

//export to CAP

// COMMAND ----------

val Result = 
if (type_of_ETL == 0) { 
  if (exportToBlobStorage_Baltika (job,notebook,fname, sqldf_bal, readPath ) == "Success" &&      
      exportToBlobStorage_CAP (job,notebook,0, sqldf, "OPENORDERSTUE_" , readPath, readPath_GBS) == "Success" 
      )  "Success" else "Failure"  }
else if (type_of_ETL == 1) { 
  if (exportToBlobStorage_CAP (job,notebook,1, sqldf, "OPENORDERSTUE_" , readPath, readPath_GBS) == "Success"       
     ) "Success" else  "Failure"  }
else if (type_of_ETL == 2) { 
  if (exportToBlobStorage_Baltika (job,notebook,fname, sqldf_bal, readPath ) == "Success" && 
      /*exportToBlobStorage_CAP (job,notebook,0, sqldf, "OPENORDERSTUE_") == "Success" && */
      exportToBlobStorage_CAP (job,notebook,1, sqldf, "OPENORDERSTUE_" , readPath, readPath_GBS) == "Success"  
     ) "Success" else "Failure" }
else "Unexpected parameter"


//save to log
save_to_log_last_step(job,notebook,Result,notebook_start_time)

dbutils.notebook.exit(Result)