// Databricks notebook source
// MAGIC %run /Users/o_mazur@carlsberg.ua/dcd_etl_functions

// COMMAND ----------

//event files name

// COMMAND ----------

val fname_direct = "events_direct_ru.csv"
val fname_indirect = "events_indirect_ru.csv"

// COMMAND ----------

// source file with events

// COMMAND ----------

val file_location_direct_indirect = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/manually/"
val file_events_direct = "events_direct_ru.csv"
val file_events_indirect = "events_indirect_ru.csv"
val manually_path = "ETL/manually/"

// COMMAND ----------

//util variables

// COMMAND ----------

val job = "dcd_notebook_workflow_event"
val notebook = "events.scala"
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

//file_events_direct

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = false
val file_location = file_location_direct_indirect + file_events_direct
val file_location_log = manually_path + file_events_direct
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("events")

save_to_log_file_processing(job,notebook,start_time, df, file_location_log )

//number of rows in the file > 1
if (df.count == 0) {
  save_to_log_if_source_is_empty(job,notebook,start_time, file_location_log)
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
  //throw new Exception("the source file is empty")
}

//if the source file is up todate

if (!get_last_modified_date_for_blob(file_location_log).equals(current_date) && is_logic_changed == 0  && is_regularly_updated_table ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, file_location_log )
  save_to_log_last_step(job,notebook,"Skipped",notebook_start_time)
  dbutils.notebook.exit("Skipped")  
  //throw new Exception("the source file is not up to date")
}

//some tables are not updating on a regular base
if (!get_last_modified_date_for_blob(file_location_log).equals(current_date) && is_logic_changed == 0 && !is_regularly_updated_table  ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, file_location_log )
}

// COMMAND ----------

//file_events_indirect

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = false
val file_location = file_location_direct_indirect + file_events_indirect
val file_location_log = manually_path + file_events_indirect
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("events_indirect")

save_to_log_file_processing(job,notebook,start_time, df, file_location_log )

//number of rows in the file > 1
if (df.count == 0) {
  save_to_log_if_source_is_empty(job,notebook,start_time, file_location_log)
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")   
  //throw new Exception("the source file is empty")
}

//if the source file is up todate

if (!get_last_modified_date_for_blob(file_location_log).equals(current_date) && is_logic_changed == 0 && is_regularly_updated_table ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, file_location_log )
  save_to_log_last_step(job,notebook,"Skipped",notebook_start_time)
  dbutils.notebook.exit("Skipped") 
  //throw new Exception("the source file is not up to date")
}

//some tables are not updating on a regular base
if (!get_last_modified_date_for_blob(file_location_log).equals(current_date) && is_logic_changed == 0 && !is_regularly_updated_table  ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, file_location_log )
}


// COMMAND ----------

val query_direct_full = """
select distinct
calendar_yearmonth_end partition_name,  *  
from events 
"""

// COMMAND ----------

val query_direct_incremental = """
select distinct
calendar_yearmonth_end partition_name, calendar_yearmonth_end calendar_yearmonth,   *  
from events 
""" +
{if (type_of_data_extract == 1) 
 s""" 
 where calendar_yearmonth_end >= year(date_add(current_date(),-1 * $num_of_days_before_current_date))*100 + month(date_add(current_date(),-1 * $num_of_days_before_current_date))
 
 """ 
 else ""}

// COMMAND ----------

val sqldf_direct_full = spark.sql(query_direct_full)

// COMMAND ----------

val sqldf_direct_incremental = spark.sql(query_direct_incremental)

// COMMAND ----------

val query_indirect_full = """ 
select distinct calendar_yearmonth_end partition_name,  *  from events_indirect 
"""

// COMMAND ----------

val query_indirect_incremental = """ 
select distinct calendar_yearmonth_end partition_name, calendar_yearmonth_end calendar_yearmonth, *  from events_indirect 
""" +
{if (type_of_data_extract == 1) 
 s""" 
 where calendar_yearmonth_end >= year(date_add(current_date(),-1 * $num_of_days_before_current_date))*100 + month(date_add(current_date(),-1 * $num_of_days_before_current_date))
 
 """ 
 else ""}

// COMMAND ----------

val sqldf_indirect_full = spark.sql(query_indirect_full)

// COMMAND ----------

val sqldf_indirect_incremental = spark.sql(query_indirect_incremental)

// COMMAND ----------

//Final testing before result export

// COMMAND ----------

exportToBlobStorage_Baltika_Result_Before_Testing(job,notebook,fname_direct, sqldf_direct_full, readPath)

// COMMAND ----------

exportToBlobStorage_Baltika_Result_Before_Testing(job,notebook,fname_indirect, sqldf_indirect_full, readPath)

// COMMAND ----------

val sqldf_result_wd = load_preliminary_result(fname_direct)

// COMMAND ----------

val sqldf_result_wi = load_preliminary_result(fname_indirect)

// COMMAND ----------

save_to_log_start_result_testing(job,notebook)

// COMMAND ----------

if (Test_Number_of_Rows(job,notebook, sqldf_result_wd, fname_direct) == "FAILED") {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")    
}

// COMMAND ----------

if (Test_Number_of_Rows(job,notebook, sqldf_result_wi, fname_indirect) == "FAILED") {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")    
}

// COMMAND ----------

//end Final testing before result export

// COMMAND ----------

val Result = 
if (type_of_ETL == 0) { 
  if (exportToBlobStorage_Baltika (job,notebook,fname_direct, sqldf_direct_full, readPath ) == "Success" && 
      exportToBlobStorage_Baltika (job,notebook,fname_indirect, sqldf_indirect_full, readPath ) == "Success" &&  
      exportToBlobStorage_CAP (job,notebook,0, sqldf_direct_incremental, "EVENTSDIRECT_" , readPath, readPath_GBS) == "Success" && 
      exportToBlobStorage_CAP (job,notebook,0, sqldf_indirect_incremental, "EVENTSINDIRECT_", readPath, readPath_GBS ) == "Success" 
     )  "Success" else "Failure"  }
else if (type_of_ETL == 1) { 
  if (exportToBlobStorage_CAP (job,notebook,1, sqldf_direct_incremental, "EVENTSDIRECT_", readPath, readPath_GBS) == "Success" == "Success" && 
      exportToBlobStorage_CAP (job,notebook,1, sqldf_indirect_incremental, "EVENTSINDIRECT_" , readPath, readPath_GBS)  == "Success" =="Success"
     ) "Success" else  "Failure"  }
else if (type_of_ETL == 2) { 
  if (exportToBlobStorage_Baltika (job,notebook,fname_direct, sqldf_direct_full, readPath ) == "Success" && 
      exportToBlobStorage_Baltika (job,notebook,fname_indirect, sqldf_indirect_full, readPath ) == "Success" && 
      exportToBlobStorage_CAP (job,notebook,1, sqldf_direct_incremental, "EVENTSDIRECT_" , readPath, readPath_GBS) == "Success" && 
      exportToBlobStorage_CAP (job,notebook,1, sqldf_indirect_incremental, "EVENTSINDIRECT_" , readPath, readPath_GBS) == "Success"
     ) "Success" else "Failure" }
else "Unexpected parameter"


//save to log
save_to_log_last_step(job,notebook,Result,notebook_start_time)

dbutils.notebook.exit(Result)