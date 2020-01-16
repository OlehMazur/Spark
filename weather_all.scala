// Databricks notebook source
// MAGIC %run /Users/o_mazur@carlsberg.ua/dcd_etl_functions

// COMMAND ----------

//numbers of week back from current date, if "0" - current week
//numbers of week ahead from current date

// COMMAND ----------

val numbers_of_weeks_back: Int = 0
val numbers_of_weeks_ahead: Int = 78
val type_of_scenario: Int = 1 // if 0 - with new weather data extraction process

// COMMAND ----------

//constants

// COMMAND ----------

val SearchPath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/Transformation/History/"
//val file_type = "csv"
val fname_direct = "weather_direct_ru.csv"
val fname_indirect = "weather_indirect_ru.csv"
val year_list : Seq[String] = Seq("2016","2017","2018","2019","2020", "2021")

// COMMAND ----------

//util variables

// COMMAND ----------

val job = "dcd_notebook_workflow_weather"
val notebook = "weather_all.scala"
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

val start_time = LocalDateTime.now

val is_regularly_updated_table = false
val source_file_name = "Plant_City.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Plant_City")

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

val start_time = LocalDateTime.now

val is_regularly_updated_table = false
val source_file_name = "PGO_City.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("PGO_City")

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

if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0 && is_regularly_updated_table  ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, source_file_name )
  save_to_log_last_step(job,notebook,"Skipped",notebook_start_time)
  dbutils.notebook.exit("Skipped")  
  //throw new Exception("the source file is not up to date")
}


// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = false
val source_file_name = "ETL/tmp/city.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("all_city_list")

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

//val city_list = spark.sql("select distinct city from Plant_City where city = 'Kostroma' ") //Test delete where !!!
val city_list = spark.sql("""
select distinct city 
from Plant_City 
where city not in (
select distinct city from Plant_City where city not in (select  distinct city from all_city_list)
)
""")

// COMMAND ----------

val city_plant = spark.sql("""
select distinct city, plant 
from Plant_City
where city not in (
select distinct city from Plant_City where city not in (select  distinct city from all_city_list)
)
""")
city_plant.createOrReplaceTempView("city_plant")

// COMMAND ----------

//numbers of week ahead from current date

// COMMAND ----------

val mv  =LocalDateTime.now.plusWeeks(-1 * numbers_of_weeks_back)
val current_minus_n_weeks = mv.getYear * 10000 + mv.getMonthValue *100 +mv.getDayOfMonth
val query_mv = s"select DayName as report_date, WeekId from Calendar where datekey = $current_minus_n_weeks" 
val sqldf_mv = spark.sql(query_mv)
sqldf_mv.createOrReplaceTempView("report_week_info")

val pp  = mv.plusWeeks(numbers_of_weeks_ahead)
val current_plus_n_weeks = pp.getYear * 10000 + pp.getMonthValue *100 +pp.getDayOfMonth
val query = s"select WeekId from Calendar where datekey = $current_plus_n_weeks" 
val sqldf = spark.sql(query)
sqldf.createOrReplaceTempView("till_week_info")


// COMMAND ----------

//check if city is in City list (correct name )

// COMMAND ----------

// MAGIC %sql 
// MAGIC select distinct city from Plant_City where city not in (select  distinct city from all_city_list)

// COMMAND ----------

val current_year = "2021"

// COMMAND ----------

if (type_of_scenario == 0) 
{
    city_list.collect.foreach {
     (x) => { val status = dbutils.notebook.run("weather_dark_sky_search", 
                                 0, 
                                 Map(
                                     "City" -> x.toString().replace("[", "").replace("]", ""),
                                     "Year" -> current_year,
                                     "API_Key" -> "d4b20ef8a9ad8bcf2449be822fba03a4",
                                     "CONTAINER_NAME"  -> "prod"     
                                    )
                                ) 
                    println(status)
                    }
    }
}


// COMMAND ----------

//final file creation

// COMMAND ----------

import sqlContext.implicits._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.expr

val schema_year = "time,apparentTemperatureMax,cloudCover,humidity,windSpeed,city,date"
val schema_rdd_year = StructType(schema_year.split(",").map(fieldName => StructField(fieldName, StringType, true)) )
var result_df = sqlContext.createDataFrame(sc.emptyRDD[Row], schema_rdd_year)


for (year <- year_list) {

  val schema_string = "time,apparentTemperatureMax,cloudCover,humidity,windSpeed,city,date"
  val schema_rdd = StructType(schema_string.split(",").map(fieldName => StructField(fieldName, StringType, true)) )
  var empty_df = sqlContext.createDataFrame(sc.emptyRDD[Row], schema_rdd)

  val name : String = year  
  val file_list : Seq[String] = dbutils.fs.ls(SearchPath).map(_.path).filter(_.contains(name))
  for (file <- file_list)  {
    val file_list : Seq[String] = dbutils.fs.ls(file).map(_.path).filter(_.contains(name)) 
    if (file_list.size > 0) {
 
     var city = file_list(0).replace(file, "").replace("weather_", "").replace("_"+name + ".csv", "") 
     var file_location = file + file_list(0).replace(file, "")
     var dff = spark.read.format(file_type).option("delimiter", ";").option("header", "true").load(file_location) 
    
     if (!dff.columns.contains("time")) dff = dff.withColumn("time", expr("null") ) 
     if (!dff.columns.contains("apparentTemperatureMax"))  dff = dff.withColumn("apparentTemperatureMax", expr("null"))
     if (!dff.columns.contains("cloudCover")) dff = dff.withColumn("cloudCover", expr("null")) 
     if (!dff.columns.contains("humidity")) dff = dff.withColumn("humidity", expr("null")) 
     if (!dff.columns.contains("windSpeed")) dff = dff.withColumn("windSpeed", expr("null")) 
    
    dff = dff.select("time", "apparentTemperatureMax", "cloudCover", "humidity", "windSpeed") 
    
    var empty_df_city_date = dff.withColumn("city", lit(city)).withColumn("date", expr("cast(from_unixtime(time) as date)"))

   empty_df = empty_df.union(empty_df_city_date)
    }
  }
  
  result_df = result_df.union(empty_df)
   
}  
result_df.createOrReplaceTempView("main")


// COMMAND ----------

val num_of_week_left_query = "select int(max(cl.weekid) - t.WeekId) num_of_week_left from main m left join  calendar cl on cast(m.date as timestamp)  = cl.DayName cross join till_week_info t group by  t.WeekId"

// COMMAND ----------

val num_of_week_left_before_new_extraction = spark.sql(num_of_week_left_query).first.getInt(0)

// COMMAND ----------

get_weather_extraction_info (job, notebook, num_of_week_left_before_new_extraction )

// COMMAND ----------

sendEmail_forAdmin("Weather data extraction", num_of_week_left_before_new_extraction.toString() + " weeks left before the new extraction ")

// COMMAND ----------

val sql_query_direct = """
select 
distinct cl.MonthId partition_name,  m.city, c.plant, cl.weekid calendar_yearweek, /*date time,*/ date_add(cast(date as date),1 ) time,   m.apparentTemperatureMax, m.cloudCover, m.humidity, m.windSpeed
from main m 
left join city_plant c  on  m.city = c.city 
left join calendar cl on cast(m.date as timestamp)  = cl.DayName
where cl.weekid <= (select distinct WeekId from till_week_info)
"""

// COMMAND ----------

val sqldf_direct = spark.sql(sql_query_direct)

// COMMAND ----------

//Load hole file to Baltika for Direct Weather

// COMMAND ----------

val sql_query_direct_incremental = """
select *
from (
select 
distinct cl.MonthId partition_name, cl.MonthId calendar_yearmonth,  m.city, c.plant, cl.weekid calendar_yearweek, /*date time,*/ date_add(cast(date as date),1 ) time,   m.apparentTemperatureMax, m.cloudCover, m.humidity, m.windSpeed
from main m 
left join city_plant c  on  m.city = c.city 
left join calendar cl on cast(m.date as timestamp)  = cl.DayName
where cl.weekid <= (select distinct WeekId from till_week_info)
) tab """  +
{if (type_of_data_extract == 1) 
 s""" 
 where calendar_yearmonth >= year(date_add(current_date(),-1 * $num_of_days_before_current_date))*100 + month(date_add(current_date(),-1 * $num_of_days_before_current_date))
 
 """ 
 else ""}

// COMMAND ----------

val sqldf_direct_incremental = spark.sql(sql_query_direct_incremental)

// COMMAND ----------

//Weather Indirect 

// COMMAND ----------

val sql_query_indirect = """
select 
distinct cl.MonthId partition_name,  m.city, c.pgo PGO , cl.weekid calendar_yearweek, /*date time,*/ date_add(cast(date as date),1 ) time,   m.apparentTemperatureMax, m.cloudCover, m.humidity, m.windSpeed
from main m 
left join pgo_city c  on  m.city = c.city_eng 
left join calendar cl on cast(m.date as timestamp)  = cl.DayName
where cl.weekid <= (select distinct WeekId from till_week_info) and c.pgo is not null
"""

// COMMAND ----------

val sqldf_indirect = spark.sql(sql_query_indirect)

// COMMAND ----------

//Load hole file to Baltika for Indirect Weather

// COMMAND ----------

val sql_query_indirect_incremental = """
select *
from (
select 
distinct cl.MonthId partition_name, cl.MonthId calendar_yearmonth, m.city, c.pgo PGO , cl.weekid calendar_yearweek, /*date time,*/ date_add(cast(date as date),1 ) time,   m.apparentTemperatureMax, m.cloudCover, m.humidity, m.windSpeed
from main m 
left join pgo_city c  on  m.city = c.city_eng 
left join calendar cl on cast(m.date as timestamp)  = cl.DayName
where cl.weekid <= (select distinct WeekId from till_week_info) and c.pgo is not null
) tab
""" +{if (type_of_data_extract == 1) 
 s""" 
 where calendar_yearmonth >= year(date_add(current_date(),-1 * $num_of_days_before_current_date))*100 + month(date_add(current_date(),-1 * $num_of_days_before_current_date))
 
 """ 
 else ""}

// COMMAND ----------

val sqldf_indirect_incremental = spark.sql(sql_query_indirect_incremental)

// COMMAND ----------

//Final testing before result export

// COMMAND ----------

exportToBlobStorage_Baltika_Result_Before_Testing(job,notebook,fname_direct, sqldf_direct, readPath)

// COMMAND ----------

exportToBlobStorage_Baltika_Result_Before_Testing(job,notebook,fname_indirect, sqldf_indirect, readPath)

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
  if (exportToBlobStorage_Baltika (job,notebook,fname_direct, sqldf_direct ,readPath ) == "Success" && 
      exportToBlobStorage_Baltika (job,notebook,fname_indirect, sqldf_indirect, readPath ) == "Success" &&  
      exportToBlobStorage_CAP (job,notebook,0, sqldf_direct_incremental, "WEATHERDIRECT_" , readPath, readPath_GBS) == "Success" && 
      exportToBlobStorage_CAP (job,notebook,0, sqldf_indirect_incremental, "WEATHERINDIRECT_" , readPath, readPath_GBS) == "Success" 
     )  "Success" else "Failure"  }
else if (type_of_ETL == 1) { 
  if (exportToBlobStorage_CAP (job,notebook,1, sqldf_direct_incremental, "WEATHERDIRECT_" , readPath, readPath_GBS) == "Success" && 
      exportToBlobStorage_CAP (job,notebook,1, sqldf_indirect_incremental, "WEATHERINDIRECT_" , readPath, readPath_GBS) == "Success" 
     ) "Success" else  "Failure"  }
else if (type_of_ETL == 2) { 
  if (exportToBlobStorage_Baltika (job,notebook,fname_direct, sqldf_direct, readPath ) == "Success" && 
      exportToBlobStorage_Baltika (job,notebook,fname_indirect, sqldf_indirect, readPath ) == "Success" &&  
      exportToBlobStorage_CAP (job,notebook,1, sqldf_direct_incremental, "WEATHERDIRECT_" , readPath, readPath_GBS) == "Success" && 
      exportToBlobStorage_CAP (job,notebook,1, sqldf_indirect_incremental, "WEATHERINDIRECT_" , readPath, readPath_GBS) == "Success" 
     ) "Success" else "Failure" }
else "Unexpected parameter"


//save to log
save_to_log_last_step(job,notebook,Result,notebook_start_time)

dbutils.notebook.exit(Result)