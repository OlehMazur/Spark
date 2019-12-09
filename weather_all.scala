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

val num_of_days_before_current_date: Int = 30
//val num_of_days_after_current_date: Int = 30   

// COMMAND ----------

//Type of scenario: 0 (full weather extraction  ) , 1 (incremental weather extraction  - should be this on for dayly loading)

// COMMAND ----------

val type_of_scenario: Int = 0

// COMMAND ----------

//numbers of week back from current date, if "0" - current week
//numbers of week ahead from current date

// COMMAND ----------

val numbers_of_weeks_back: Int = 0
val numbers_of_weeks_ahead: Int = 78

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
val writePath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/Result" 
val writePath_СAP = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/export_to_CAP"
val SearchPath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/Transformation/History/"
val file_type = "csv"
val fname_direct = "weather_direct_ru.csv"
val fname_indirect = "weather_indirect_ru.csv"

// COMMAND ----------

//constants (CAP)

// COMMAND ----------

val writePath_GBS = "wasbs://dcd@prdcbwesa01.blob.core.windows.net/RU" 
val readPath_GBS = "wasbs://dcd@prdcbwesa01.blob.core.windows.net/RU/ru_tmp" 

// COMMAND ----------

val year_list : Seq[String] = Seq("2016","2017","2018","2019","2020", "2021")

// COMMAND ----------

val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/Plant_City.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Plant_City")

// COMMAND ----------

val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/PGO_City.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("PGO_City")

// COMMAND ----------

val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/Calendar.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Calendar")

// COMMAND ----------

val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/tmp/city.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("all_city_list")

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

// val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/Result/weather_ru.csv"
// val file_type = "csv"
// val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
// df.createOrReplaceTempView("weather")

// COMMAND ----------

//check if it's a new city added to Plant_City file

// COMMAND ----------

// %sql 
// select distinct city from Plant_City where city not in (select  distinct city from weather)

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

//delete

// COMMAND ----------

// val city_list2 = spark.sql("""
// select city 
// from (
// select 'Kursk' as  city
// union 
// select 'Orenburg' as city
// union 
// select 'Krasnoyarsk' as city
// union
// select 'Voronezh' as city 
// union 
// select 'Nizhny Novgorod' as city
// )
// """)

// COMMAND ----------

// val current_year = "2017"

// COMMAND ----------


//     city_list2.collect.foreach {
//      (x) => { val status = dbutils.notebook.run("test_w", 
//                                  0, 
//                                  Map(
//                                      "City" -> x.toString().replace("[", "").replace("]", ""),
//                                      "Year" -> current_year,
//                                      "API_Key" -> "d4b20ef8a9ad8bcf2449be822fba03a4",
//                                      "CONTAINER_NAME"  -> "prod"     
//                                     )
//                                 ) 
//                     println(status)
//                     }
//     }



// COMMAND ----------

//end delete

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

// %sql 
// select distinct year(date) from main 

// COMMAND ----------

//val year_list : Seq[String] = Seq("2016","2017","2018","2019","2020")

// COMMAND ----------

// var i:Long = 0
// for (year <- year_list) {
//  var g:Long = 0
//   val name : String = year  
//   val file_list : Seq[String] = dbutils.fs.ls(SearchPath).map(_.path).filter(_.contains(name))
//   for (file <- file_list)  {
//     val file_list : Seq[String] = dbutils.fs.ls(file).map(_.path).filter(_.contains(name)) 
//     if (file_list.size > 0) {
 
//      var city = file_list(0).replace(file, "").replace("weather_", "").replace("_"+name + ".csv", "") 
//      var file_location = file + file_list(0).replace(file, "")
//      var dff = spark.read.format(file_type).option("delimiter", ";").option("header", "true").load(file_location) 
//       //println (year)
//       //println(dff.count)
//       var y = dff.count
//      i = i+ y
//     }
     
//   }
 
// println(year)
// println(g)
// }
//  println (i) 
 

// COMMAND ----------

// %sql
// select  year(date), count(*)  from main group by year(date) --order by 1 --where city = 'Irkutsk' --and date <> '2015-12-31'

// COMMAND ----------

// %sql select count(*) from main

// COMMAND ----------

// %sql 
// select 
// distinct cl.MonthId partition_name,  m.city, c.plant, cl.weekid calendar_yearweek, /*date time,*/ date_add(cast(date as date),1 ) time,   m.apparentTemperatureMax, m.cloudCover, m.humidity, m.windSpeed
// from main m 
// left join city_plant c  on  m.city = c.city 
// left join calendar cl on cast(m.date as timestamp)  = cl.DayName
// where cl.weekid <= (select distinct WeekId from till_week_info)

// COMMAND ----------

// %sql select * from till_week_info

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

def exportToBlobStorage_Baltika_direct: String = { 

import com.databricks.WorkflowException
import java.io.FileNotFoundException

var Result = "Failure"   

try {
 
//val sql_df = spark.sql(sql_query_direct)
sqldf_direct.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").save(readPath)

val name : String = "part-00000"  
val file_list : Seq[String] = dbutils.fs.ls(readPath).map(_.path).filter(_.contains(name))
val read_name = if (file_list.length >= 1 ) file_list(0).replace(readPath + "/", "")
val row_count = spark.read.format("csv").option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_list(0)).count   
dbutils.fs.mv(readPath+"/"+read_name , writePath+"/"+fname_direct)   
dbutils.fs.rm(readPath , recurse = true) 
if (row_count > 0) Result = "Success" else println("The file " +writePath+"/"+fname_direct + " is empty !" )
} 
catch {
  case e:FileNotFoundException => println("Error, " + e)
  case e:WorkflowException  => println("Error, " + e)
}

  Result
}

//dbutils.notebook.exit(Result)



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

def exportToBlobStorage_direct (type_of_ETL:Int): String = { 

import com.databricks.WorkflowException
import java.io.FileNotFoundException

var Result = "Failure" 
val partition_field = "partition_name"
val export_format = "com.databricks.spark.csv"
val export_delimiter = Character.toString(7.toChar)

var readPath_ETL = if (type_of_ETL == 0) readPath else if (type_of_ETL == 1) readPath_GBS else null
var writePath_ETL= if (type_of_ETL == 0) writePath_СAP else if (type_of_ETL == 1) writePath_GBS else null

try {
  sqldf_direct_incremental
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
   var fname = "WEATHERDIRECT_" + partition_name + "_RU_DCD"+ ".csv" 
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

// val Result = 
// if (type_of_ETL == 0) exportToBlobStorage(0) 
// else if (type_of_ETL == 1) exportToBlobStorage(1)
// else if (type_of_ETL == 2) {exportToBlobStorage(0); exportToBlobStorage(1) }
// else "Unexpected parameter"

// dbutils.notebook.exit(Result)

// COMMAND ----------

//Weather Indirect 

// COMMAND ----------

val sql_query_indirect = """
select 
distinct cl.MonthId partition_name,  m.city, c.pgo , cl.weekid calendar_yearweek, /*date time,*/ date_add(cast(date as date),1 ) time,   m.apparentTemperatureMax, m.cloudCover, m.humidity, m.windSpeed
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

def exportToBlobStorage_Baltika_indirect: String = { 

import com.databricks.WorkflowException
import java.io.FileNotFoundException

var Result = "Failure"   

try {
 
//val sql_df = spark.sql(sql_query_direct)
sqldf_indirect.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").save(readPath)

val name : String = "part-00000"  
val file_list : Seq[String] = dbutils.fs.ls(readPath).map(_.path).filter(_.contains(name))
val read_name = if (file_list.length >= 1 ) file_list(0).replace(readPath + "/", "")
val row_count = spark.read.format("csv").option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_list(0)).count   
dbutils.fs.mv(readPath+"/"+read_name , writePath+"/"+fname_indirect)   
dbutils.fs.rm(readPath , recurse = true) 
if (row_count > 0) Result = "Success" else println("The file " +writePath+"/"+fname_indirect + " is empty !" )
} 
catch {
  case e:FileNotFoundException => println("Error, " + e)
  case e:WorkflowException  => println("Error, " + e)
}
  Result

}
//dbutils.notebook.exit(Result)



// COMMAND ----------

val sql_query_indirect_incremental = """
select *
from (
select 
distinct cl.MonthId partition_name, cl.MonthId calendar_yearmonth, m.city, c.pgo , cl.weekid calendar_yearweek, /*date time,*/ date_add(cast(date as date),1 ) time,   m.apparentTemperatureMax, m.cloudCover, m.humidity, m.windSpeed
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

def exportToBlobStorage_indirect (type_of_ETL:Int): String = { 

import com.databricks.WorkflowException
import java.io.FileNotFoundException

var Result = "Failure" 
val partition_field = "partition_name"
val export_format = "com.databricks.spark.csv"
val export_delimiter = Character.toString(7.toChar)

var readPath_ETL = if (type_of_ETL == 0) readPath else if (type_of_ETL == 1) readPath_GBS else null
var writePath_ETL= if (type_of_ETL == 0) writePath_СAP else if (type_of_ETL == 1) writePath_GBS else null

try {
  sqldf_indirect_incremental
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
   var fname = "WEATHERINDIRECT_" + partition_name + "_RU_DCD"+ ".csv" 
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
if (type_of_ETL == 0) { if (exportToBlobStorage_Baltika_direct == "Success"  && exportToBlobStorage_Baltika_indirect  == "Success" && exportToBlobStorage_direct(0) == "Success" && exportToBlobStorage_indirect(0) == "Success" ) "Success" else "Failure" }
else if (type_of_ETL == 1)  { if (exportToBlobStorage_direct(1) == "Success" && exportToBlobStorage_indirect(1) == "Success" ) "Success" else "Failure"  }
else if (type_of_ETL == 2)  { if (exportToBlobStorage_Baltika_direct == "Success"  && exportToBlobStorage_Baltika_indirect  == "Success" && exportToBlobStorage_direct(0) == "Success" && exportToBlobStorage_indirect(0) == "Success" && exportToBlobStorage_direct(1) == "Success" && exportToBlobStorage_indirect(1) == "Success" )}
else "Unexpected parameter"

dbutils.notebook.exit(Result)

// COMMAND ----------

// %sql 
// select city , year (time) year ,  count (distinct time) num_of_rows 
// from weather_direct_ru
// where year(time) <> 2021
// group by city , year(time)
// having (num_of_rows < 364)
