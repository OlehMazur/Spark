// Databricks notebook source
//Type of ETL: 0 (only Baltika ) 1 (only CAP) 2 (Both)

// COMMAND ----------

val type_of_ETL: Int = 0

// COMMAND ----------

//Type of scenario: 0 (full weather extraction  ) , 1 (incremental weather extraction )

// COMMAND ----------

val type_of_scenario: Int = 0

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
val SearchPath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/Transformation/Weather/"
val file_type = "csv"
val fname = "weather_ru.csv"

// COMMAND ----------

//constants (CAP)

// COMMAND ----------

val writePath_GBS = "wasbs://dcd@prdcbwesa01.blob.core.windows.net/RU" 
val readPath_GBS = "wasbs://dcd@prdcbwesa01.blob.core.windows.net/RU/ru_tmp" 

// COMMAND ----------

//val year_list : Seq[String] = Seq("2019")
val year_list : Seq[String] = Seq("2016","2017","2018","2019","2020")

// COMMAND ----------

val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/Plant_City.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Plant_City")

// COMMAND ----------

val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/Calendar.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Calendar")

// COMMAND ----------

//val city_list = spark.sql("select distinct city from Plant_City where city = 'Kostroma' ") //Test delete where !!!
val city_list = spark.sql("select distinct city from Plant_City ")

// COMMAND ----------

val city_plant = spark.sql("select distinct city, plant from Plant_City")
city_plant.createOrReplaceTempView("city_plant")

// COMMAND ----------

val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/Result/weather_ru.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("weather")

// COMMAND ----------

//check if it's a new city added to Plant_City file

// COMMAND ----------

// MAGIC %sql 
// MAGIC select distinct city from Plant_City where city not in (select  distinct city from weather)

// COMMAND ----------

// dbutils.notebook.run("test2", 0, Map("city" -> "Rivne"))
// dbutils.notebook.exit("Success")

// COMMAND ----------

val current_year = "2016"

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



// COMMAND ----------

val sql_query = """
select distinct cl.MonthId partition_name,  m.city, c.plant, cl.weekid calendar_yearweek, date time, m.apparentTemperatureMax, m.cloudCover, m.humidity, m.windSpeed
from main m left join city_plant c  on  m.city = c.city left join calendar cl on cast(m.date as timestamp)  = cl.DayName
"""

// COMMAND ----------

val sqldf = spark.sql(sql_query)

// COMMAND ----------

//Load hole file to Baltika

// COMMAND ----------

import com.databricks.WorkflowException
import java.io.FileNotFoundException

var Result = "Failure"   

try {
 
val sql_df = spark.sql(sql_query)
sql_df.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").save(readPath)

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

//dbutils.notebook.exit(Result)



// COMMAND ----------

def exportToBlobStorage (type_of_ETL:Int): String = { 

import com.databricks.WorkflowException
import java.io.FileNotFoundException

var Result = "Failure" 
val partition_field = "partition_name"
val export_format = "com.databricks.spark.csv"
val export_delimiter = Character.toString(7.toChar)

var readPath_ETL = if (type_of_ETL == 0) readPath else if (type_of_ETL == 1) readPath_GBS else null
var writePath_ETL= if (type_of_ETL == 0) writePath_СAP else if (type_of_ETL == 1) writePath_GBS else null

try {
  sqldf
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