// Databricks notebook source
val storage_account_name = "staeeprodbigdataml2c"
val storage_account_access_key = "EHYumrwso4XLSUHpvLptI33z7mumiZwZOErjrlP8FiW51Bb6NS2PaWJsqW9hsMttbZizgQjUexFZfZDBQJebYw=="
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

// COMMAND ----------

val readPath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/test_res.csv"
val writePath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/Result" 
//val result =  "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/Transformation/Weather"
val SearchPath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/Transformation/Weather/"
val file_type = "csv"
val fname = "weather_ru.csv"

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

val city_list = spark.sql("select distinct city from Plant_City")

// COMMAND ----------

val city_plant = spark.sql("select distinct city, plant from Plant_City")
city_plant.createOrReplaceTempView("city_plant")

// COMMAND ----------

// dbutils.notebook.run("test2", 0, Map("city" -> "Rivne"))
// dbutils.notebook.exit("Success")

// COMMAND ----------

city_list.collect.foreach {
 (x) => { val status = dbutils.notebook.run("weather_dark_sky", 
                             0, 
                             Map(
                                 "City" -> x.toString().replace("[", "").replace("]", ""),
                                 "Year" -> "2018",
                                 "API_Key" -> "d4b20ef8a9ad8bcf2449be822fba03a4",
                                 "CONTAINER_NAME"  -> "prod"     
                                )
                            ) 
                println(status)
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

val schema_string = "time,apparentTemperatureMax,cloudCover,humidity,windSpeed,city,date"
val schema_rdd = StructType(schema_string.split(",").map(fieldName => StructField(fieldName, StringType, true)) )
var empty_df = sqlContext.createDataFrame(sc.emptyRDD[Row], schema_rdd)

val name : String = "2019"   
val file_list : Seq[String] = dbutils.fs.ls(SearchPath).map(_.path).filter(_.contains(name))
for (file <- file_list)  {
  val file_list : Seq[String] = dbutils.fs.ls(file).map(_.path).filter(_.contains(name)) 
  var city = file_list(0).replace(file, "").replace("weather_", "").replace("_"+name + ".csv", "") 
  var file_location = file + file_list(0).replace(file, "")
  var dff = spark.read.format(file_type).option("delimiter", ";").option("header", "true").load(file_location).select("time", "apparentTemperatureMax", "cloudCover", "humidity", "windSpeed") 
  var empty_df_city_date = dff.withColumn("city", lit(city)).withColumn("date", expr("from_unixtime(time ,'YYYY-MM-dd')"))
  
  empty_df = empty_df.union(empty_df_city_date)
}

//empty_df.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").save(readPath)

// val name_ : String = "part-00000"  
// val file_list_ : Seq[String] = dbutils.fs.ls(readPath).map(_.path).filter(_.contains(name_))
// val read_name = if (file_list_.length >= 1 ) file_list_(0).replace(readPath + "/", "") 
// dbutils.fs.mv(readPath+"/"+read_name , result+"/"+fname)   
// dbutils.fs.rm(readPath , recurse = true) 
empty_df.createOrReplaceTempView("main")


// COMMAND ----------

val sql_qury = """
select distinct m.city, c.plant, cl.weekid calendar_yearweek, date time, m.apparentTemperatureMax, m.cloudCover, m.humidity, m.windSpeed
from main m left join city_plant c  on  m.city = c.city left join calendar cl on cast(m.date as timestamp)  = cl.DayName
"""

// COMMAND ----------

import com.databricks.WorkflowException
import java.io.FileNotFoundException

var Result = "Failure"   

try {
 
val sql_df = spark.sql(sql_qury)
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

dbutils.notebook.exit(Result)

