// Databricks notebook source
//Type of ETL: 0 (only Baltika ) 1 (only CAP) 2 (Both)

// COMMAND ----------

val type_of_ETL: Int = 1

// COMMAND ----------

//Type of data extraction: 0 (full) , 1 (incremental )

// COMMAND ----------

val type_of_data_extract: Int = 1

// COMMAND ----------

// The range of month in case of incremental loading (only if type_of_data_extract = 1 !!! )

// COMMAND ----------

val num_of_days_before_current_date: Int = 30
//val num_of_days_after_current_date: Int = 30   

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

//constants (Batlika)

// COMMAND ----------

val readPath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/test_res.csv"
val writePath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/Result" //ETL/Result //etl_fbkp
val writePath_СAP = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/export_to_CAP"
val fname_direct = "events_direct_ru.csv"
val fname_indirect = "events_indirect_ru.csv"

// COMMAND ----------

//constants (CAP)

// COMMAND ----------

val writePath_GBS = "wasbs://dcd@prdcbwesa01.blob.core.windows.net/RU" 
val readPath_GBS = "wasbs://dcd@prdcbwesa01.blob.core.windows.net/RU/ru_tmp" 

// COMMAND ----------

// file with events

// COMMAND ----------

//files location should be 
//prod\ETL\manually

// COMMAND ----------

// val formatted_file_location_direct = "wasbs://formated@staeeprodbigdataml2c.blob.core.windows.net/"
// val file_events_direct = "events_ru_final+UEFA20.csv"
// val formatted_file_location_indirect = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/"
// val file_events_indirect = "/Weather_Events_files/events_indirect_new.csv"

// COMMAND ----------

val file_location_direct_indirect = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/manually/"
val file_events_direct = "events_direct_ru.csv"
val file_events_indirect = "events_indirect_ru.csv"

// COMMAND ----------

val file_location = file_location_direct_indirect + file_events_direct
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("events")

// COMMAND ----------

val file_location = file_location_direct_indirect + file_events_indirect
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("events_indirect")

// COMMAND ----------

//PGO_City.csv

// COMMAND ----------

// val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/PGO_City.csv"
// val file_type = "csv"
// val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
// df.createOrReplaceTempView("pgo_city")

// COMMAND ----------

val query_direct_full = """
select 
calendar_yearmonth_end partition_name,  *  
from events 
"""

// COMMAND ----------

val query_direct_incremental = """
select 
calendar_yearmonth_end partition_name,  *  
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

//export to CAP

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
   var fname = "EVENTSDIRECT_" + partition_name + "_RU_DCD" + ".csv"
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

//export to ETL\Result

// COMMAND ----------


def exportToBlobStorage_Baltika_direct: String = { 

import com.databricks.WorkflowException
import java.io.FileNotFoundException
  
val fname = fname_direct
var Result = "Failure"   

try {
sqldf_direct_full.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").save(readPath)

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
  
  Result

}
//dbutils.notebook.exit(Result)



// COMMAND ----------

//indirect Events

// COMMAND ----------

// val sqldf = spark.sql(""" 
// select distinct
// e.rank,
// e.local_rank,
// e.calendar_yearweek_first_seen,
// e.calendar_yearweek_start,
// e.calendar_yearweek_end,
// e.calendar_yearmonth_first_seen,
// e.calendar_yearmonth_start,
// e.calendar_yearmonth_end,
// e.major_sport_events,
// e.other_sport_events,
// e.festival,
// e.holiday,
// e.world_cup,
// plant_info.pgo as plant,
// e.city
// from events e left join pgo_city plant_info on e.city = plant_info.City_ru
// """)

// COMMAND ----------

//export to ETL\Result

// COMMAND ----------

val query_indirect_full = """ 
select calendar_yearmonth_end partition_name,  *  from events_indirect 
"""

// COMMAND ----------

val query_indirect_incremental = """ 
select calendar_yearmonth_end partition_name,  *  from events_indirect 
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

//Export to ETL\Result

// COMMAND ----------

def exportToBlobStorage_Baltika_indirect: String = { 

import com.databricks.WorkflowException
import java.io.FileNotFoundException
  
val fname = fname_indirect
var Result = "Failure"   

try {
sqldf_indirect_full.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").save(readPath)

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
  
  Result

}
//dbutils.notebook.exit(Result)


// COMMAND ----------

// Export to CAP 

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
   var fname = "EVENTSINDIRECT_" + partition_name + "_RU_DCD" + ".csv"
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
if (type_of_ETL == 0) { if (exportToBlobStorage_Baltika_direct == "Success" && exportToBlobStorage_Baltika_indirect == "Success" &&  exportToBlobStorage_direct (0) == "Success" && exportToBlobStorage_indirect(0) =="Success" )  "Success" else "Failure"  }
else if (type_of_ETL == 1) { if (exportToBlobStorage_direct (1) == "Success" && exportToBlobStorage_indirect(1) =="Success") "Success" else  "Failure"}
else if (type_of_ETL == 2) { if (exportToBlobStorage_Baltika_direct == "Success" && exportToBlobStorage_Baltika_indirect == "Success" &&  exportToBlobStorage_direct (0) == "Success" && exportToBlobStorage_indirect(0) =="Success" && exportToBlobStorage_direct (1) == "Success" && exportToBlobStorage_indirect(1) == "Success")   "Success" else "Failure" }
else "Unexpected parameter"

dbutils.notebook.exit(Result)