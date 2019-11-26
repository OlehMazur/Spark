// Databricks notebook source
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

val type_of_ETL: Int = 1

// COMMAND ----------

//constants (Batlika)

// COMMAND ----------

val readPath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/test_res.csv"
val writePath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/Result" //ETL/Result //etl_fbkp
val writePath_СAP = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/export_to_CAP"
val fname = "events_ru.csv"

// COMMAND ----------

//constants (CAP)

// COMMAND ----------

val writePath_GBS = "wasbs://dcd@prdcbwesa01.blob.core.windows.net/RU" 
val readPath_GBS = "wasbs://dcd@prdcbwesa01.blob.core.windows.net/RU/ru_tmp" 

// COMMAND ----------

//file with events

// COMMAND ----------

val formatted_file_location = "wasbs://formated@staeeprodbigdataml2c.blob.core.windows.net/"
val file_events = "events_ru_final+UEFA20.csv"

// COMMAND ----------

val file_location = formatted_file_location + file_events
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("events")

// COMMAND ----------

val sqldf = spark.sql(""" 
select calendar_yearmonth_end partition_name,  *  from events 
""")

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
   var fname = "EVENTS_" + partition_name + "_RU_DCD" + ".csv"
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
if (type_of_ETL == 0) exportToBlobStorage(0) 
else if (type_of_ETL == 1) exportToBlobStorage(1)
else if (type_of_ETL == 2) {exportToBlobStorage(0); exportToBlobStorage(1) }
else "Unexpected parameter"

dbutils.notebook.exit(Result)