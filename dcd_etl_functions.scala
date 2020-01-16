// Databricks notebook source
val type_of_ETL: Int = 2 //Type of ETL: 0 (only Baltika ) 1 (only CAP) 2 (Both)

// COMMAND ----------

val type_of_data_extract: Int = 0 //Type of data extraction: 0 (full) , 1 (incremental )

// COMMAND ----------

val num_of_days_before_current_date: Int = 30 // The range of month in case of incremental loading (only if type_of_data_extract = 1 !!! )
//val num_of_days_after_current_date: Int = 30   

// COMMAND ----------

val is_logic_changed = 0 // the parameter should be set to 1 if some logic was changed and full re-load is needed

// COMMAND ----------

val send_email_or_not = 1 // send an email after job has finished its run: 1 - yes, 0 - no

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

//constants (Baltika)

// COMMAND ----------

val log_location_path = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/Log/"
val writePath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/Result" 
//val writePath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/etl_fbkp" //Test
val writePath_СAP = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/export_to_CAP"
//val writePath_СAP = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/prod_fbkp" //Test
val source_file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/"
val write_path_tmp = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/tmp/preprocessing"

// COMMAND ----------

//constants (CAP)

// COMMAND ----------

val writePath_GBS = "wasbs://dcd@prdcbwesa01.blob.core.windows.net/RU" 

// COMMAND ----------

//import libraries

// COMMAND ----------

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.text.SimpleDateFormat
import java.util.Date
import com.databricks.WorkflowException
import java.io.FileNotFoundException
import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.StorageException
import com.microsoft.azure.storage.blob.CloudBlob
import com.microsoft.azure.storage.blob.CloudBlobClient
import com.microsoft.azure.storage.blob.CloudBlobContainer
import org.scalatest.Assertions._
import java.util.Properties
import javax.mail.{Message, Session}
import javax.mail.internet.{InternetAddress, MimeMessage}
import scala.io.Source

// COMMAND ----------

//util variables

// COMMAND ----------

val formatter = new SimpleDateFormat("yyyy-MM-dd")
val current_date = formatter.format(new java.util.Date())
val time_measure = ChronoUnit.SECONDS
val host = "smtp.gmail.com"
val port = "587" 
val recipients = Array("Oleg.Mazur@carlsberg.ua", "larionov_sg@baltika.com", "prokopev_da@baltika.com")
val username = "databricksetlbot@gmail.com"
val password = dbutils.secrets.get("send_email", "EMAIL_SMTP_PASSWORD") 

// COMMAND ----------

 //functions

// COMMAND ----------

def getTime(a: => LocalDateTime) = a

// COMMAND ----------

def get_last_modified_date_for_blob(blobName: String ) : String  = {

  val StorageConnectionStringTemplate = "DefaultEndpointsProtocol=https;" + 
          "DefaultEndpointsProtocol=https;" +
          "AccountName=%s;" +
          "AccountKey=%s"
  val  accountName = storage_account_name
  val accountKey = storage_account_access_key
  val containerName = "prod"
  val storageConnectionString = String.format(StorageConnectionStringTemplate, accountName, accountKey)
  val storageAccount: CloudStorageAccount  = CloudStorageAccount.parse(storageConnectionString)
  val client = storageAccount.createCloudBlobClient()
  val container: CloudBlobContainer = client.getContainerReference(containerName)
  val blob: CloudBlob = container.getBlobReferenceFromServer(blobName)
  val lastModifiedDate: Date = blob.getProperties().getLastModified()
  val formatter = new SimpleDateFormat("yyyy-MM-dd")
  val date = formatter.format(lastModifiedDate)
  date
}

// COMMAND ----------

def init_log_table (job:String) = {

  var query = ""
  
  query = "use etl_info"
  spark.sql(query)
  
  query = "drop table if exists " + job
  spark.sql(query)
  
  query = """ 
  create table if not exists """ + job + """ (
    event string,
    job string,
    notebook string,
    start_time timestamp,
    end_time timestamp,
    duration bigint,  
    notebook_status string,
    count_of_records bigint, 
    file_name string,
    last_modified_date string   
    )
  USING delta
  """ 
 spark.sql(query)  
} 


// COMMAND ----------

def save_to_log_last_step (job: String, notebook:String,  notebook_status: String, notebook_start_time:java.time.LocalDateTime) = {
  val event = "the notebook has finished its run"
  val start_time =  notebook_start_time
  val end_time = LocalDateTime.now
  val duration = notebook_start_time.until(end_time, time_measure)
  val count_of_records = 0L
  val file_name = ""
  val last_modified_date = ""
  val log_message = s"insert into " +job + $" values ('$event','$job', '$notebook', '$start_time', '$end_time', $duration, '$notebook_status', $count_of_records, '$file_name', '$last_modified_date'  )"
  spark.sql(log_message) 
}  

// COMMAND ----------

def save_to_log_first_step (job: String, notebook:String) = {
  val event = "the notebook has been started"
  val start_time =  LocalDateTime.now
  val end_time = LocalDateTime.now
  val duration = start_time.until(end_time, time_measure)
  val notebook_status = "running"
  val count_of_records = 0L
  val file_name = ""
  val last_modified_date =""
  val log_message = s"insert into " + job + $" values ('$event','$job', '$notebook', '$start_time', '$end_time', $duration, '$notebook_status', $count_of_records, '$file_name', '$last_modified_date'  )"
  spark.sql(log_message)
  } 

// COMMAND ----------

def save_to_log_parameters_value (job: String, notebook:String, parameter_name:String, parameter_value: Int) = {
  val start_time = LocalDateTime.now
  val event = "parameter " + parameter_name  + " set to " + parameter_value.toString()
  val end_time = LocalDateTime.now
  val duration = start_time.until(end_time, time_measure)
  val notebook_status = "running"
  val count_of_records = 0L
  val file_name = ""
  val last_modified_date =""
  val log_message =  s" insert into " + job + $" values ('$event','$job', '$notebook', '$start_time', '$end_time', $duration, '$notebook_status', $count_of_records, '$file_name', '$last_modified_date')"
  spark.sql(log_message)
}  

// COMMAND ----------

def save_to_log_file_processing (job: String, notebook:String, start_time: java.time.LocalDateTime, df: org.apache.spark.sql.DataFrame, file_name:String )  =  {
  val end_time = LocalDateTime.now
  val duration = start_time.until(end_time, time_measure)
  val notebook_status = "running"
  val event = "source file processing" 
  val count_of_records =  df.count
  val last_modified_date = get_last_modified_date_for_blob(file_name)
  val log_message = s" insert into " + job + $" values ('$event','$job', '$notebook', '$start_time', '$end_time', $duration, '$notebook_status', $count_of_records, '$file_name', '$last_modified_date' )"
  spark.sql(log_message)
} 

// COMMAND ----------

def save_to_log_if_source_is_empty (job: String, notebook:String, start_time: java.time.LocalDateTime, file_name:String) =  {
 val event =  "the data source is empty, please redo the extraction"
 val notebook_status = "Exception" 
 val end_time = LocalDateTime.now 
 val duration = start_time.until(end_time, time_measure) 
 val count_of_records = 0L
 val last_modified_date =get_last_modified_date_for_blob(file_name)
 val log_message = s" insert into " +  job +  $" values  ('$event','$job', '$notebook', '$start_time', '$end_time', $duration, '$notebook_status', $count_of_records, '$file_name', '$last_modified_date' )"
 spark.sql(log_message) 
}  

// COMMAND ----------

def save_to_log_if_source_is_not_updated (job: String, notebook:String, start_time: java.time.LocalDateTime, file_name:String) {
val end_time = LocalDateTime.now
val event =  "the data source file is not up to date, please update the file"
val notebook_status = "Exception" 
val duration = start_time.until(end_time, time_measure)   
val count_of_records = 0L 
val last_modified_date = get_last_modified_date_for_blob(file_name)  
val log_message = s" insert into " +  job + $" values  ('$event','$job', '$notebook', '$start_time', '$end_time', $duration, '$notebook_status', $count_of_records, '$file_name', '$last_modified_date' )"
spark.sql(log_message) 
} 

// COMMAND ----------

def exportToBlobStorage_CAP (job: String, notebook:String, type_of_ETL:Int, df: org.apache.spark.sql.DataFrame, file_name_start_with:String, readPath:String, readPath_GBS:String): String = { 

var Result = "Failure" 
val partition_field = "partition_name"
val export_format = "com.databricks.spark.csv"
val export_delimiter = Character.toString(7.toChar)

var readPath_ETL = if (type_of_ETL == 0) readPath else if (type_of_ETL == 1) readPath_GBS else null
var writePath_ETL= if (type_of_ETL == 0) writePath_СAP else if (type_of_ETL == 1) writePath_GBS else null

try {
  
  df
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
  
  var event = "loading to CAP has been started"
  var start_time = LocalDateTime.now 
  var end_time = LocalDateTime.now
  var file_name = file_name_start_with
  var notebook_status = "running"
  var log_message =  s"insert into " +  job +  $" values ('$event','$job', '$notebook', '$start_time', '$end_time', 0, 'running', 0,'$file_name' , '') "
  
  for (path <- path_list) {
   start_time = LocalDateTime.now 
    
   var partition_name = path.replace(readPath_ETL + "/" + partition_field + "=", "").replace("/", "")
   var file_list : Seq[String] = dbutils.fs.ls(path).map(_.path).filter(_.contains(name)) 
   var read_name =  if (file_list.length >= 1 ) file_list(0).replace(path + "/", "") 
  // var fname = "Indirect_RU_cp1251_" + partition_name + ".csv" 
   var fname = file_name_start_with + partition_name + "_RU_DCD" + ".csv"
   dbutils.fs.mv(read_name.toString , writePath_ETL+"/"+fname) 
   
   end_time = LocalDateTime.now
   //save log
   var duration = start_time.until(end_time, time_measure) 
   event = "export to blob (CAP)"
  
   var count_of_records = spark.read.format("csv").option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(writePath_ETL+"/"+fname).count   
  
   file_name = fname
   var last_modified_date = ""
   log_message = log_message +  s" ,  ('$event','$job', '$notebook', '$start_time', '$end_time', $duration, '$notebook_status', $count_of_records, '$file_name', '$last_modified_date' ) " 
   //spark.sql(log_message)
    }
  spark.sql(log_message)
  
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

def exportToBlobStorage_CAP_hole_file (job: String, notebook:String, type_of_ETL:Int, df: org.apache.spark.sql.DataFrame, file_name_start_with:String, readPath:String, readPath_GBS:String): String = { 

var Result = "Failure" 
val export_format = "com.databricks.spark.csv"
val export_delimiter = Character.toString(7.toChar)

var readPath_ETL = if (type_of_ETL == 0) readPath else if (type_of_ETL == 1) readPath_GBS else null
var writePath_ETL= if (type_of_ETL == 0) writePath_СAP else if (type_of_ETL == 1) writePath_GBS else null

try {
  
  val start_time = LocalDateTime.now 
  
  df
  .coalesce(1)
  .write.mode("overwrite")
  .format(export_format)
  .option("header", "true")
  .option("inferSchema", "true")
  .option("delimiter", export_delimiter)
  //.option("encoding", "cp1251")
  .save(readPath_ETL)
  
  val name : String = "part-00000"  
  val file_list : Seq[String] = dbutils.fs.ls(readPath_ETL).map(_.path).filter(_.contains(name))
  val read_name = if (file_list.length >= 1 ) file_list(0).replace(readPath_ETL + "/", "")
  val row_count = spark.read.format("csv").option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_list(0)).count   
  val fname = file_name_start_with + "_RU_DCD" + ".csv"
  dbutils.fs.mv(readPath_ETL+"/"+read_name , writePath_ETL+"/"+fname)   
  dbutils.fs.rm(readPath , recurse = true) 
  if (row_count > 0) Result = "Success" else println("The file " +writePath+"/"+fname + " is empty !" )

  val end_time = LocalDateTime.now
  //save log
  val duration = start_time.until(end_time, time_measure) 
  val event = "export to blob (CAP)"
  val count_of_records =  row_count
  val file_name = fname
  val last_modified_date = ""  
  var notebook_status = "running"  
  val log_message =  s" insert into " +  job +  $" values ('$event','$job', '$notebook', '$start_time', '$end_time', $duration, '$notebook_status', $count_of_records, '$file_name', '$last_modified_date')" 
  spark.sql(log_message) 
  
  } 
catch {
    case e:FileNotFoundException => println("Error, " + e)
    case e:WorkflowException  => println("Error, " + e)
  }

  Result
}



// COMMAND ----------

def exportToBlobStorage_Baltika (job: String, notebook:String, fname:String, df: org.apache.spark.sql.DataFrame, readPath:String ) : String = { 
  
var Result = "Failure"   

try {

val start_time = LocalDateTime.now 
  
df.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").save(readPath)

val name : String = "part-00000"  
val file_list : Seq[String] = dbutils.fs.ls(readPath).map(_.path).filter(_.contains(name))
val read_name = if (file_list.length >= 1 ) file_list(0).replace(readPath + "/", "")
val row_count = spark.read.format("csv").option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_list(0)).count   
dbutils.fs.mv(readPath+"/"+read_name , writePath+"/"+fname)   
dbutils.fs.rm(readPath , recurse = true) 
if (row_count > 0) Result = "Success" else println("The file " +writePath+"/"+fname + " is empty !" )
  
val end_time = LocalDateTime.now
//save log
val duration = start_time.until(end_time, time_measure) 
val event = "export to blob"
val count_of_records =  row_count
val file_name = fname
val last_modified_date = ""  
var notebook_status = "running"  
val log_message =  s" insert into " +  job + $" values ('$event','$job', '$notebook', '$start_time', '$end_time', $duration, '$notebook_status', $count_of_records, '$file_name', '$last_modified_date')" 
spark.sql(log_message) 
  
} 
catch {
  case e:FileNotFoundException => println("Error, " + e)
  case e:WorkflowException  => println("Error, " + e)
}
  
  Result

}



// COMMAND ----------

def save_to_log_job_result (job: String, notebook_status:String, start_time: java.time.LocalDateTime, end_time: java.time.LocalDateTime, duration: Long, job_status:String, file_name:String) = {

val event = "job has finished its run, log is ready for view"
val notebook = ""
val count_of_records = 0L
val last_modified_date = ""  

  //safe to log table
val query_event =  s"insert into " + job + $" values ('$event','$job', '$notebook', '$start_time', '$end_time', $duration, '$job_status', $count_of_records, '$file_name', '$last_modified_date'  )"
spark.sql(query_event)

}

// COMMAND ----------

def export_log_to_csv (file_name:String, readPath:String, job:String) = {
  
val query =  "select * from " +job +  " order by end_time desc"
val log_df = spark.sql(query)

var Result = "Failure"   

try {
log_df.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").save(readPath)

val name : String = "part-00000"  
val file_list : Seq[String] = dbutils.fs.ls(readPath).map(_.path).filter(_.contains(name))
val read_name = if (file_list.length >= 1 ) file_list(0).replace(readPath + "/", "")
val row_count = spark.read.format("csv").option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_list(0)).count   
dbutils.fs.mv(readPath+"/"+read_name , log_location_path +file_name)   
dbutils.fs.rm(readPath , recurse = true) 
if (row_count > 0) Result = "Success" else println("The file " +log_location_path+file_name + " is empty !" )
} 
catch {
  case e:FileNotFoundException => println("Error, " + e)
  case e:WorkflowException  => println("Error, " + e)
}
  
}

// COMMAND ----------

def Test_Number_of_Rows (job: String, notebook:String, sqldf: org.apache.spark.sql.DataFrame , file_name:String ):String = {
  var test_result = "SUCCEEDED"
  val count_of_records_df = sqldf.count
  try {
    assert(0 < count_of_records_df)
  }
  catch {
    case _: org.scalatest.exceptions.TestFailedException => test_result = "FAILED"
  }
 
  
  //save to log 
  val event =  "Test: number of rows > 0 : " +  test_result
  val start_time =  LocalDateTime.now
  val end_time = LocalDateTime.now
  val duration = start_time.until(end_time, time_measure)
  val notebook_status = if (test_result == "SUCCEEDED") "Success" else "Exception"
  val count_of_records = count_of_records_df
  val last_modified_date =""
  val log_message = s"insert into " + job + $" values ('$event','$job', '$notebook', '$start_time', '$end_time', $duration, '$notebook_status', $count_of_records, '$file_name', '$last_modified_date'  )"
  spark.sql(log_message)
  
  test_result
} 

// COMMAND ----------

def Test_Is_incomplete_period (job: String, notebook:String,sqldf: org.apache.spark.sql.DataFrame, file_name:String):String = {
  
  var test_result = "SUCCEEDED"
  
  sqldf.createOrReplaceTempView("sqldf_table")
  val query = """ 
  select * 
  from (
  select  calendar_yearweek -  LAG(calendar_yearweek, 1,  null) OVER (ORDER BY calendar_yearweek) AS diff
  from sqldf_table
  group by calendar_yearweek
  )tab
  where (diff > 1 and diff < 48)
  """
  val sqldf2_result = spark.sql(query)
  
  
  try {
    assert(sqldf2_result.isEmpty)
  }
  catch {
    case _: org.scalatest.exceptions.TestFailedException => test_result = "FAILED"
  }
  
  
  //save to log 
  val event =  "Test: if all the calendar_yearweeks in the set : " +  test_result
  val start_time =  LocalDateTime.now
  val end_time = LocalDateTime.now
  val duration = start_time.until(end_time, time_measure)
  val notebook_status = if (test_result == "SUCCEEDED") "Success" else "Exception"
  val count_of_records = sqldf.count
  val last_modified_date =""
  val log_message = s"insert into " + job + $" values ('$event','$job', '$notebook', '$start_time', '$end_time', $duration, '$notebook_status', $count_of_records, '$file_name', '$last_modified_date'  )"
  spark.sql(log_message)
  
 test_result 
} 

// COMMAND ----------

def Test_Missing_Filed (job: String, notebook:String, sqldf: org.apache.spark.sql.DataFrame, missing_field:String, file_name:String):String = {
  
  var test_result = "SUCCEEDED"
  
  sqldf.createOrReplaceTempView("sqldf_table")
  val query = """ 
  select  *
  from sqldf_table
  where """ + missing_field + """ is null
 
  """
  val sqldf2_result = spark.sql(query)
  
  
  try {
    assert(sqldf2_result.isEmpty)
  }
  catch {
    case _: org.scalatest.exceptions.TestFailedException => test_result = "FAILED"
  }
 
  
  //save to log 
  val event =  "Test: " + missing_field +  " column contains NULL : " +  test_result
  val start_time =  LocalDateTime.now
  val end_time = LocalDateTime.now
  val duration = start_time.until(end_time, time_measure)
  val notebook_status = if (test_result == "SUCCEEDED") "Success" else "Exception"
  val count_of_records = sqldf.count
  val last_modified_date =""
  val log_message = s"insert into " + job + $" values ('$event','$job', '$notebook', '$start_time', '$end_time', $duration, '$notebook_status', $count_of_records, '$file_name', '$last_modified_date'  )"
  spark.sql(log_message)
  
  test_result
  
} 

// COMMAND ----------

def Test_Values_In_Correct_Range (job: String, notebook:String, sqldf: org.apache.spark.sql.DataFrame, period: String, field:String, min_value:BigInt, max_value:BigInt, file_name:String ):String = {
  
  //period should be: monthly or weekly
  var query_period = if (period == "monthly") "calendar_yearmonth" else if (period == "weekly") "calendar_yearweek" else null
  
  var test_result = "SUCCEEDED"
  
  sqldf.createOrReplaceTempView("sqldf_table")
  val query = """ 
  select  sum( + """ + field + """) value
  from sqldf_table
  group by """ + query_period + """
  having (value < """ + min_value + """ or  value > """ +  max_value + """)
  """
  val sqldf2_result = spark.sql(query)
  
  
  try {
    assert(sqldf2_result.isEmpty)
  }
  catch {
    case _: org.scalatest.exceptions.TestFailedException => test_result = "FAILED"
  }
  
 
  //save to log 
  val event =  "Test: if the " + period + " sum of " + field.replace("`", "") + " is within the range [" + min_value + ", " + max_value + "] : "  +  test_result
  val start_time =  LocalDateTime.now
  val end_time = LocalDateTime.now
  val duration = start_time.until(end_time, time_measure)
  val notebook_status = if (test_result == "SUCCEEDED") "Success" else "Warning"
  val count_of_records = sqldf.count
  val last_modified_date =""
  val log_message = s"insert into " + job + $" values ('$event','$job', '$notebook', '$start_time', '$end_time', $duration, '$notebook_status', $count_of_records, '$file_name', '$last_modified_date'  )"
  spark.sql(log_message)
  
  test_result
  
} 

// COMMAND ----------

def save_to_log_start_result_testing (job: String, notebook:String) {
  val event = "the testing process for the result has been started"
  val start_time =  LocalDateTime.now
  val end_time = LocalDateTime.now
  val duration = start_time.until(end_time, time_measure)
  val notebook_status = "running"
  val count_of_records = 0L
  val file_name = ""
  val last_modified_date =""
  val log_message = s"insert into " + job + $" values ('$event','$job', '$notebook', '$start_time', '$end_time', $duration, '$notebook_status', $count_of_records, '$file_name', '$last_modified_date'  )"
  spark.sql(log_message)
} 

// COMMAND ----------

def exportToBlobStorage_Baltika_Result_Before_Testing (job: String, notebook:String, fname:String, df: org.apache.spark.sql.DataFrame, readPath:String ) : String = { 
  
var Result = "Failure"   

try {

val start_time = LocalDateTime.now 
  
df.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").save(readPath)

val name : String = "part-00000"  
val file_list : Seq[String] = dbutils.fs.ls(readPath).map(_.path).filter(_.contains(name))
val read_name = if (file_list.length >= 1 ) file_list(0).replace(readPath + "/", "")
val row_count = spark.read.format("csv").option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_list(0)).count   
dbutils.fs.mv(readPath+"/"+read_name , write_path_tmp+"/"+fname)   
dbutils.fs.rm(readPath , recurse = true) 
if (row_count > 0) Result = "Success" else println("The file " +write_path_tmp+"/"+fname + " is empty !" )
  
val end_time = LocalDateTime.now
//save log
val duration = start_time.until(end_time, time_measure) 
val event = "preliminary result file has been saved"
val count_of_records =  row_count
val file_name = fname
val last_modified_date = ""  
var notebook_status = "running"  
val log_message =  s" insert into " +  job + $" values ('$event','$job', '$notebook', '$start_time', '$end_time', $duration, '$notebook_status', $count_of_records, '$file_name', '$last_modified_date')" 
spark.sql(log_message) 
  
} 
catch {
  case e:FileNotFoundException => println("Error, " + e)
  case e:WorkflowException  => println("Error, " + e)
}
  
  Result

}



// COMMAND ----------

def load_preliminary_result (source_file_name: String): org.apache.spark.sql.DataFrame = {
  val source_file_location = write_path_tmp
  val file_location = source_file_location + "/" + source_file_name
  val file_type = "csv"
  val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
  df
}

// COMMAND ----------

def sendEmail(mailSubject: String, mailContent: String) = {
    val properties = new Properties()
    properties.put("mail.smtp.port", port)
    properties.put("mail.smtp.auth", "true")
    properties.put("mail.smtp.starttls.enable", "true")

    val session = Session.getDefaultInstance(properties, null)
    val message = new MimeMessage(session)
    
    recipients.foreach{ (x)=>message.addRecipient(Message.RecipientType.TO, new InternetAddress(x))  } 
  
    message.setSubject(mailSubject)
    message.setContent(mailContent, "text/html")

    val transport = session.getTransport("smtp")
    transport.connect(host, username, password)
    transport.sendMessage(message, message.getAllRecipients)
}