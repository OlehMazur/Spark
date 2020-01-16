// Databricks notebook source
// MAGIC %run /Users/o_mazur@carlsberg.ua/dcd_etl_functions

// COMMAND ----------

//Configuration (Baltika)
val storage_account_name = "staeeprodbigdataml2c"
val storage_account_access_key = "EHYumrwso4XLSUHpvLptI33z7mumiZwZOErjrlP8FiW51Bb6NS2PaWJsqW9hsMttbZizgQjUexFZfZDBQJebYw=="
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

// COMMAND ----------

val job = "dcd_notebook_workflow_indirect"
val readPath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/tmp/" + job

// COMMAND ----------

//Init log DB

// COMMAND ----------

init_log_table(job)

// COMMAND ----------

val start_time = getTime(LocalDateTime.now)

//is_bal
println("is_bal.scala notebook has been started...")
var start_ms = getTime(LocalDateTime.now)
val status_ms = dbutils.notebook.run("is_bal", timeoutSeconds = 0)
var end_ms = getTime(LocalDateTime.now)
println("status for is_bal.scala notebook : " + status_ms)
println("elapsed time " + start_ms.until(end_ms, ChronoUnit.MINUTES) + " minutes" )


//if_bal
var start_hfa = getTime(LocalDateTime.now)
val status_hfa = {
  if (status_ms == "Success") 
  {
    println("if_bal.scala notebook has been started...")
    dbutils.notebook.run("if_bal", timeoutSeconds = 0)
  }
  else println("Error from the previous step !")
}
var end_hfa = getTime(LocalDateTime.now)
println("status for if_bal.scala notebook : " + status_hfa)
println("elapsed time " + start_hfa.until(end_hfa, ChronoUnit.MINUTES) + " minutes" )

val end_time = getTime(LocalDateTime.now)


val job_status = 
if (status_ms == "Success" && status_hfa == "Success" ) "Success"
else if (status_ms == "Skipped" && status_hfa == "Skipped"    ) "Skipped"
else "Failure"

val file_name = "log_" + job + "_" +  LocalDateTime.now.toString().replace(":", "-") + "_" +job_status +  ".csv"

val duration = start_time.until(end_time, ChronoUnit.SECONDS)
//save to log
save_to_log_job_result(job, job_status, start_time, end_time, duration, job_status, file_name)

//export log file 
export_log_to_csv(file_name, readPath, job)

//sending email
if (send_email_or_not == 1) sendEmail(job_status + " - " + "Azure Databricks job " + job + " has finished its run", "The log is ready for view, blob storage folder: prod/ETL/Log/" + file_name )

if (job_status == "Failure") throw new Exception("the job has finished its run with status Failure")

