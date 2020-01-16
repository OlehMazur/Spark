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

val job = "dcd_notebook_workflow_df"
val readPath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/tmp/" + job

// COMMAND ----------

//Init log DB

// COMMAND ----------

init_log_table (job)

// COMMAND ----------

//df_bal
println("df_bal.scala notebook has been started...")
val start_time = getTime(LocalDateTime.now)
val notebook_status = dbutils.notebook.run("df_bal", timeoutSeconds = 0)
val end_time = getTime(LocalDateTime.now)
val duration = start_time.until(end_time, ChronoUnit.SECONDS)
println("status for df_bal.scala notebook : " + notebook_status)
println("elapsed time " + duration + " seconds" )

val job_status = 
if (notebook_status == "Success"  ) "Success"
else if (notebook_status == "Skipped"  ) "Skipped"
else "Failure"

val file_name = "log_" + job + "_" +  LocalDateTime.now.toString().replace(":", "-") + "_" +job_status +  ".csv"

//save to log
save_to_log_job_result(job, notebook_status, start_time, end_time, duration, job_status, file_name)

//export log file 
export_log_to_csv(file_name, readPath,job)

if (job_status == "Failure") throw new Exception("the job has finished its run with status Failure")
