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

val job = "dcd_notebook_workflow_hfa_All"
val readPath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/tmp/" + job

// COMMAND ----------

//Init log DB

// COMMAND ----------

init_log_table(job)

// COMMAND ----------

val start_time = getTime(LocalDateTime.now)

//md_sku
println("md_sku.scala notebook has been started...")
var start_ms = getTime(LocalDateTime.now)
val status_ms = dbutils.notebook.run("md_sku", timeoutSeconds = 0)
var end_ms = getTime(LocalDateTime.now)
println("status for md_sku.scala notebook : " + status_ms)
println("elapsed time " + start_ms.until(end_ms, ChronoUnit.MINUTES) + " minutes" )

//sell_in_all 
println("sell_in_all.scala notebook has been started...")
var start_si = getTime(LocalDateTime.now)
//val status_si = dbutils.notebook.run("sell_in_all", timeoutSeconds = 0)
val status_si = dbutils.notebook.run("sell_in_all_temporary", timeoutSeconds = 0)
var end_si = getTime(LocalDateTime.now)
println("status for sell_in_all.scala notebook : " + status_si)
println("elapsed time " + start_si.until(end_si, ChronoUnit.MINUTES) + " minutes" )

//hfa_all
var start_hfa = getTime(LocalDateTime.now)
val status_hfa = {
  if (status_si == "Success") 
  {
    println("hfa_all.scala notebook has been started...")
    //dbutils.notebook.run("hfa_all", timeoutSeconds = 0)
    dbutils.notebook.run("hfa_all_temporary", timeoutSeconds = 0)
  }
  else println("Error from the previous step !")
}
var end_hfa = getTime(LocalDateTime.now)

println("status for hfa_all.scala notebook : " + status_hfa)
println("elapsed time " + start_hfa.until(end_hfa, ChronoUnit.MINUTES) + " minutes" )

//promo_all

var start_promo = getTime(LocalDateTime.now)
val status_promo = {
  if (status_hfa == "Success") 
  {
    println("promo_all.scala notebook has been started...")
    //dbutils.notebook.run("promo_all", timeoutSeconds = 0)
    dbutils.notebook.run("promo_all_temporary", timeoutSeconds = 0)
  }
  else println("Error from the previous step !")
}
var end_promo = getTime(LocalDateTime.now)

println("status for promo_all.scala notebook : " + status_promo)
println("elapsed time " + start_promo.until(end_promo, ChronoUnit.MINUTES) + " minutes" )

//df_bal

println("df_bal.scala notebook has been started...")
val start_df = getTime(LocalDateTime.now)
//val status_df = dbutils.notebook.run("df_bal", timeoutSeconds = 0)
val status_df = dbutils.notebook.run("df_bal_temporary", timeoutSeconds = 0)
val end_df = getTime(LocalDateTime.now)
println("status for df_bal.scala notebook : " + status_df)
println("elapsed time " + start_df.until(end_df, ChronoUnit.MINUTES) + " minutes" )


val end_time = getTime(LocalDateTime.now)

val job_status = 
if (status_ms == "Success" && status_si == "Success" && status_promo == "Success" &&  status_hfa == "Success" && status_df == "Success" ) "Success"
else if (status_ms == "Skipped" && status_si == "Skipped" && status_promo == "Skipped" &&  status_hfa == "Skipped" && status_df ==  "Skipped"  ) "Skipped"
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



// COMMAND ----------

