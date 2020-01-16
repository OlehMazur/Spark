// Databricks notebook source
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

def getTime(a: => LocalDateTime) = a

//md_sku
println("md_sku.scala notebook has been started...")
var start_ms = getTime(LocalDateTime.now)
val status_ms = dbutils.notebook.run("md_sku", timeoutSeconds = 0)
var end_ms = getTime(LocalDateTime.now)
println("status for md_sku.scala notebook : " + status_ms)
println("elapsed time " + start_ms.until(end_ms, ChronoUnit.MINUTES) + " minutes" )

//sell-in 
println("sell_in.scala notebook has been started...")
var start_si = getTime(LocalDateTime.now)
val status_si = dbutils.notebook.run("sell_in", timeoutSeconds = 0)
var end_si = getTime(LocalDateTime.now)
println("status for sell_in.scala notebook : " + status_si)
println("elapsed time " + start_si.until(end_si, ChronoUnit.MINUTES) + " minutes" )

//hfa
var start_hfa = getTime(LocalDateTime.now)
val status_hfa = {
  if (status_si == "Success") 
  {
    println("hfa.scala notebook has been started...")
    dbutils.notebook.run("hfa", timeoutSeconds = 0)
  }
  else println("Error from the previous step !")
}
var end_hfa = getTime(LocalDateTime.now)

println("status for hfa.scala notebook : " + status_hfa)
println("elapsed time " + start_hfa.until(end_hfa, ChronoUnit.MINUTES) + " minutes" )


// COMMAND ----------

