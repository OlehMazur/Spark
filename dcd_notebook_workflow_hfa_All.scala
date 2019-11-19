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

//sell_in_all 
println("sell_in_all.scala notebook has been started...")
var start_si = getTime(LocalDateTime.now)
val status_si = dbutils.notebook.run("sell_in_all", timeoutSeconds = 0)
var end_si = getTime(LocalDateTime.now)
println("status for sell_in_all.scala notebook : " + status_si)
println("elapsed time " + start_si.until(end_si, ChronoUnit.MINUTES) + " minutes" )

//hfa_all
var start_hfa = getTime(LocalDateTime.now)
val status_hfa = {
  if (status_si == "Success") 
  {
    println("hfa_all.scala notebook has been started...")
    dbutils.notebook.run("hfa_all", timeoutSeconds = 0)
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
    dbutils.notebook.run("promo_all", timeoutSeconds = 0)
  }
  else println("Error from the previous step !")
}
var end_promo = getTime(LocalDateTime.now)

println("status for promo_all.scala notebook : " + status_promo)
println("elapsed time " + start_promo.until(end_promo, ChronoUnit.MINUTES) + " minutes" )



// COMMAND ----------

