// Databricks notebook source
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

def getTime(a: => LocalDateTime) = a

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

//promo
println("promo.scala notebook has been started...")
var start_promo = getTime(LocalDateTime.now)
val status_promo = dbutils.notebook.run("promo", timeoutSeconds = 0)
var end_promo = getTime(LocalDateTime.now)
println("status for promo.scala notebook : " + status_promo)
println("elapsed time " + start_promo.until(end_promo, ChronoUnit.MINUTES) + " minutes" )

//oo_friday
println("oo_friday.scala notebook has been started...")
var start_oo_friday = getTime(LocalDateTime.now)
val status_oo_friday = dbutils.notebook.run("oo_friday", timeoutSeconds = 0)
var end_oo_friday = getTime(LocalDateTime.now)
println("status for oo_friday.scala notebook : " + status_oo_friday)
println("elapsed time " + start_oo_friday.until(end_oo_friday, ChronoUnit.MINUTES) + " minutes" )

//oo_tuesday
println("oo_tuesday.scala notebook has been started...")
var start_oo_tuesday = getTime(LocalDateTime.now)
val status_oo_tuesday = dbutils.notebook.run("oo_tuesday", timeoutSeconds = 0)
var end_oo_tuesday = getTime(LocalDateTime.now)
println("status for oo_tuesday.scala notebook : " + status_oo_tuesday)
println("elapsed time " + start_oo_tuesday.until(end_oo_tuesday, ChronoUnit.MINUTES) + " minutes" )

//oo_wednesday
println("oo_wednesday.scala notebook has been started...")
var start_oo_wednesday = getTime(LocalDateTime.now)
val status_oo_wednesday = dbutils.notebook.run("oo_wednesday", timeoutSeconds = 0)
var end_oo_wednesday = getTime(LocalDateTime.now)
println("status for oo_wednesday.scala notebook : " + status_oo_wednesday)
println("elapsed time " + start_oo_wednesday.until(end_oo_wednesday, ChronoUnit.MINUTES) + " minutes" )

//md_sku
println("md_sku.scala notebook has been started...")
var start_md_sku = getTime(LocalDateTime.now)
val status_md_sku = dbutils.notebook.run("md_sku", timeoutSeconds = 0)
var end_md_sku = getTime(LocalDateTime.now)
println("status for md_sku.scala notebook : " + status_md_sku)
println("elapsed time " + start_md_sku.until(end_md_sku, ChronoUnit.MINUTES) + " minutes" )



// COMMAND ----------

