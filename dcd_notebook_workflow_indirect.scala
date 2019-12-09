// Databricks notebook source
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

def getTime(a: => LocalDateTime) = a

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



