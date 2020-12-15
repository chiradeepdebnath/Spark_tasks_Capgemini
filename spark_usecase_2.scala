// Databricks notebook source
//https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5996310910311000/2894282466980639/2803807529895173/latest.html

// COMMAND ----------

val apdata = sc.textFile("/FileStore/tables/airports-1.text")

// COMMAND ----------

val USdata = apdata.filter( line => line.split(",")(3) == "\"United States\"" )
USdata.take(2)

// COMMAND ----------

def splitIP(line:String) = {
  
  val datasplit = line.split(",") 
  
  val apname = datasplit(1)
  
  val apcity = datasplit(2)
  
  (apname, apcity)
}

// COMMAND ----------

val reqdata = USdata.map(splitIP)

reqdata.take(2)


// COMMAND ----------

USdata.map( line => {
  val splitdata = line.split(",")
  splitdata(1) + " -- " + splitdata(2) }).take(10)

// COMMAND ----------


