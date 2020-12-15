// Databricks notebook source
val julydata=sc.textFile("/FileStore/tables/nasa_july.tsv")
val augustdata=sc.textFile("/FileStore/tables/nasa_august.tsv")

// COMMAND ----------

val julyhost=julydata.map(x=>x.split("\t")(0))
val augusthost=augustdata.map(x=>x.split("\t")(0))

// COMMAND ----------

val intersectRDD=julyhost.intersection(augusthost)

// COMMAND ----------

def headerRemover(line:String):Boolean=!(line.startsWith("host"))
intersectRDD.filter(x=>headerRemover(x))count()

// COMMAND ----------


