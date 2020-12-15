// Databricks notebook source
//https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5996310910311000/4209650493362979/2803807529895173/latest.html

// COMMAND ----------

val data = sc.textFile("/FileStore/tables/airports-1.text")

// COMMAND ----------

val dataRdd = data.map(line => ( line.split(",")(1), line.split(",")(3) ))

dataRdd.take(3)

// COMMAND ----------

val NotCanada = dataRdd.filter( x => x._2 != "\"Canada\"")
NotCanada.take(2)


// COMMAND ----------

NotCanada.filter(x => x._2 == "\"Canada\"").take(2)

// COMMAND ----------

val listData = List("Chiradeep 2020","Nancy 1998", "Abhinav 1997", "Kartik 2100")

// COMMAND ----------

val kvrdd = sc.parallelize(listData)

// COMMAND ----------

val rddnew = kvrdd.map(x => (x.split(" ")(0), x.split(" ")(1).toInt))

rddnew.take(3)

// COMMAND ----------

rddnew.mapValues(x => x+10).take(3)

// COMMAND ----------

val data2 = data.map(line => ( line.split(",")(1), line.split(",")(11) ))

data2.take(5)


// COMMAND ----------

data2.mapValues(x => x.toLowerCase).take(5)

// COMMAND ----------


