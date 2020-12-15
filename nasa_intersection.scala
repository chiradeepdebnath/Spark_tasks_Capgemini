// Databricks notebook source
//https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5996310910311000/846561231414273/2803807529895173/latest.html

// COMMAND ----------

val julydata=sc.textFile("/FileStore/tables/nasa_july.tsv")
val augustdata=sc.textFile("/FileStore/tables/nasa_august.tsv")

// COMMAND ----------

val julyRdd = julydata.map(x => x.split("\t")(0) ) 
val augRdd = augustdata.map(x => x.split("\t")(0) )

// COMMAND ----------

val intersectionRdd = julyRdd.intersection(augRdd)

intersectionRdd.collect()

// COMMAND ----------

val finalRdd = intersectionRdd.filter(x => !(x.contains("host")))

finalRdd.collect()


// COMMAND ----------

finalRdd.count()

// COMMAND ----------


