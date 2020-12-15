// Databricks notebook source
//https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5996310910311000/2894282466980672/2803807529895173/latest.html

// COMMAND ----------

val data = List("chiradeep", "debnath", "chiradeep", "sir", "debnath", "capg")

// COMMAND ----------

val datardd = sc.parallelize(data)
datardd.count()


// COMMAND ----------

datardd.countByValue()

// COMMAND ----------

val data2 = List(1,2,3,4,5)

val data2rdd = sc.parallelize(data2)

// COMMAND ----------

val productRDD = data2rdd.reduce( (x,y) => x*y )

productRDD

// COMMAND ----------


