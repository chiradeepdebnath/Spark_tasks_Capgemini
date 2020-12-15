// Databricks notebook source
//https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5996310910311000/2894282466980647/2803807529895173/latest.html

// COMMAND ----------

val augRdd = sc.textFile("/FileStore/tables/nasa_august.tsv")

val julyRdd = sc.textFile("/FileStore/tables/nasa_july.tsv")

// COMMAND ----------

val unionRdd = augRdd.union(julyRdd)

unionRdd.take(2)


// COMMAND ----------

val header = unionRdd.first

// COMMAND ----------

unionRdd.filter(line =>line != header).take(2)

// COMMAND ----------

def headerRemover(line:String): Boolean = !(line.startsWith("host"))

// COMMAND ----------

val finalRdd = unionRdd.filter(x => headerRemover(x))
finalRdd.take(2)

// COMMAND ----------

val zeroRes = finalRdd.filter(x => x.split("\t")(5).toInt == 0)

val bytes = finalRdd.filter(x => x.split("\t")(6).toInt >= 1000)

(zeroRes.collect() , bytes.collect())

// COMMAND ----------

bytes.count()

// COMMAND ----------

val reqdata = finalRdd.filter(x => ( x.split("\t")(5).toInt == 0 || x.split("\t")(6).toInt >= 1000))

reqdata.count()


// COMMAND ----------

finalRdd.sample(withReplacement = true, fraction=0.20).collect()

// COMMAND ----------


