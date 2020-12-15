// Databricks notebook source
val data=sc.textFile("/FileStore/tables/airports-1.text")
data.collect()

// COMMAND ----------

val data=sc.textFile("/FileStore/tables/airports-1.text")
data.collect()
data.filter(e=>e.country=="United states")

// COMMAND ----------

// /FileStore/tables/airports.csv

val airData=sc.textfile("/FileStore/tables/airports-1.text")
val LocatedData=airData.filter(line =>{
  (line.split(",")(6) > "\"40\"") && (line.split(",")(3) contains "Island")
})
LocatedData.collect()

// COMMAND ----------

val airData=sc.textFile("/FileStore/tables/airports-1.text")
val LocatedData=airData.filter(line =>{
  (line.split(",")(6) > "\"40\"") && (line.split(",")(3) contains "Island")
})
LocatedData.collect()|

// COMMAND ----------

val airData=sc.textFile("/FileStore/tables/airports-1.text")
val LocatedData=airData.filter(line =>{
  (line.split(",")(6) > "\"40\"") && (line.split(",")(3) contains "Island")
})
LocatedData.collect()

// COMMAND ----------

airData

// COMMAND ----------

val newtask=airData.map(x=> (x.split(",")(0),x.split(",")(11)))
newtask.take(3)

// COMMAND ----------

val task1=airData.map(x=> (x.split(",")(1),x.split(",")(11).toLowerCase))
task1.take(3)

// COMMAND ----------

val airData=sc.textFile("/FileStore/tables/airports-1.text")

// COMMAND ----------

val task1=airData.map(x=> (x.split(",")(1),x.split(",")(11).toLowerCase))
task1.take(3)

// COMMAND ----------


