// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC ## Overview
// MAGIC 
// MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
// MAGIC 
// MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

// COMMAND ----------

//https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5996310910311000/1097318194761041/2803807529895173/latest.html

// COMMAND ----------

// MAGIC %python
// MAGIC # File location and type
// MAGIC file_location = "/FileStore/tables/numberData.csv"
// MAGIC file_type = "csv"
// MAGIC 
// MAGIC # CSV options
// MAGIC infer_schema = "false"
// MAGIC first_row_is_header = "false"
// MAGIC delimiter = ","
// MAGIC 
// MAGIC # The applied options are for CSV files. For other file types, these will be ignored.
// MAGIC df = spark.read.format(file_type) \
// MAGIC   .option("inferSchema", infer_schema) \
// MAGIC   .option("header", first_row_is_header) \
// MAGIC   .option("sep", delimiter) \
// MAGIC   .load(file_location)
// MAGIC 
// MAGIC display(df)

// COMMAND ----------

// MAGIC %python
// MAGIC # Create a view or table
// MAGIC 
// MAGIC temp_table_name = "numberData_csv"
// MAGIC 
// MAGIC df.createOrReplaceTempView(temp_table_name)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC /* Query the created temp table in a SQL cell */
// MAGIC 
// MAGIC select * from `numberData_csv`

// COMMAND ----------

// MAGIC %python
// MAGIC # With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
// MAGIC # Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
// MAGIC # To do so, choose your table name and uncomment the bottom line.
// MAGIC 
// MAGIC permanent_table_name = "numberData_csv"
// MAGIC 
// MAGIC # df.write.format("parquet").saveAsTable(permanent_table_name)

// COMMAND ----------

val numberRDD=spark.read.textFile("/FileStore/tables/numberData.csv").rdd

// COMMAND ----------

val header=numberRDD.first
val withoutheader=numberRDD.filter(line=>line!=header)
withoutheader.collect
val intdata=withoutheader.map(line=>line.toInt)
intdata.collect()

// COMMAND ----------

def prime_num(a:Int):Boolean={
  var r=true
  var x=0
  if(a==0 || a==1){
    return false
  }
  else
  {
    for(x<-2 until a)
    {
      if(a%x==0)
      {
        r=false
      }
    }
    return r
  }
}

// COMMAND ----------

val primefilter=intdata.filter(line=>prime_num(line))
primefilter.collect()

// COMMAND ----------

primefilter.count()

// COMMAND ----------

val sumData = primefilter.sum()
println("Total sum "+sumData)

// COMMAND ----------


