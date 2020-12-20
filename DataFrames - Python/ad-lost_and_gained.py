# Databricks notebook source
#https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5996310910311000/2246070797776684/2803807529895173/latest.html

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/ad.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "ad_csv"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC select * from `ad_csv`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "ad_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

#/FileStore/tables/ad.csv
 
from pyspark.sql import *
dfad = spark.read.csv("/FileStore/tables/ad.csv",header=True,inferSchema=True)

# COMMAND ----------

dfad.show()

# COMMAND ----------

dfad.registerTempTable('adTable')
sqlContext.sql('select * from adTable').show()

# COMMAND ----------

dfcolor_2018 = dfad.select('channel','advertiser').filter((dfad['channel']=='color') & (dfad['year']==2018))
dfstarplus_2018 = dfad.select('channel','advertiser').filter((dfad['channel']=='star-plus') & (dfad['year'] == 2018))
dfcolor_2019 = dfad.select('channel','advertiser').filter((dfad['channel']=='color') & (dfad['year']==2019))
dfstarplus_2019 = dfad.select('channel','advertiser').filter((dfad['channel']=='star-plus') & (dfad['year'] == 2019))
 
 
dfcolor_2018.show()
dfstarplus_2018.show()
dfcolor_2019.show()
dfstarplus_2019.show()

# COMMAND ----------

color_lost_2019 = dfcolor_2018.exceptAll(dfcolor_2019).withColumnRenamed('advertiser','lost advertiser')
color_new_2019  = dfcolor_2019.exceptAll(dfcolor_2018).withColumnRenamed('advertiser','new advertiser')
starplus_lost_2019 = dfstarplus_2018.exceptAll(dfstarplus_2019).withColumnRenamed('advertiser','lost advertiser')
starplus_new_2019  = dfstarplus_2019.exceptAll(dfstarplus_2018).withColumnRenamed('advertiser','new advertiser')
starplus_lost_2019.show()
starplus_new_2019.show()
color_lost_2019.show()
color_new_2019.show()

# COMMAND ----------

from pyspark.sql.types import StructField, StructType,StringType
from pyspark.sql.dataframe import 
LostFoundSchema = StructType([StructField('channel',StringType(),True),
                              StructField('advertiser_lost',StringType(),True),
                              StructField('advertiser_found',StringType(),True)])

# COMMAND ----------

from pyspark.sql.functions import lit
df1 = starplus_lost_2019.join(starplus_new_2019,'channel','leftouter')
df2 = color_lost_2019.join(color_new_2019,'channel','inner')
dfLostFound = df1.union(df2).withColumn('year',lit(2019))

# COMMAND ----------

dfLostFound.select('year','channel','lost advertiser','new advertiser').show()

# COMMAND ----------


