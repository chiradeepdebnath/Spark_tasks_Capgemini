# Databricks notebook source
# https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5996310910311000/2189058127913501/2803807529895173/latest.html

# COMMAND ----------

# DBTITLE 1,Q1
val=1440000
jugal=13*val/36
chandan=12*val/36
raunak=11*val/36

print("jugal: Rs. %d"%jugal)
print("chandan: Rs. %d"%chandan)
print("raunak: Rs. %d"%raunak)

# COMMAND ----------

# DBTITLE 1,Q3
def time_conversion(seconds): 
    seconds=seconds%(24*3600) 
    hours=seconds//3600
    seconds%=3600
    minutes=seconds//60
    seconds%=60
      
    return "%d:%02d:%02d" % (hours, minutes, seconds) 
  

# COMMAND ----------

time_in_seconds =1000
#= input("Enter time in seconds: ")
print(time_conversion(time_in_seconds))

# COMMAND ----------


