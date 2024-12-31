# Databricks notebook source
# MAGIC %run ../Config

# COMMAND ----------

import json
import requests

# COMMAND ----------

df_all_username=spark.table('dim_userprofile').select('username').collect()
stats_all_username=[]
for username in df_all_username:
    data=getData(chess_com_endpoints['userprofile'],'userstat',username[0])
    stats_all_username.append(json.dumps(data))

# COMMAND ----------

df_userstats=spark.read.json(spark.sparkContext.parallelize(stats_all_username))
df_userstats.write.format("delta").mode("overwrite").option('overwriteschema','true').saveAsTable("userstats")

# COMMAND ----------

spark.table('userstats').display()

# COMMAND ----------


