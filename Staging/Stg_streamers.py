# Databricks notebook source
# MAGIC %run ../Config

# COMMAND ----------

import json

# COMMAND ----------

response=getData(chess_com_endpoints['streamers'],'streamers')
streamers_info_all=[]
for streamer in response['streamers']:
    streamers_info_all.append(json.dumps(streamer))

# COMMAND ----------

df_streamerinfo=spark.read.json(spark.sparkContext.parallelize(streamers_info_all))
df_streamerinfo.write.format("delta").mode("overwrite").option('overwriteSchema', 'true').saveAsTable("streamers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from streamers

# COMMAND ----------


