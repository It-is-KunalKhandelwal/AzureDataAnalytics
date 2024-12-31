# Databricks notebook source
# MAGIC %run ../Config

# COMMAND ----------

import json

# COMMAND ----------

response=getData(chess_com_endpoints['username'],'username','GM')
player_info_all=[]
for player in response['players']:
    player_info=getData(chess_com_endpoints['userprofile'],'userprofile',player)
    player_info_all.append(json.dumps(player_info))


# COMMAND ----------

df_userprofile=spark.read.json(spark.sparkContext.parallelize(player_info_all))
df_userprofile.write.format("delta").mode("overwrite").saveAsTable("dim_userprofile")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_userprofile
