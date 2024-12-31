# Databricks notebook source
# MAGIC %run ../Config

# COMMAND ----------

import json

# COMMAND ----------

response=getData(chess_com_endpoints['leaderboard'],'leaderboard')
leaderboard_info_all=[]
for leaderboard in response['daily']:
    leaderboard_info_all.append(json.dumps(leaderboard))

# COMMAND ----------

response.keys()

# COMMAND ----------

df_leaderboardinfo=spark.read.json(spark.sparkContext.parallelize(leaderboard_info_all))
df_leaderboardinfo.write.format("delta").mode("overwrite").option('overwriteSchema', 'true').saveAsTable("leaderboard")
