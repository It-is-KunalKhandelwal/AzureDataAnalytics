# Databricks notebook source
url = "https://api.chess.com/pub/country/RS"

# Split the URL by '/'
parts = url.split('/')[-1]

# COMMAND ----------

parts

# COMMAND ----------

df_isocountrycode=spark.table('iso_codes')
display(col('df_isocountrycode.Name'))

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import split, col,explode,from_unixtime

# COMMAND ----------

df = spark.table('dim_userprofile').select(
    'country', 'followers', 'is_streamer', 'joined', 'last_online', 'league', 
    'name', 'player_id', 'status', 'streaming_platforms', 'title', 'username', 'verified'
)

df = df.withColumn("country_code", split(col("country"), "/").getItem(5))

df = df.join(
    df_isocountrycode, 
    df.country_code == df_isocountrycode.Code, 
    how='left'
).select(
    df['*'],
    df_isocountrycode['Name'].alias('country_name')
)

df=df.withColumn('streaming_platforms',explode('streaming_platforms'))
df=df.withColumn('streaming_platforms',col('streaming_platforms.type'))
df=df.withColumn("joined", from_unixtime(df["joined"], "yyyy-MM-dd"))
df=df.drop('country','last_online','country_code')
df.write.format('delta').mode('overwrite').saveAsTable('userprofile')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from userprofile

# COMMAND ----------


