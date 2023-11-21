# Databricks notebook source
# DBTITLE 1,Import all Library functions
# MAGIC %run ../LibraryFunctions/master_library

# COMMAND ----------

spark = SparkSession.builder.appName('ETL_CreateOrUpdate_DataLoadHistory').getOrCreate()

# COMMAND ----------

dbutils.widgets.text("IsIncremental", "false")

IsIncremental = dbutils.widgets.get("IsIncremental")
IsIncremental = True if IsIncremental.upper() == "TRUE" else False

# COMMAND ----------

sourcePath = raw_layer_root_path + "/" + "config/DataLoadHistory.parquet"
targetPath = curated_layer_root_path + "/" + "config/DataLoadHistory"

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS config")
spark.sql("USE DATABASE config")

# COMMAND ----------

dataLoadHistoryDf = spark.read.parquet(sourcePath)
display(dataLoadHistoryDf)

# COMMAND ----------

DataLoadHistory_SourceDf = DeltaTable.forPath(spark, targetPath)

# COMMAND ----------

if IsIncremental:
    DataLoadHistory_SourceDf = DeltaTable.forPath(spark, targetPath)
    (DataLoadHistory_SourceDf.alias('source')
    .merge(dataLoadHistoryDf.alias('target'), "source.DataLoadHistoryID = target.DataLoadHistoryID")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
    )
else:
    dataLoadHistoryDf.write.mode('overwrite').format("delta").option("path", targetPath).saveAsTable('DataLoadHistory')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from config.DataLoadHistory
