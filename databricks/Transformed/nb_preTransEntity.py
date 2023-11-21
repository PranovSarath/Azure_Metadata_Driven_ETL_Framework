# Databricks notebook source
# DBTITLE 1,Import all Library functions
# MAGIC %run ../LibraryFunctions/master_library

# COMMAND ----------

# DBTITLE 1,Remove all Databricks widgets from the dashboard - commented out.
#dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Create a new SparkSession
spark = SparkSession.builder.appName('ETL_CreatePreTransEntity').getOrCreate()

# COMMAND ----------

# DBTITLE 1,Create and initialise the widgets to fetch parameters from ADF
dbutils.widgets.text("sourceSubjectName", "Sales_Cleansed")
dbutils.widgets.text("sourceEntityName", "Customer")
dbutils.widgets.text("sourceSubjectID", "26")
dbutils.widgets.text("sourceEntityID", "2")

dbutils.widgets.text("targetSubjectName", "Sales_Trans")
dbutils.widgets.text("targetEntityName", "Customer")
dbutils.widgets.text("targetSubjectID", "27")
dbutils.widgets.text("targetEntityID", "3")

dbutils.widgets.text("dataHistoryID", "1")

# COMMAND ----------

# DBTITLE 1,Retrieve the input parameters from the ADF pipeline
#Source
sourceSubjectName = dbutils.widgets.get("sourceSubjectName")
sourceEntityName = dbutils.widgets.get("sourceEntityName")
sourceSubjectID = int(dbutils.widgets.get("sourceSubjectID"))
sourceEntityID = int(dbutils.widgets.get("sourceEntityID"))

#Target
targetSubjectName = dbutils.widgets.get("targetSubjectName")
targetEntityName = dbutils.widgets.get("targetEntityName")
targetSubjectID = int(dbutils.widgets.get("targetSubjectID"))
targetEntityID = int(dbutils.widgets.get("targetEntityID"))

dataHistoryID = int(dbutils.widgets.get('dataHistoryID'))
sourceLayer = "Cleansed"
targetLayer = "Transformed"

# COMMAND ----------

# DBTITLE 1,Set the cleansedSubjectName and targetSubjectName
cleansedSubjectName = sourceSubjectName.replace(f"_{sourceLayer}", "")
cleansedSubjectName

transformedSubjectName = targetSubjectName

targetSubjectName = targetSubjectName.replace("Trans", "PreTrans")
targetSubjectName

# COMMAND ----------

# DBTITLE 1,Set the source and target paths
entityCleansedPath = cleansed_layer_root_path + cleansedSubjectName + "/" + sourceEntityName + "/" + sourceEntityName + '.parquet'
targetEntityPath = pre_transformed_layer_root_path + targetSubjectName + "/" + sourceEntityName + "/"

# COMMAND ----------

# DBTITLE 1,Load the cleansed source file
cleansedEntityDf = spark.read.parquet(entityCleansedPath)
display(cleansedEntityDf)

# COMMAND ----------

# DBTITLE 1,Create 2 additional columns - DataLoadHistory (will act as a foreign key) and Timestamp for PreTrans processing.
cleansedEntityDf = cleansedEntityDf.withColumn('FK_ID_DataLoadHistory', lit(dataHistoryID).cast(IntegerType()))
preTransEntity = cleansedEntityDf.withColumn('PreTrans_CreatedTime', current_timestamp())

# COMMAND ----------

# DBTITLE 1,Write the file to pretransformed layer in Azure Data Lake Gen2 and create a Delta table for querying
spark.sql(f"CREATE DATABASE IF NOT EXISTS {targetSubjectName}")
spark.sql(f"USE DATABASE {targetSubjectName}")
preTransEntity.write.mode('overwrite').format("delta").option("path", targetEntityPath).saveAsTable(f"{targetSubjectName}.{sourceEntityName}")
spark.sql(f"REFRESH TABLE {targetSubjectName}.{sourceEntityName}")

# COMMAND ----------

# DBTITLE 1,Garbage collection
del preTransEntity
del cleansedEntityDf

# COMMAND ----------

# DBTITLE 1,Create Database for Transformed Layer
spark.sql(f"CREATE DATABASE IF NOT EXISTS {transformedSubjectName}")
