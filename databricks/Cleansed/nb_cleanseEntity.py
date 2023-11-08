# Databricks notebook source
# DBTITLE 1,Import all Library functions
# MAGIC %run ../LibraryFunctions/master_library

# COMMAND ----------

# DBTITLE 1,Remove all Databricks widgets from the dashboard - commented out.
#dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Create the Databricks widgets for passing in parameters
dbutils.widgets.text("sourceSubjectName", "Sales")
dbutils.widgets.text("sourceEntityName", "Customer")
dbutils.widgets.text("sourceSubjectID", "25")
dbutils.widgets.text("sourceEntityID", "1")

dbutils.widgets.text("targetSubjectName", "Sales_Cleansed")
dbutils.widgets.text("targetEntityName", "Customer")
dbutils.widgets.text("targetSubjectID", "26")
dbutils.widgets.text("targetEntityID", "2")

dbutils.widgets.text("dataHistoryID", "1")
dbutils.widgets.text('IngestionTime', "2023-11-08 14:31:07.263")

# COMMAND ----------

# DBTITLE 1,Retrieve the values from the Databricks widgets
sourceSubjectName = dbutils.widgets.get("sourceSubjectName")
sourceEntityName = dbutils.widgets.get("sourceEntityName")
sourceSubjectID = int(dbutils.widgets.get("sourceSubjectID"))
sourceEntityID = int(dbutils.widgets.get("sourceEntityID"))
#Target Entity
targetSubjectName = dbutils.widgets.get("targetSubjectName")
targetEntityName = dbutils.widgets.get("targetEntityName")
targetSubjectID = int(dbutils.widgets.get("targetSubjectID"))
targetEntityID = int(dbutils.widgets.get("targetEntityID"))

dataHistoryID = int(dbutils.widgets.get('dataHistoryID'))
ingestionTime = dbutils.widgets.get('IngestionTime')
sourceLayer = "Raw"
targetLayer = "Cleansed"

# COMMAND ----------

# DBTITLE 1,Set the entity source path and target path
sourceEntityPath = raw_layer_root_path + sourceSubjectName + "/" + str(dataHistoryID) + "/" + sourceEntityName + "/" + sourceEntityName + ".csv"
targetEntityPath = cleansed_layer_root_path + sourceSubjectName + "/" + sourceEntityName + "/" + sourceEntityName + '.parquet'

# COMMAND ----------

# DBTITLE 1,Create a new SparkSession
spark = SparkSession.builder.appName("ETL_CleanseEntity").getOrCreate()

# COMMAND ----------

# DBTITLE 1,Load the source Entity File
sourceEntityDf = spark.read.option('header',True).option('inferSchema', True).csv(sourceEntityPath)

# COMMAND ----------

display(sourceEntityDf)

# COMMAND ----------

# DBTITLE 1,Rename the Source Entity column names
#Rename the Source Entity column names
sourceEntityDf = fn_renameColumns(sourceEntityDf)

#Replace any NaN values with nulls
sourceEntityDf = sourceEntityDf.replace(float('nan'), None)

#Remove extra spaces from the string fields.
for entry in sourceEntityDf.schema:
   if str(entry.dataType) == 'StringType()':
       sourceEntityDf = sourceEntityDf.withColumn(entry.name, trim(col(entry.name)))       

# COMMAND ----------

# DBTITLE 1,Update the METADATA.ColumnDetails table with the source entity schema
sourceEntityDf = fn_updateMetadataInControlDB(sourceEntityDf, sourceEntityID)

# COMMAND ----------

# DBTITLE 1,Add IngestionTime and CleansedTime columns to the cleansed entity
cleansedEntity = sourceEntityDf.withColumn('IngestionTime', lit(ingestionTime).cast(TimestampType()))
cleansedEntity = cleansedEntity.withColumn('CleansedTime', current_timestamp())

# COMMAND ----------

display(cleansedEntity)

# COMMAND ----------

cleansedEntity = fn_updateMetadataInControlDB(cleansedEntity, targetEntityID)

# COMMAND ----------

print(targetEntityPath)

# COMMAND ----------

cleansedEntity.write.mode('overwrite').parquet(targetEntityPath)
