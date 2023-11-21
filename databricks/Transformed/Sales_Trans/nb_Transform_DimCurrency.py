# Databricks notebook source
# DBTITLE 1,Import all Library functions
# MAGIC %run ../../LibraryFunctions/master_library

# COMMAND ----------

# DBTITLE 1,Remove all Databricks widgets from the dashboard - commented out.
#dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Create a new SparkSession
spark = SparkSession.builder.appName('ETL_CreateTransformedDimCurrency').getOrCreate()

# COMMAND ----------

# DBTITLE 1,Create and initialise the widgets to fetch parameters from ADF
dbutils.widgets.text("sourceSubjectName", "Sales_Cleansed")
dbutils.widgets.text("sourceEntityName", "Currency")
dbutils.widgets.text("sourceSubjectID", "27")
dbutils.widgets.text("sourceEntityID", "9")

dbutils.widgets.text("targetSubjectName", "Sales_Trans")
dbutils.widgets.text("targetEntityName", "DimCurrency")
dbutils.widgets.text("targetSubjectID", "28")
dbutils.widgets.text("targetEntityID", "10")

dbutils.widgets.text("dataHistoryID", "1")
dbutils.widgets.text("BusinessKeys", "CurrencyCode;Name;")

dbutils.widgets.text("LoadType", "FullLoad")
dbutils.widgets.text("EntityType", "SCD-1")

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
sourceLayer = "PreTransformed"
targetLayer = "Transformed"
businesskeys = dbutils.widgets.get("BusinessKeys")

trans_loadType = dbutils.widgets.get("LoadType")
trans_entityType =  dbutils.widgets.get("EntityType")

# COMMAND ----------

preTrans_SubjectName = sourceSubjectName.replace("Cleansed", "PreTrans")
preTrans_SubjectName

# COMMAND ----------

targetSubjectName

# COMMAND ----------

preTransEntityDf = spark.read.table(f'{preTrans_SubjectName}.{sourceEntityName}')

# COMMAND ----------

display(preTransEntityDf)

# COMMAND ----------

for entry in preTransEntityDf.schema:
    if str(entry.dataType) == 'StringType()':
        col_name = str(entry.name)
        preTransEntityDf = preTransEntityDf.withColumn(col_name, when(col(col_name).isNull(), '').otherwise(trim(col(col_name))))

preTransEntityDf.show()

# COMMAND ----------

preTransEntityDf.createOrReplaceTempView('vw_transformedQuery')

# COMMAND ----------

transformedDf = spark.sql("""
                          SELECT CONCAT('CurrencyCode=', CurrencyCode, ';Name=', Name) AS BusinessKey,
                          CurrencyCode,
                          Name,
                          ModifiedDate,
                          current_timestamp() as Transformed_CreatedTime,
                          '{0}' as TransformedSubject,
                          ModifiedDate as ValidFromDWLoadDate
                          FROM vw_transformedQuery                      
                          """.format(targetSubjectName))

# COMMAND ----------

display(transformedDf)

# COMMAND ----------

# MAGIC %run ../nb_TransformedProcessing

# COMMAND ----------


