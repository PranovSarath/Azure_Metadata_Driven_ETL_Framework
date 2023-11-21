# Databricks notebook source
# DBTITLE 1,Import all Library functions
# MAGIC %run ../LibraryFunctions/master_library

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Create a new SparkSession
spark = SparkSession.builder.appName('ETL_CreateCuratedDimensionTables').getOrCreate()

# COMMAND ----------

# DBTITLE 1,Remove all Databricks widgets from the dashboard - commented out.
#dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Create and initialise the widgets to fetch parameters from ADF
dbutils.widgets.text("sourceSubjectName", "Sales_Trans")
dbutils.widgets.text("sourceEntityName", "DimCurrency")
dbutils.widgets.text("sourceSubjectID", "28")
dbutils.widgets.text("sourceEntityID", "10")

dbutils.widgets.text("targetSubjectName", "Sales_Curated")
dbutils.widgets.text("targetEntityName", "DimCurrency")
dbutils.widgets.text("targetSubjectID", "29")
dbutils.widgets.text("targetEntityID", "22")

dbutils.widgets.text("dataHistoryID", "1")
dbutils.widgets.text("BusinessKeys", "CurrencyCode;Name;")

dbutils.widgets.text("LoadType", "FullLoad")
dbutils.widgets.text("EntityType", "SCD-1")
dbutils.widgets.text("IdColumn", "CurrencyRateID")

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
sourceLayer = "Transformed"
targetLayer = "Curated"
businesskeys = dbutils.widgets.get("BusinessKeys")

trans_loadType = dbutils.widgets.get("LoadType")
trans_entityType =  dbutils.widgets.get("EntityType")
entityID_column_name = dbutils.widgets.get("IdColumn")

# COMMAND ----------

transformedEntityDf = spark.read.table(f'{sourceSubjectName}.{sourceEntityName}')

# COMMAND ----------

targetEntityPath = curated_layer_root_path + targetSubjectName + "/" + targetEntityName + "/"

# COMMAND ----------

if trans_entityType == "SCD-1":
    #Set target Entity Path
    targetEntityPath = curated_layer_root_path + targetSubjectName + "/" + targetEntityName + "/"
    business_key_col="BusinessKey"
    transformed_subject_col="TransformedSubject"
    valid_from_col = "ValidFromDWLoadDate"

    #Read existing SCD Type 1 table if it exists
    try:
        existingCuratedEntityDf = DeltaTable.forName(spark, f'{targetSubjectName}.{targetEntityName}')
    except Exception as e:
        print(f"Table {targetSubjectName}.{targetEntityName} does not exist. Creating a new one.")
        sql_statement = fn_generateCreateTableSqlStatement(transformedEntityDf, targetSubjectName, targetEntityName, targetEntityPath, trans_entityType)
        spark.sql(f'CREATE DATABASE IF NOT EXISTS {targetSubjectName}')
        spark.sql(f'USE DATABASE {targetSubjectName}')
        spark.sql(sql_statement)
        spark.sql(f'REFRESH TABLE {targetSubjectName}.{targetEntityName}')

        existingCuratedEntityDf = DeltaTable.forName(spark, f'{targetSubjectName}.{targetEntityName}')

    
    window_spec = Window.partitionBy("BusinessKey").orderBy(desc("ValidFromDWLoadDate"))
    # Using first() window function to get the latest ModifiedDate for each BusinessKey
    transformedEntityDf = transformedEntityDf.withColumn("first_ValidFromDWLoadDate", first("ValidFromDWLoadDate").over(window_spec))
    #transformedEntityDf = transformedEntityDf.withColumn(f"DL_{targetEntityName}_ID", monotonically_increasing_id())
    transformedEntityDf_filtered = transformedEntityDf.filter(col('first_ValidFromDWLoadDate') == col('ValidFromDWLoadDate'))
    transformedEntityDf = transformedEntityDf.drop('ValidFromDWLoadDate')
    transformedEntityDf = transformedEntityDf.withColumnRenamed('first_ValidFromDWLoadDate', 'ValidFromDWLoadDate')

    original_alias = "original"
    updates_alias = "updates"

    # Specify join condition
    join_condition = (col(f"{original_alias}.{business_key_col}") == col(f"{updates_alias}.{business_key_col}"))
    update_condition = (col(f"{original_alias}.{valid_from_col}") <= col(f"{updates_alias}.{valid_from_col}"))

    # Create a dictionary comprehension for update and insert values
    update_values = {f"{original_alias}.{col_name}": col(f"{updates_alias}.{col_name}") for col_name in transformedEntityDf.columns}
    insert_values = {f"{original_alias}.{col_name}": col(f"{updates_alias}.{col_name}") for col_name in transformedEntityDf.columns}

    # Merge Delta table with new dataset
    (existingCuratedEntityDf.alias(original_alias)
        .merge(transformedEntityDf.alias(updates_alias), join_condition)
        .whenMatchedUpdate(condition=update_condition, set=update_values)
        .whenNotMatchedInsert(values=insert_values)
        .execute())  
    
    print(f'\n\nSuccessfully updated/created SCD Type-1 curated table - {targetSubjectName}.{targetEntityName}')

# COMMAND ----------

transformedEntityDf

# COMMAND ----------

if trans_entityType == "SCD-2":
    #Set target Entity Path
    targetEntityPath = curated_layer_root_path + targetSubjectName + "/" + targetEntityName + "/"
    business_key_col="BusinessKey"
    transformed_subject_col="TransformedSubject"
    valid_from_col = "ValidFromDWLoadDate"
    valid_to_col = "ValidToDWLoadDate"

    #Read existing SCD Type 1 table if it exists
    try:
        existingCuratedEntityDf = DeltaTable.forName(spark, f'{targetSubjectName}.{targetEntityName}')
    except Exception as e:
        print(f"Table {targetSubjectName}.{targetEntityName} does not exist. Creating a new one.")
        sql_statement = fn_generateCreateTableSqlStatement(transformedEntityDf, targetSubjectName, targetEntityName, targetEntityPath, trans_entityType)
        spark.sql(f'CREATE DATABASE IF NOT EXISTS {targetSubjectName}')
        spark.sql(f'USE DATABASE {targetSubjectName}')
        spark.sql(sql_statement)
        spark.sql(f'REFRESH TABLE {targetSubjectName}.{targetEntityName}')

        existingCuratedEntityDf = DeltaTable.forName(spark, f'{targetSubjectName}.{targetEntityName}')

    
    existingDatadf = existingCuratedEntityDf.toDF()
    existingDatadf = existingDatadf.select(*transformedEntityDf.columns)

    combinedEntityDf = transformedEntityDf.union(existingDatadf).distinct()

    window_spec = Window.partitionBy("BusinessKey").orderBy(desc("ValidFromDWLoadDate"))
    # Using first() window function to get the latest ModifiedDate for each BusinessKey
    combinedEntityDf = combinedEntityDf.withColumn("ValidToDWLoadDate", lag("ValidFromDWLoadDate", offset=1).over(window_spec))
    combinedEntityDf = combinedEntityDf.withColumn("IsLatest", when(col('ValidToDWLoadDate').isNull(), True).otherwise(False))

    original_alias = "original"
    updates_alias = "updates"

    # Specify the join condition
    match_join_condition = (col(f"{original_alias}.{business_key_col}") == col(f"{updates_alias}.{business_key_col}")) 
    matched_update_condition = (col(f"{original_alias}.{valid_from_col}") == col(f"{updates_alias}.{valid_from_col}"))
    if entityID_column_name.strip(' ') != '':
        #matched_update_condition = (col(f"{original_alias}.{entityID_column_name}") == col(f"{updates_alias}.{entityID_column_name}"))
        match_join_condition = ((col(f"{original_alias}.{business_key_col}") == col(f"{updates_alias}.{business_key_col}")) &
                                (col(f"{original_alias}.{entityID_column_name}") == col(f"{updates_alias}.{entityID_column_name}")) )

    update_condition = (col(f"{original_alias}.{valid_from_col}") <= col(f"{updates_alias}.{valid_from_col}"))

    # Create a dictionary comprehension for update and insert values
    update_values = {f"{original_alias}.{col_name}": col(f"{updates_alias}.{col_name}") for col_name in combinedEntityDf.columns}
    insert_values = {f"{original_alias}.{col_name}": col(f"{updates_alias}.{col_name}") for col_name in combinedEntityDf.columns}

    # Create a condition for setting the end date of the old record
    end_date_condition = (col(f"{original_alias}.{valid_from_col}") <= col(f"{updates_alias}.{valid_from_col}"))

    # Update the existing records, setting the end date for the old version and IsLatest to False
    (existingCuratedEntityDf.alias(original_alias)
        .merge(combinedEntityDf.alias(updates_alias), match_join_condition)
        .whenMatchedUpdate(
            set= update_values
        )
        .whenNotMatchedInsert(
            values= insert_values
        )
        .execute())

    print(f'\n\nSuccessfully updated/created curated table - {targetSubjectName}.{targetEntityName}')
    
    print(f'\n\nSuccessfully updated/created SCD Type-2 curated table - {targetSubjectName}.{targetEntityName}')
