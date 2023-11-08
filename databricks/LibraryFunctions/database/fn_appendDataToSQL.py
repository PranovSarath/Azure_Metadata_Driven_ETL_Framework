# Databricks notebook source
#from pyspark.sql import SparkSession

# COMMAND ----------

def fn_appendDataToSQL(dataframe, table="[METADATA].[ColumnDetails]"):
    key_vault_scope = "az-kv-secret-scope-001"
    spark = SparkSession.builder.appName("AzureSQLAppend").getOrCreate()
    jdbc_url = dbutils.secrets.get(scope=key_vault_scope, key="controldb-jdbc-conn-string")
    try:
        # Write back to the table in "overwrite" mode
        (dataframe.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .mode("append")
        .save()
        )
        print("Upsert operation completed successfully.")
    except Exception as e:
        print("Failed to perform upsert operation:", str(e))
