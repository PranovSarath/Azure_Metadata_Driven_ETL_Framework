# Databricks notebook source
#from pyspark.sql import SparkSession

# COMMAND ----------

def fn_execPushdownQueryInControlDB(pushdown_query=""):
    """
    Input Parameter:
    pushdown_query : str
                    This parameter should be of the form:
                    "(select * from [METADATA].[Entity] WHERE EntityID = 1) as temp_alias"

    Output Parameter:
    df_read: DataFrame containing the output of the query
    """
    spark = SparkSession.builder.appName("AzureSQLPushdownQuery").getOrCreate()
    key_vault_scope = "az-kv-secret-scope-001"
    jdbc_conn_string = dbutils.secrets.get(scope=key_vault_scope, key="controldb-jdbc-conn-string")
    
    df_read = (spark.read
    .format("jdbc")
    .option("url", jdbc_conn_string)
    .option("dbtable", pushdown_query)
    .option("fetchSize", "100")
    .load()
    )

    return df_read
