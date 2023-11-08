# Databricks notebook source
print('Importing Standard modules and functions')

# COMMAND ----------

try:
    #Import SparkSession.
    from pyspark.sql import SparkSession
    #Import functions from the pyspark.sql module for working with DataFrames.
    from pyspark.sql.functions import *

    #Import types from the pyspark.sql module for defining schema and data types.
    from pyspark.sql.types import *
except Exception as e:
    print(f'Encounter exception while importing standard modules and functions\nError: {str(e)}')

# COMMAND ----------

print('Successfully imported standard modules and function')
