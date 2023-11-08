# Databricks notebook source
def fn_execQueryInSQLControlDB(query):
    spark = SparkSession.builder.appName("AzureSQLExecQuery").getOrCreate()
    # Fetch the driver manager from your spark context
    try:
        driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
        # Create a connection object using a jdbc-url, + sql uname & pass
        key_vault_scope = "az-kv-secret-scope-001"
        jdbc_url = dbutils.secrets.get(scope=key_vault_scope, key="controldb-jdbc-conn-string")
        con = driver_manager.getConnection(jdbc_url)

        # Create callable query statement and execute it
        exec_query = con.prepareCall(query)
        exec_query.execute()

        # Close connections
        exec_query.close()
        con.close()
        print('Query Successfully executed on the database')
    except Exception as e:
        print(f'Query execution on the database failed with error : {str(e)}')
