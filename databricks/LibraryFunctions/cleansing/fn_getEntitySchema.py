# Databricks notebook source
def fn_getEntitySchema(df, entityID=1):
    sourceEntity = df
    schema = sourceEntity.schema
    columnDetails = []
    for entry in schema: 
        column = entry.name
        datatype = str(entry.dataType)
        is_nullable = not entry.nullable
        max_length = fn_getDataTypeMaxLength(entry.dataType)
        description = None

        columnDetails.append((entityID, column, datatype, max_length, is_nullable, description))

    schema = StructType([
        StructField("EntityID", IntegerType(), False),
        StructField("ColumnName", StringType(), False),
        StructField("DataType", StringType(), False),
        StructField("MaxLength", IntegerType(), True),
        StructField("IsNullable", BooleanType(), False),
        StructField("Description", StringType(), True)
        ])

    metadata_df = spark.createDataFrame(columnDetails, schema=schema)
    return metadata_df

# COMMAND ----------


