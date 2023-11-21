# Databricks notebook source
def fn_updateMetadataInControlDB(sourceDf, entityID):
    #Check if metadata of the entity already exists in the control database
    check_metadata_query = f"(SELECT  [EntityID] as EntityID_df1, [ColumnName] as ColumnName_df1, [DataType] as DataType_df1, [MaxLength] as MaxLength_df1, [IsNullable] as IsNullable_df1, [Description] as [Description_df1] FROM [METADATA].[ColumnDetails] WHERE EntityID = {int(entityID)} and IsActiveFlag = 1) as temp_alias"
    existing_metadata_records = fn_execPushdownQueryInControlDB(check_metadata_query)

    entitySchema = fn_getEntitySchema(sourceDf, entityID)

    #Check if metadata records exist for the source entity in the Metadata.ColumnDetails table in the control DB.
    if existing_metadata_records.count() == 0:
        fn_appendDataToSQL(entitySchema)
    #If metadata records do not exist, then...
    else:
        existing_and_current_metadata_records = existing_metadata_records.alias('df1').join(entitySchema.alias('df2'), existing_metadata_records['ColumnName_df1'] == entitySchema['ColumnName'], how='full')
        #Check if the number of columns in the source file and in the Metadata.ColumnDetails table match
        try:
            #No need to write back to the METADATA.ColumnDetails table if count matches
            assert existing_and_current_metadata_records.count() == existing_metadata_records.count() 
        except Exception as e:
            #If exception is thrown, set existing Metadata records to inactive.
            #Update the Metadata with the latest source entity schema.
            queryStatement = f"UPDATE [METADATA].[ColumnDetails] SET [IsActiveFlag] = 0 WHERE [EntityID] = {int(entityID)}"
            fn_execQueryInSQLControlDB(queryStatement)
            fn_appendDataToSQL(entitySchema)
            print('METADTA.ColumnDetails table updated with the latest entity schema information')
        try:
            #Check if the schema present in the METADATA.ColumnDetails table and the actual source file is matching
            existing_and_current_metadata_records = existing_and_current_metadata_records.withColumn('IsSchemaMatching', when((col('df1.DataType_df1') == col('df2.DataType')) & ((col('df1.ColumnName_df1') == col('df2.ColumnName') ) & (col('df1.ColumnName_df1').isNull()) &  (col('df2.ColumnName').isNull())  ) & ((col('df1.MaxLength_df1') == col('df2.MaxLength')) | col('df2.MaxLength').isNull() | col('df1.MaxLength_df1').isNull()) & (col('df1.IsNullable_df1')== col('df2.IsNullable'))  , True).otherwise(False))

            #Ensure schema in the Metadata.ColumnDetails table is an exact match to the current source entity schema.
            assert existing_and_current_metadata_records.select("IsSchemaMatching").filter(col("IsSchemaMatching") == False).count() == 0 
            print('Current entity schema matches exactly with the existing schema in METADATA.ColumnDetails')
        except Exception as e:
            unmatched_schema_columns = existing_and_current_metadata_records.filter(col("IsSchemaMatching") == False)

            #If TypeCasting throws an error, then proceed with current schema and update the new schema in Control DB.
            try:
                for row in existing_and_current_metadata_records.filter(col("IsSchemaMatching") == False).collect():
                    initialDataType = row["DataType_df1"]
                    currDataType = row["DataType"]
                    columnName = row["ColumnName"]
                    existingColumnName = row["ColumnName_df1"]
                    if initialDataType != currDataType and (existingColumnName is not None) and (columnName is not None):
                        print(initialDataType, currDataType, columnName, row["ColumnName_df1"])
                        print('-----------------------------------7-------------------------------')
                        sourceDf = sourceDf.withColumn(columnName, col(columnName).cast(initialDataType))
                print('Current entity has been typecasted to match existing schema in METADATA.ColumnDetails')
            except Exception as e:
                queryStatement = f"UPDATE [METADATA].[ColumnDetails] SET [IsActiveFlag] = 0 WHERE [EntityID] = {int(entityID)}"
                fn_execQueryInSQLControlDB(queryStatement)
                fn_appendDataToSQL(entitySchema)   
                print('METADTA.ColumnDetails table updated with the latest entity schema information')
    return sourceDf
