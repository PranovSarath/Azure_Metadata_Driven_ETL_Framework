# Databricks notebook source
# DBTITLE 1,Print TransformedProcessing prerequisites.
print('Executing post processing step for the entity...')
print('Please ensure that the following variables are initialised')
print('===================================================================================')
print('targetSubjectName, targetEntityName, targetSubjectID, targetEntityID, businesskeys')
print('===================================================================================')
assert targetSubjectName is not None, "The variable - targetSubjectName is not initialised"
assert targetEntityName is not None, "The variable - targetEntityName is not initiliased"
assert targetSubjectID is not None, "The variable - targetSubjectID is not initialised"
assert targetEntityID is not None, "The variable - targetEntityID is not initialised"

print('\n\nExecuting.......')
targetLayer = "Transformed"

# COMMAND ----------

# DBTITLE 1,SCD Type 1 Dimension Processing
if trans_entityType == 'SCD-1':

    #Set target Entity Path
    targetEntityPath = transformed_layer_root_path + targetSubjectName + "/" + targetEntityName + "/"

    print('Post Processing SCD-1 Dimension')
    print("Please ensure 'businesskeys' variable is initialised to begin processing\n")
    print("Else SCD-1 Dimension transform processing will fail")
    assert businesskeys is not None, "The variable - businesskeys is not initialised"
    print('businesskeys variable is initialised. Proceeding with validations\n')

    #1. Check if the BusinessKey column is present
    assert 'BusinessKey' in transformedDf.columns, f"No 'BusinessKey' column found in the SCD-1 Dimension Table {targetSubjectName}.{targetEntityName}\n"
    print('Stage 1 complete: BusinessKey column found in the dimension!\n')

    #2. Check if all records are unique
    count_values = transformedDf.groupBy('BusinessKey').agg(count('*').alias('Count')).select('Count').collect()
    assert all(count_values['Count'] <= 1 for count_values in count_values), "AssertionError: Count for some records is greater than 1"
    print('Stage 2 complete: All rows are unique!\n')

    #3. Check all string columns do not contain any null values
    string_columns = [col_name for col_name, data_type in transformedDf.dtypes if data_type == 'string']
    for col_name in string_columns:
        assert transformedDf.filter(col(col_name).isNull()).count() == 0, f"Null values found in column {col_name} in the entity"
    print('Stage 3 complete: No string columns not contain any null values\n')
        
    #4. Check BusinessKeys are populated correctly
    #4.1: Split the businesskeys string into a list of column names
    transformedValidationCopyDf = transformedDf.select('*')
    columns_to_concat = [col_name.strip() for col_name in businesskeys.split(';') if col_name.strip() != '']
    #4.2: Concatenate the specified columns with their values in the specified format
    concatenated_column = concat_ws(';', *[when(transformedValidationCopyDf[col_name].isNotNull(), concat(lit(f"{col_name}="), transformedValidationCopyDf[col_name])).otherwise(lit('')) for col_name in columns_to_concat])
    #4.3: Add a new column with the concatenated value
    result = transformedValidationCopyDf.withColumn("ConcatenatedBusinessKey", concatenated_column)
    assert result.select('BusinessKey', 'ConcatenatedBusinessKey').filter(col('BusinessKey') != col('ConcatenatedBusinessKey')).count() == 0, "BusinessKey column is not populated correctly for this entity. Please fix the error."
    print('Stage 4 complete: BusinessKey column is populated correctly!\n')


    #Enter entity metadata into the [METADATA].[ColumnDetails] table
    _ = fn_updateMetadataInControlDB(transformedDf, targetEntityID)

    #Check if an 'ID' column exists in the Entity Table
    id_columnName = fn_findIDColumnInEntity(transformedDf, targetEntityName)
    if id_columnName is not None:
        id_col_update_stmt = f"UPDATE METADATA.Entity SET IdColumn = '{id_columnName}' WHERE EntityID = {targetEntityID}"
        fn_execQueryInSQLControlDB(id_col_update_stmt)
        print('ID Column Updated in the METADATA.Entity Table')


    #Create a new table
    spark.sql(f'CREATE DATABASE IF NOT EXISTS {targetSubjectName}')
    spark.sql(f'USE DATABASE {targetSubjectName}')
    transformedDf.write.mode('overwrite').option("overwriteSchema", "true").format("delta").option("path", targetEntityPath).saveAsTable(f"{targetSubjectName}.{targetEntityName}")
    spark.sql(f"REFRESH TABLE {targetSubjectName}.{targetEntityName}")

    print('Transformed SCD-1 Dimension Entity has been created')
    print('Delta table created in the target location')
    print('SCD-1 Dimension Transformed processing complete!!!')

# COMMAND ----------

# DBTITLE 1,SCD Type 2 Dimension Processing
if trans_entityType == 'SCD-2':

    #Set target Entity Path
    targetEntityPath = transformed_layer_root_path + targetSubjectName + "/" + targetEntityName + "/"


    print('Post Processing SCD-2 Dimension')
    print("Please ensure 'businesskeys' variable is initialised to begin processing")
    print("Else SCD-2 Dimension transform processing will fail")
    assert businesskeys is not None, "The variable - businesskeys is not initialised"
    print('businesskeys variable is initialised. Proceeding with validations\n')

    #1. Check if the BusinessKey column is present
    assert 'BusinessKey' in transformedDf.columns, f"No 'BusinessKey' column found in the SCD-1 Dimension Table {targetSubjectName}.{targetEntityName}\n"
    print('Stage 1 complete: BusinessKey column found in the dimension!\n')


    #Check if ValidFrom field is present
    assert "ValidFromDWLoadDate" in transformedDf.columns, f"No 'ValidFromDWLoadDate' column found in the SCD-1 Dimension Table {targetSubjectName}.{targetEntityName}\n"

    #2. Check if ValidToDWLoadDate field is present
    if 'ValidToDWLoadDate' in transformedDf.columns:
        print('ValidToDWLoadDate column is also present in the entity')
    else:
        print('ValidToDWLoadDate not found in the list of columns. This will be created automatically during SCD-2 dimension processing step')

    print('Stage 2 complete: ValidFromDWLoadDate column found in the dimension')

    #3. Check if there's only one valid record for a given business key
    count_values = transformedDf.groupBy('BusinessKey','ValidFromDWLoadDate').agg(count('*').alias('Count')).select('Count').collect()
    assert all(count_values['Count'] <= 1 for count_values in count_values), "AssertionError: Count for some records is greater than 1"
    print('Stage 3 complete: All BusinessKeys have only 1 valid record!\n')


    #4. Check all string columns do not contain any null values
    string_columns = [col_name for col_name, data_type in transformedDf.dtypes if data_type == 'string']
    for col_name in string_columns:
        assert transformedDf.filter(col(col_name).isNull()).count() == 0, f"Null values found in column {col_name} in the entity"
    print('Stage 4 complete: No string columns not contain any null values\n')


    #5. Check BusinessKeys are populated correctly
    #5.1: Split the businesskeys string into a list of column names
    transformedValidationCopyDf = transformedDf.select('*')
    columns_to_concat = [col_name.strip() for col_name in businesskeys.split(';') if col_name.strip() != '']
    #5.2: Concatenate the specified columns with their values in the specified format
    concatenated_column = concat_ws(';', *[when(transformedValidationCopyDf[col_name].isNotNull(), concat(lit(f"{col_name}="), transformedValidationCopyDf[col_name])).otherwise(lit('')) for col_name in columns_to_concat])
    #5.3: Add a new column with the concatenated value
    result = transformedValidationCopyDf.withColumn("ConcatenatedBusinessKey", concatenated_column)
    assert result.select('BusinessKey', 'ConcatenatedBusinessKey').filter(col('BusinessKey') != col('ConcatenatedBusinessKey')).count() == 0, "BusinessKey column is not populated correctly for this entity. Please fix the error."
    print('Stage 5 complete: BusinessKey column is populated correctly!\n')
    

    #Enter entity metadata into the [METADATA].[ColumnDetails] table
    _ = fn_updateMetadataInControlDB(transformedDf, targetEntityID)

    #Check if an 'ID' column exists in the Entity Table
    id_columnName = fn_findIDColumnInEntity(transformedDf, targetEntityName)
    if id_columnName is not None:
        id_col_update_stmt = f"UPDATE METADATA.Entity SET IdColumn = '{id_columnName}' WHERE EntityID = {targetEntityID}"
        fn_execQueryInSQLControlDB(id_col_update_stmt)
        print('ID Column Updated in the METADATA.Entity Table')


    #Create a new table
    spark.sql(f'CREATE DATABASE IF NOT EXISTS {targetSubjectName}')
    spark.sql(f'USE DATABASE {targetSubjectName}')
    transformedDf.write.mode('overwrite').option("overwriteSchema", "true").format("delta").option("path", targetEntityPath).saveAsTable(f"{targetSubjectName}.{targetEntityName}")
    spark.sql(f"REFRESH TABLE {targetSubjectName}.{targetEntityName}")

    print('Transformed SCD-2 Dimension Entity has been created')
    print('Delta table created in the target location')
    print('SCD-2 Dimension Transformed processing complete!!!')

# COMMAND ----------

# DBTITLE 1,Dimension Table processing
if trans_entityType == 'Fact':
    import re

    #Set target Entity Path
    targetEntityPath = transformed_layer_root_path + targetSubjectName + "/" + targetEntityName + "/"


    print('Post Processing Fact table')
    print("Please ensure 'transformedDf' variable is initialised to begin processing")
    print("FACT transform processing will fail")
    assert transformedDf is not None, "The variable - transformedDf is not initialised"
    print("Please make sure the 'transformedDf' dataframe is initialised. Proceeding with validations\n")


    #1. Check if ValidFrom field is present
    assert "ValidFromDWLoadDate" in transformedDf.columns, f"No 'ValidFromDWLoadDate' column found in the SCD-1 Dimension Table {targetSubjectName}.{targetEntityName}\n"
    print('Stage 1 complete: ValidFromDWLoadDate column is present in the Fact Entity.')

    #2. Check if there are any foregin values to get mapped to during runtime
    fk_count = 0
    for col_name in transformedDf.columns:
        if re.search('FK_.*_ID', col_name):
            fk_count += 1
            print(f"Foregin Key relation found: {col_name.split('_')[1]}")
    if fk_count > 0:
        print(f'\n\nForeign Key checks complete. {fk_count} foreign key relations found to dimension tables')
    else:
        print('No ForeignKey relations found')
    print('Stage 2 complete: ForeignKey relations checked and validated')

    #3. Check for duplicate records in the Fact table
    assert transformedDf.count() == transformedDf.distinct().count() , 'Fact entity - {targetEntityName} contains duplicate records. Please ensure that they are removed from the transformations!'
    print('Step 3 complete: No duplicate records found in the transformed entity.')


    #Enter entity metadata into the [METADATA].[ColumnDetails] table
    _ = fn_updateMetadataInControlDB(transformedDf, targetEntityID)

    #Create a new table
    spark.sql(f'CREATE DATABASE IF NOT EXISTS {targetSubjectName}')
    spark.sql(f'USE DATABASE {targetSubjectName}')
    transformedDf.write.mode('overwrite').option("overwriteSchema", "true").format("delta").option("path", targetEntityPath).saveAsTable(f"{targetSubjectName}.{targetEntityName}")
    spark.sql(f"REFRESH TABLE {targetSubjectName}.{targetEntityName}")

    print('Transformed Fact Entity has been created')
    print('Delta table created in the target location')
    print('Fact table Transformed processing complete!!!')
