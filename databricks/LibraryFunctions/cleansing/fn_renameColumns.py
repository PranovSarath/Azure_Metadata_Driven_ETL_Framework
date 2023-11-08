# Databricks notebook source
def fn_renameColumns(df, replacement_char= "_"):
    cleaned_df = df
    for column_name in df.columns:
        # Remove spaces and invalid characters
        cleaned_column_name = ''.join(c if c.isalnum() or c in ['_', '-', '.'] else replacement_char for c in column_name.strip(' '))
        
        # Rename the column
        cleaned_df = cleaned_df.withColumnRenamed(column_name, cleaned_column_name)
    return cleaned_df
