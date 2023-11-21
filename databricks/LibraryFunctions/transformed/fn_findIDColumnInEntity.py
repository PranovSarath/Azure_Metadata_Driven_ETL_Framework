# Databricks notebook source
def fn_findIDColumnInEntity(df, targetEntityName):
    cleanedEntityName = targetEntityName.replace('Dim','').replace('dim','').replace('_', '').lower()
    columnsList = list(df.columns)
    ID_idx = None
    for idx, col_name in enumerate(columnsList):
        if f"{cleanedEntityName}id" == col_name.lower():
            ID_idx = idx
            break
    if ID_idx is not None:
        return columnsList[ID_idx]
    else:
        return None
