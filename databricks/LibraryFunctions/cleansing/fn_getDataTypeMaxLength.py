# Databricks notebook source
def fn_getDataTypeMaxLength(data_type):
    if data_type == StringType():
        return 255  # A common default length for string types
    elif data_type in [IntegerType(), DoubleType(), LongType()]:
        return None  # Numeric types typically do not have a maximum length
    elif TimestampType():
        return None
    else:
        None
