# Databricks notebook source
# function to check if table are created after each layer
def tableExists(tableName, dbName):
  return spark.catalog.tableExists(f"{dbName}.{tableName}")

# COMMAND ----------

import re

# Normalize column names across the database
def normalizeColumnNames(df):
  return df.toDF(*[re.sub('[ ,-;{}()\n\t="]', '_', c.lower()) for c in df.columns])
  
