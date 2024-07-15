# Databricks notebook source
# MAGIC %run ../helper/common

# COMMAND ----------

from pyspark.sql.types import Row, StructType, StructField, StringType

# COMMAND ----------

def test_normalizeColumnNames():
  schema = StructType([
        StructField("Product Name", StringType() ,False),
        StructField("Product-Code", StringType() ,False)
    ])
  expected_schema = StructType([
      StructField("product_name", StringType() ,False),
      StructField("product_code", StringType() ,False)
  ])
  data = [
      Row("Test Product", "1234"),
      Row("Test Product 2", "7896"),
  ]
  input_df = spark.createDataFrame(data, schema)
  
  actual_df = normalizeColumnNames(input_df)
  
  #Assert
  assert actual_df.schema == expected_schema
  print("Test Passed !")

test_normalizeColumnNames()

# COMMAND ----------

def test_tableExists(table, db):
  assert tableExists(table, db) is True

tables = {
  "bronze": ["customers", "orders", "products"],
  "silver": [""],
  "gold": [""]
}
for db in tables:
  for table in tables[db]:
    test_tableExists("customers", "bronze")
