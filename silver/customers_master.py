# Databricks notebook source
# MAGIC %md
# MAGIC #### Create enriched customers table
# MAGIC - Handle null values
# MAGIC - Correct data types
# MAGIC - Data quality checks and correct any schema issues

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Configuration Parameters

# COMMAND ----------

silver_location = "dbfs:/FileStore/assignment/silver/customers_master"

# Table name
bronze_table_name = "bronze.customers"
table_name = "silver.customers_master"

# COMMAND ----------

dbutils.widgets.text("bronze_processing_date", "2024-07-14")
bronze_processing_date = dbutils.widgets.get('bronze_processing_date')

# COMMAND ----------

# Retrieve the currently added data

from pyspark.sql.functions import to_date, col

bronze_customers_df = (spark.read
             .table(bronze_table_name)
             .filter(to_date(col("_processing_date")) == f'{bronze_processing_date}')
            )

# COMMAND ----------

# MAGIC %md 
# MAGIC 1. Do data quality checks

# COMMAND ----------

data_dq_check = bronze_customers_df.filter(col("customer_id").isNull())
data_dq_clean = bronze_customers_df.subtract(data_dq_check)
print("Rows with null customer ids: ", data_dq_check.count())

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Data Deduplication

# COMMAND ----------

from pyspark.sql.functions import to_date, row_number
from pyspark.sql import Window

#Assumption [customer_id] as a primary key
data_window_spec = Window.partitionBy("customer_id").orderBy(to_date(col("_processing_date")).desc())
latest_customers = (data_dq_clean
                   .withColumn("row_number", row_number().over(data_window_spec))
                   .filter("row_number = 1").drop("row_number"))

# COMMAND ----------

# MAGIC %md
# MAGIC Normalize Customer Name and Phone number

# COMMAND ----------

# MAGIC %run ../helper/functions

# COMMAND ----------

final_df = normalize_customer_name(
  normalize_phone_no(latest_customers, "phone"), "customer_name")

# COMMAND ----------

# MAGIC %md
# MAGIC Add metadata column

# COMMAND ----------

from pyspark.sql.functions import when, date_trunc, current_timestamp

processing_date = date_trunc('second', current_timestamp())
final_df = final_df.withColumn("_processing_date", processing_date)

# COMMAND ----------

# MAGIC %md
# MAGIC Merge into Silver Table

# COMMAND ----------

from delta.tables import DeltaTable

# check if the silverLocation contain the delta table
if(DeltaTable.isDeltaTable(spark, silver_location)): 

    DeltaTable.forPath(spark, silver_location).alias("target").merge(
        source = final_df.alias("src"),
        condition = "src.customer_id = target.customer_id"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll().execute()
else:
    # If no, save the file to silverLocation
    final_df.write.mode("overwrite").format("delta").save(silver_location)

# COMMAND ----------

# create the database and table, if required

spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING delta LOCATION '{silver_location}'")
