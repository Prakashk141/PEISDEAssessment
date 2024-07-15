# Databricks notebook source
# MAGIC %md
# MAGIC ##### Configuration Parameters

# COMMAND ----------

gold_location = "dbfs:/FileStore/assignment/gold/profit_by_year"

# Table name
silver_table_name = "silver.enriched_orders"
table_name = "gold.profit_by_year"

# COMMAND ----------

dbutils.widgets.text("silver_processing_date", "2024-07-14")
bronze_processing_date = dbutils.widgets.get('silver_processing_date')

# COMMAND ----------

# Retrieve the currently added data

from pyspark.sql.functions import to_date, col

def getOrdersData():
  return (spark.read
          .table(silver_table_name)
        )
  
enriched_order_df = getOrdersData()

# COMMAND ----------

from pyspark.sql.functions import year
from pyspark.sql.functions import sum, round

# COMMAND ----------

def generateYearlyProfit(input_df):
  year_col_df = input_df.withColumn("year", year(col("order_date")))
  return (year_col_df
            .groupby(col("year"))
            .agg(round(sum(col("profit")), 2).alias("profit_by_year"))
          )

# COMMAND ----------

final_df = generateYearlyProfit(enriched_order_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Create gold layer table

# COMMAND ----------

from delta.tables import DeltaTable

# check if the silverLocation contain the delta table
if(DeltaTable.isDeltaTable(spark, gold_location)): 

    DeltaTable.forPath(spark, gold_location).alias("target").merge(
        source = final_df.alias("src"),
        condition = "src.year = target.year"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll().execute()
else:
    # If no, save the file to silverLocation
    final_df.write.mode("overwrite").format("delta").save(gold_location)

# COMMAND ----------

# create the database and table, if required

spark.sql("CREATE DATABASE IF NOT EXISTS gold")
spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING delta LOCATION '{gold_location}'")
