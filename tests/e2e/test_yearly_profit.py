# Databricks notebook source
# MAGIC %pip install pytest==8.2.2
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import pytest
from pyspark.sql import Row
from pyspark.sql.functions import col
from unittest.mock import patch

# COMMAND ----------

# MAGIC %run ../../gold/profit_by_year

# COMMAND ----------

# Create a DataFrame to be returned by the mock
@pytest.fixture
def fake_orders_data():
    data = [
        Row(customer_id="1", order_date="2022-01-01 00:00:00", profit="10"),
        Row(customer_id="2", order_date="2022-02-01 00:00:00", profit="30"),
        Row(customer_id="3", order_date="2023-01-01 00:00:00", profit="40"),
        Row(customer_id="1", order_date="2023-02-01 00:00:00", profit="100"),
        Row(customer_id="1", order_date="2023-03-01 00:00:00", profit="80"),
    ]
    return spark.createDataFrame(data)

# Test function to check if run_code works as expected
def test_e2e_yearly_profit(fake_orders_data):
    # Mock the upstream table data to return the fake_data
    with patch('getOrdersData', return_value=fake_orders_data):
        expected_data = [
            Row(year='2022', profit_by_year='40'),
            Row(year='2022', profit_by_year='220'),
        ]
        
        # Run the code end to end
        result_data = generateYearlyProfit(spark)
        
        # Convert result_data to a DataFrame to make it comparable
        result_df = spark.createDataFrame(result_data)
        
        # Sort both dataframes by user_id before comparison to avoid order issues
        expected_df = spark.createDataFrame(expected_data).orderBy("year")
        result_sorted = result_df.orderBy("year")

        # Assert that the transformed data is as expected
        assert result_sorted.collect() == expected_df.collect(), "The transformed data does not match expected output"

test_e2e_yearly_profit(fake_orders_data)
