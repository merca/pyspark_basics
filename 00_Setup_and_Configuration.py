# Databricks notebook source
# MAGIC %md
# MAGIC # 00. Setup and Configuration - Python/PySpark in Databricks
# MAGIC 
# MAGIC Welcome to Python and PySpark fundamentals for Data Engineers and Analysts!
# MAGIC 
# MAGIC This notebook covers:
# MAGIC - Understanding the Databricks environment
# MAGIC - SparkSession basics
# MAGIC - Configuration and best practices
# MAGIC - Environment setup for the course

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Understanding Databricks Environment
# MAGIC 
# MAGIC In Databricks, several things are pre-configured for you:
# MAGIC - **SparkSession** is already available as `spark`
# MAGIC - **SparkContext** is available as `sc`
# MAGIC - Common libraries are pre-installed
# MAGIC - Cluster resources are managed automatically

# COMMAND ----------

# Let's explore what's available in our environment
print("Python version:")
import sys
print(sys.version)

print("\nSpark version:")
print(spark.version)

print("\nSpark configuration (first 10 items):")
for conf in list(spark.sparkContext.getConf().getAll())[:10]:
    print(f"{conf[0]}: {conf[1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. SparkSession - Your Gateway to Spark
# MAGIC 
# MAGIC **For SQL users**: Think of SparkSession as your database connection, but much more powerful!
# MAGIC 
# MAGIC - It's your entry point to all Spark functionality
# MAGIC - Handles DataFrame creation, SQL execution, and configuration
# MAGIC - In Databricks, it's pre-created as `spark`

# COMMAND ----------

# The spark variable is your SparkSession
print(f"SparkSession: {spark}")
print(f"Application Name: {spark.sparkContext.appName}")
print(f"Master: {spark.sparkContext.master}")

# You can also create your own SparkSession (usually not needed in Databricks)
from pyspark.sql import SparkSession

# This is what happens behind the scenes in Databricks:
# spark = SparkSession.builder \
#     .appName("MyApp") \
#     .config("spark.sql.adaptive.enabled", "true") \
#     .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Essential Imports and Setup
# MAGIC 
# MAGIC Let's import the libraries we'll use throughout the course:

# COMMAND ----------

# Core PySpark imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *  # This imports all SQL functions
from pyspark.sql.types import *      # This imports all data types

# Standard Python libraries
import pandas as pd
from datetime import datetime, date
import json

print("✅ All imports successful!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Working with Databricks File System (DBFS)
# MAGIC 
# MAGIC DBFS is Databricks' distributed file system. Think of it as your data warehouse file system.

# COMMAND ----------

# List what's in the root DBFS
display(dbutils.fs.ls("/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Creating Sample Data for Learning
# MAGIC 
# MAGIC Let's create some sample datasets that we'll use throughout the notebooks:

# COMMAND ----------

# Create sample employee data - this is like a reference table in SQL
employees_data = [
    (1, "John Doe", "Engineering", "Senior Engineer", 95000, date(2020, 1, 15)),
    (2, "Jane Smith", "Marketing", "Marketing Manager", 75000, date(2019, 3, 10)),
    (3, "Mike Johnson", "Engineering", "Data Engineer", 85000, date(2021, 6, 1)),
    (4, "Sarah Wilson", "Sales", "Sales Rep", 65000, date(2022, 2, 20)),
    (5, "Tom Brown", "Engineering", "Senior Engineer", 98000, date(2018, 11, 5)),
    (6, "Lisa Davis", "Marketing", "Analyst", 55000, date(2023, 1, 10)),
    (7, "David Miller", "Sales", "Sales Manager", 80000, date(2020, 8, 15)),
    (8, "Amy Taylor", "Engineering", "Data Scientist", 105000, date(2021, 4, 3)),
    (9, "Robert Garcia", "Sales", "Sales Rep", 62000, date(2022, 9, 12)),
    (10, "Emily Rodriguez", "Marketing", "Marketing Specialist", 58000, date(2023, 3, 8))
]

# Define schema (like CREATE TABLE statement in SQL)
employee_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("position", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("hire_date", DateType(), True)
])

# Create DataFrame
employees_df = spark.createDataFrame(employees_data, employee_schema)

# Display the data (like SELECT * FROM employees)
display(employees_df)

# COMMAND ----------

# Create sample sales data - this is like a transaction table
sales_data = [
    (101, 1, "2023-01-15", 1200.50, "Product A"),
    (102, 4, "2023-01-16", 850.00, "Product B"),
    (103, 7, "2023-01-17", 2300.75, "Product C"),
    (104, 9, "2023-01-18", 650.25, "Product A"),
    (105, 4, "2023-01-19", 1800.00, "Product B"),
    (106, 7, "2023-01-20", 950.50, "Product A"),
    (107, 9, "2023-01-21", 1350.25, "Product C"),
    (108, 4, "2023-02-01", 1150.00, "Product B"),
    (109, 7, "2023-02-02", 2100.75, "Product A"),
    (110, 9, "2023-02-03", 875.50, "Product C")
]

sales_schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("employee_id", IntegerType(), True),
    StructField("sale_date", StringType(), True),  # We'll convert this to date later
    StructField("amount", DoubleType(), True),
    StructField("product", StringType(), True)
])

sales_df = spark.createDataFrame(sales_data, sales_schema)

# Convert string to date (like CAST in SQL)
sales_df = sales_df.withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd"))

display(sales_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Creating Temporary Views (SQL Bridge)
# MAGIC 
# MAGIC **For SQL users**: This is like creating views in your database!
# MAGIC 
# MAGIC Temporary views allow you to use SQL syntax with DataFrames:

# COMMAND ----------

# Create temporary views - now you can use SQL!
employees_df.createOrReplaceTempView("employees")
sales_df.createOrReplaceTempView("sales")

print("✅ Temporary views created!")
print("You can now use SQL queries like:")
print("SELECT * FROM employees")
print("SELECT * FROM sales")

# COMMAND ----------

# Test with SQL - this should feel familiar!
spark.sql("""
    SELECT department, COUNT(*) as employee_count, AVG(salary) as avg_salary
    FROM employees 
    GROUP BY department 
    ORDER BY avg_salary DESC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Checking Your Setup
# MAGIC 
# MAGIC Let's verify everything is working correctly:

# COMMAND ----------

# Verification checklist
checks = []

try:
    # Check Spark session
    assert spark is not None
    checks.append("✅ SparkSession available")
except:
    checks.append("❌ SparkSession not available")

try:
    # Check DataFrame operations
    count = employees_df.count()
    assert count == 10
    checks.append(f"✅ DataFrame operations working ({count} records)")
except:
    checks.append("❌ DataFrame operations failed")

try:
    # Check SQL operations
    result = spark.sql("SELECT COUNT(*) as count FROM employees").collect()[0].count
    assert result == 10
    checks.append(f"✅ SQL operations working ({result} records)")
except:
    checks.append("❌ SQL operations failed")

try:
    # Check PySpark functions
    result = employees_df.select(avg("salary")).collect()[0][0]
    checks.append(f"✅ PySpark functions working (avg salary: ${result:,.2f})")
except:
    checks.append("❌ PySpark functions failed")

# Display results
for check in checks:
    print(check)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Best Practices and Tips
# MAGIC 
# MAGIC ### For SQL Users Transitioning to PySpark:
# MAGIC 
# MAGIC 1. **DataFrames are like tables** - you can think of them as distributed tables
# MAGIC 2. **Lazy evaluation** - operations aren't executed until you call an action (like `.show()`, `.collect()`, `.count()`)
# MAGIC 3. **Immutability** - DataFrames don't change; operations create new DataFrames
# MAGIC 4. **Use SQL when comfortable** - you can always fall back to `spark.sql()`
# MAGIC 5. **Chain operations** - like building a complex query with CTEs

# COMMAND ----------

# Example of lazy evaluation
print("Creating a filtered DataFrame (lazy operation)...")
filtered_df = employees_df.filter(col("salary") > 80000)
print("✅ Filter created (but not executed yet)")

print("\nExecuting the operation with .show()...")
filtered_df.show()
print("✅ Now the operation was executed")

# COMMAND ----------

# Example of immutability
print("Original DataFrame count:", employees_df.count())

# This creates a NEW DataFrame, doesn't modify the original
new_df = employees_df.withColumn("salary_category", 
                                when(col("salary") > 90000, "High")
                                .when(col("salary") > 70000, "Medium")
                                .otherwise("Low"))

print("Original DataFrame count (unchanged):", employees_df.count())
print("New DataFrame count:", new_df.count())

# Show the new column
new_df.select("name", "salary", "salary_category").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Common Configuration Settings
# MAGIC 
# MAGIC Here are some useful Spark configurations:

# COMMAND ----------

# View current important configurations
important_configs = [
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.serializer"
]

print("Current Spark Configuration:")
for config in important_configs:
    try:
        value = spark.conf.get(config)
        print(f"{config}: {value}")
    except:
        print(f"{config}: Not set")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Memory and Performance Tips
# MAGIC 
# MAGIC ### Key concepts for performance:
# MAGIC - **Partitions**: How your data is divided across the cluster
# MAGIC - **Caching**: Storing frequently used data in memory
# MAGIC - **Broadcast**: Sending small tables to all nodes

# COMMAND ----------

# Check DataFrame partitions
print(f"Employees DataFrame partitions: {employees_df.rdd.getNumPartitions()}")
print(f"Sales DataFrame partitions: {sales_df.rdd.getNumPartitions()}")

# Cache a DataFrame if you'll use it multiple times
employees_df.cache()
print("✅ Employees DataFrame cached")

# Force the cache by triggering an action
count = employees_df.count()
print(f"Cached DataFrame count: {count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC You now have:
# MAGIC - ✅ Understanding of the Databricks environment
# MAGIC - ✅ Sample datasets loaded (`employees_df`, `sales_df`)
# MAGIC - ✅ Temporary views created (`employees`, `sales`)
# MAGIC - ✅ Basic configuration knowledge
# MAGIC - ✅ Performance tips and best practices
# MAGIC 
# MAGIC **Next Steps**: Move on to the Python Basics notebook to learn Python fundamentals with a SQL perspective!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Reference - Cheat Sheet
# MAGIC 
# MAGIC | SQL Concept | PySpark Equivalent | Example |
# MAGIC |-------------|-------------------|---------|
# MAGIC | SELECT * FROM table | df.show() | `employees_df.show()` |
# MAGIC | SELECT col1, col2 FROM table | df.select("col1", "col2") | `employees_df.select("name", "salary")` |
# MAGIC | WHERE condition | df.filter() or df.where() | `employees_df.filter(col("salary") > 80000)` |
# MAGIC | GROUP BY | df.groupBy() | `employees_df.groupBy("department").count()` |
# MAGIC | ORDER BY | df.orderBy() | `employees_df.orderBy("salary")` |
# MAGIC | CREATE VIEW | df.createOrReplaceTempView() | `employees_df.createOrReplaceTempView("emp")` |
# MAGIC | COUNT(*) | df.count() | `employees_df.count()` |