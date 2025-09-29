# Databricks notebook source
# MAGIC %md
# MAGIC # 00. Setup and Configuration - Python/PySpark in Databricks
# MAGIC 
# MAGIC Welcome to Python and PySpark fundamentals for Data Engineers and Analysts!
# MAGIC 
# MAGIC This notebook covers:
# MAGIC - Understanding the Databricks environment (Classic & Serverless)
# MAGIC - SparkSession basics
# MAGIC - Serverless compute considerations
# MAGIC - Environment setup for the course

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Understanding Databricks Environment
# MAGIC 
# MAGIC In Databricks, several things are pre-configured for you:
# MAGIC - **SparkSession** is already available as `spark`
# MAGIC - **SparkContext** is available as `sc` (Classic clusters only)
# MAGIC - Common libraries are pre-installed
# MAGIC - Cluster resources are managed automatically
# MAGIC 
# MAGIC ### Serverless vs Classic Compute:
# MAGIC - **Serverless**: Managed, auto-scaling, limited configuration access, no RDD operations
# MAGIC - **Classic**: Full control, custom configurations, RDD support, manual scaling

# COMMAND ----------

# Let's explore what's available in our environment
print("Python version:")
import sys
print(sys.version)

print("\nSpark version:")
print(spark.version)

# Check if we're on serverless (limited configuration access)
try:
    print("\nSpark configuration (first 10 items):")
    # This may not work on serverless compute
    for conf in list(spark.sparkContext.getConf().getAll())[:10]:
        print(f"{conf[0]}: {conf[1]}")
except Exception as e:
    print(f"\nConfiguration access limited (likely serverless): {type(e).__name__}")
    print("✅ This is expected on serverless compute - configurations are managed automatically")

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

# These may be limited on serverless compute
try:
    print(f"Application Name: {spark.sparkContext.appName}")
    print(f"Master: {spark.sparkContext.master}")
except Exception as e:
    print(f"SparkContext access limited (serverless): {type(e).__name__}")
    print("✅ This is expected on serverless - Spark context is managed automatically")

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
    result = spark.sql("SELECT COUNT(*) as count FROM employees").collect()[0]["count"]
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
# MAGIC 
# MAGIC ### Serverless-Specific Best Practices:
# MAGIC 
# MAGIC 6. **Avoid RDD operations** - Use DataFrame/SQL APIs instead
# MAGIC 7. **Use mapInPandas/mapInArrow** - For custom operations instead of RDD transformations
# MAGIC 8. **Focus on DataFrame operations** - Configuration tuning is handled automatically

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
# MAGIC ### Serverless-Compatible Custom Operations
# MAGIC 
# MAGIC Instead of RDD operations, use DataFrame operations and SQL for transformations:

# COMMAND ----------

# Example 1: Custom salary calculations using DataFrame operations (always works)
print("Method 1: Using DataFrame operations (serverless-compatible)")
employees_with_bonus = employees_df \
    .withColumn("bonus", col("salary") * 0.10) \
    .withColumn("total_compensation", col("salary") + col("bonus"))

employees_with_bonus.select("name", "salary", "bonus", "total_compensation").show()

# COMMAND ----------

# Example 2: Using SQL for the same calculation (always works)
print("Method 2: Using SQL (serverless-compatible)")
employees_df.createOrReplaceTempView("emp_bonus")

bonus_sql = spark.sql("""
    SELECT 
        name,
        salary,
        salary * 0.10 as bonus,
        salary + (salary * 0.10) as total_compensation
    FROM emp_bonus
""")

bonus_sql.show()

# COMMAND ----------

# Example 3: mapInPandas for more complex operations (if needed)
print("Method 3: mapInPandas for complex operations (serverless-compatible)")

# Simple example that's guaranteed to work
def add_bonus_pandas(iterator):
    """Process data using pandas - works on serverless"""
    for pdf in iterator:
        # Simple pandas operations
        pdf['bonus'] = pdf['salary'] * 0.15  # 15% bonus
        pdf['tax_estimate'] = pdf['salary'] * 0.25  # 25% tax estimate
        yield pdf

# Use mapInPandas with iterator (more reliable)
try:
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType
    
    # Schema for the result
    bonus_schema = employees_df.schema.add("bonus", DoubleType()).add("tax_estimate", DoubleType())
    
    # Apply transformation
    pandas_result = employees_df.mapInPandas(add_bonus_pandas, schema=bonus_schema)
    pandas_result.select("name", "salary", "bonus", "tax_estimate").show(5)
    print("✅ mapInPandas worked successfully!")
    
except Exception as e:
    print(f"mapInPandas not available: {type(e).__name__}")
    print("✅ This is expected on some serverless configurations")
    print("   ➡️ Use DataFrame operations (Method 1) or SQL (Method 2) instead")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Configuration Settings (Classic vs Serverless)
# MAGIC 
# MAGIC **Serverless**: Configurations are managed automatically, limited access
# MAGIC 
# MAGIC **Classic**: Full configuration control available

# COMMAND ----------

# View current important configurations (when available)
important_configs = [
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled", 
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.serializer"
]

print("Spark Configuration Access:")
try:
    for config in important_configs:
        try:
            value = spark.conf.get(config)
            print(f"{config}: {value}")
        except:
            print(f"{config}: Not accessible")
except Exception as e:
    print(f"Configuration access limited: {type(e).__name__}")
    print("✅ On serverless, configurations are optimized automatically")
    print("   ➡️ Focus on DataFrame/SQL operations instead of low-level tuning")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Performance Tips (Serverless-Compatible)
# MAGIC 
# MAGIC ### Key concepts for performance:
# MAGIC - **Caching**: Storing frequently used data in memory
# MAGIC - **DataFrame Operations**: Use DataFrame/SQL instead of RDDs on serverless
# MAGIC - **Lazy Evaluation**: Operations are optimized before execution

# COMMAND ----------

# Check if we can access partition information (Classic compute)
try:
    print(f"Employees DataFrame partitions: {employees_df.rdd.getNumPartitions()}")
    print(f"Sales DataFrame partitions: {sales_df.rdd.getNumPartitions()}")
    print("✅ Running on Classic compute - RDD access available")
except Exception as e:
    print(f"RDD access not available: {type(e).__name__}")
    print("✅ Running on Serverless compute - RDD operations not supported")
    print("   ➡️ Use DataFrame operations and SQL for optimal performance")

# Caching operations (different support on Classic vs Serverless)
print("\n=== Caching Performance Test ===")
try:
    # This works on Classic compute but may have limitations on Serverless
    employees_df.cache()
    print("✅ Employees DataFrame cached (Classic compute)")
    
    # Force the cache by triggering an action
    count = employees_df.count()
    print(f"Cached DataFrame count: {count}")
    
except Exception as e:
    print(f"❌ Caching operation failed: {type(e).__name__}")
    print("✅ This is expected on some serverless configurations")
    print("   ➡️ Serverless automatically manages memory optimization")
    
    # Get count without explicit caching
    count = employees_df.count()
    print(f"DataFrame count (no explicit cache): {count}")

# Alternative ways to check DataFrame structure (serverless-compatible)
print(f"\nDataFrame schema columns: {len(employees_df.columns)}")
print(f"DataFrame estimated size: {employees_df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC You now have:
# MAGIC - ✅ Understanding of the Databricks environment (Classic & Serverless)
# MAGIC - ✅ Sample datasets loaded (`employees_df`, `sales_df`)
# MAGIC - ✅ Temporary views created (`employees`, `sales`)
# MAGIC - ✅ Serverless-compatible operations and best practices
# MAGIC - ✅ Performance tips for both compute types
# MAGIC - ✅ Custom operations using mapInPandas (serverless-friendly)
# MAGIC 
# MAGIC **Next Steps**: Move on to the Python Basics notebook to learn Python fundamentals with a SQL perspective!
# MAGIC 
# MAGIC **Remember**: This notebook works on both Classic and Serverless compute, with automatic fallbacks for unsupported operations.

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