# Databricks notebook source
# MAGIC %md
# MAGIC # 02. PySpark DataFrame Basics
# MAGIC 
# MAGIC This notebook introduces PySpark DataFrames with a focus on SQL equivalents. You'll learn how to work with distributed data using familiar SQL concepts.
# MAGIC 
# MAGIC ## Topics Covered:
# MAGIC - Creating DataFrames (like creating tables)
# MAGIC - DataFrame operations vs SQL operations
# MAGIC - Transformations and Actions (lazy vs eager evaluation)
# MAGIC - Column operations and data types
# MAGIC - Filtering, selecting, and sorting data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup - Import Required Libraries

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from datetime import date, datetime

# Verify our SparkSession is available
print(f"✅ Spark version: {spark.version}")
print(f"✅ SparkSession available: {spark}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Creating DataFrames - Like Creating Tables
# MAGIC 
# MAGIC **SQL Perspective**: Creating a DataFrame in PySpark is like creating a table or view.
# MAGIC You define the schema (column names and types) and populate it with data.

# COMMAND ----------

# Method 1: Create DataFrame from Python data with schema
# This is like: CREATE TABLE employees (id INT, name STRING, ...)

# Sample data
employee_data = [
    (1, "John Doe", "Engineering", "Senior Engineer", 95000, date(2020, 1, 15)),
    (2, "Jane Smith", "Marketing", "Marketing Manager", 75000, date(2019, 3, 10)),
    (3, "Mike Johnson", "Engineering", "Data Engineer", 85000, date(2021, 6, 1)),
    (4, "Sarah Wilson", "Sales", "Sales Rep", 65000, date(2022, 2, 20)),
    (5, "Tom Brown", "Engineering", "Senior Engineer", 98000, date(2018, 11, 5))
]

# Define schema explicitly (like column definitions in CREATE TABLE)
employee_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("position", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("hire_date", DateType(), True)
])

# Create DataFrame
employees_df = spark.createDataFrame(employee_data, employee_schema)

print("✅ DataFrame created!")
print(f"Schema: {employees_df.schema}")

# COMMAND ----------

# Method 2: Create DataFrame without explicit schema (Spark infers types)
# Like creating a table and letting the database infer column types

auto_schema_df = spark.createDataFrame([
    (1, "Product A", 29.99, True),
    (2, "Product B", 49.99, False), 
    (3, "Product C", 19.99, True)
], ["product_id", "product_name", "price", "in_stock"])

print("Auto-inferred schema:")
auto_schema_df.printSchema()

# COMMAND ----------

# Method 3: Create DataFrame from Pandas (common in data analysis)
# Like importing data from CSV or Excel into a database table

pandas_data = pd.DataFrame({
    'order_id': [101, 102, 103, 104],
    'customer': ['Alice', 'Bob', 'Carol', 'David'],
    'amount': [250.0, 175.5, 320.25, 89.99],
    'order_date': ['2023-01-15', '2023-01-16', '2023-01-17', '2023-01-18']
})

# Convert to PySpark DataFrame
orders_df = spark.createDataFrame(pandas_data)
print("✅ DataFrame created from Pandas:")
orders_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Basic DataFrame Information - Like DESCRIBE and SHOW
# MAGIC 
# MAGIC **SQL Perspective**: These operations are like `DESCRIBE TABLE` and `SELECT * FROM table LIMIT n`.

# COMMAND ----------

# Show the data - like SELECT * FROM employees LIMIT 5
print("=== SHOW DATA (like SELECT * FROM employees) ===")
employees_df.show()

# Show only first 3 rows - like LIMIT 3
print("\n=== FIRST 3 ROWS (like LIMIT 3) ===")
employees_df.show(3)

# COMMAND ----------

# Get schema information - like DESCRIBE employees
print("=== SCHEMA (like DESCRIBE employees) ===")
employees_df.printSchema()

# Get column names - like getting column list from information_schema
print("\n=== COLUMN NAMES ===")
print("Columns:", employees_df.columns)

# Get row count - like SELECT COUNT(*) FROM employees
print(f"\n=== ROW COUNT (like COUNT(*)) ===")
print(f"Total employees: {employees_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Selecting Columns - Like SELECT column1, column2
# MAGIC 
# MAGIC **SQL Perspective**: `.select()` is equivalent to the SELECT clause in SQL.

# COMMAND ----------

# Select specific columns - like SELECT name, salary FROM employees
print("=== SELECT name, salary ===")
employees_df.select("name", "salary").show()

# Select with column expressions - like SELECT name, salary * 1.1 AS new_salary
print("\n=== SELECT with calculations ===")
employees_df.select(
    "name", 
    "salary",
    (col("salary") * 1.1).alias("salary_with_raise")
).show()

# COMMAND ----------

# Select all columns - like SELECT *
print("=== SELECT * (all columns) ===")
employees_df.select("*").show(3)

# Select with column transformations - like using functions in SELECT
print("\n=== SELECT with transformations ===")
employees_df.select(
    upper(col("name")).alias("name_upper"),
    col("department"),
    col("salary"),
    year(col("hire_date")).alias("hire_year")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Filtering Data - Like WHERE Clause
# MAGIC 
# MAGIC **SQL Perspective**: `.filter()` or `.where()` are equivalent to the WHERE clause.

# COMMAND ----------

# Filter with simple condition - like WHERE salary > 80000
print("=== WHERE salary > 80000 ===")
high_earners = employees_df.filter(col("salary") > 80000)
high_earners.show()

# Multiple conditions - like WHERE department = 'Engineering' AND salary > 90000
print("\n=== WHERE department = 'Engineering' AND salary > 90000 ===")
senior_engineers = employees_df.filter(
    (col("department") == "Engineering") & (col("salary") > 90000)
)
senior_engineers.show()

# COMMAND ----------

# String filtering - like WHERE name LIKE '%John%'
print("=== WHERE name LIKE '%John%' ===")
johns = employees_df.filter(col("name").contains("John"))
johns.show()

# IN clause equivalent - like WHERE department IN ('Engineering', 'Sales')
print("\n=== WHERE department IN ('Engineering', 'Sales') ===")
tech_and_sales = employees_df.filter(col("department").isin(["Engineering", "Sales"]))
tech_and_sales.show()

# Date filtering - like WHERE hire_date >= '2020-01-01'
print("\n=== WHERE hire_date >= '2020-01-01' ===")
recent_hires = employees_df.filter(col("hire_date") >= date(2020, 1, 1))
recent_hires.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Sorting Data - Like ORDER BY
# MAGIC 
# MAGIC **SQL Perspective**: `.orderBy()` is equivalent to the ORDER BY clause.

# COMMAND ----------

# Sort by single column - like ORDER BY salary
print("=== ORDER BY salary ===")
employees_df.orderBy("salary").show()

# Sort descending - like ORDER BY salary DESC
print("\n=== ORDER BY salary DESC ===")
employees_df.orderBy(col("salary").desc()).show()

# Sort by multiple columns - like ORDER BY department, salary DESC
print("\n=== ORDER BY department, salary DESC ===")
employees_df.orderBy("department", col("salary").desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Adding and Modifying Columns - Like ALTER TABLE ADD COLUMN
# MAGIC 
# MAGIC **SQL Perspective**: `.withColumn()` adds or modifies columns, like calculated fields in SELECT or ALTER TABLE.

# COMMAND ----------

# Add a new column - like adding a calculated field
print("=== Adding calculated columns ===")
enhanced_employees = employees_df.withColumn(
    "annual_bonus", col("salary") * 0.1
).withColumn(
    "salary_category",
    when(col("salary") > 90000, "High")
    .when(col("salary") > 70000, "Medium")
    .otherwise("Low")
).withColumn(
    "years_employed",
    datediff(current_date(), col("hire_date")) / 365.25
)

enhanced_employees.select("name", "salary", "annual_bonus", "salary_category", "years_employed").show()

# COMMAND ----------

# Rename columns - like AS alias in SELECT
print("=== Renaming columns ===")
renamed_df = employees_df.withColumnRenamed("name", "employee_name") \
                        .withColumnRenamed("hire_date", "start_date")

renamed_df.select("employee_name", "start_date").show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Aggregations - Like GROUP BY and Aggregate Functions
# MAGIC 
# MAGIC **SQL Perspective**: `.groupBy()` is equivalent to GROUP BY, and aggregate functions work similarly.

# COMMAND ----------

# Basic aggregations - like SELECT COUNT(*), AVG(salary), MAX(salary), MIN(salary)
print("=== Basic aggregations (like aggregate functions) ===")
employees_df.agg(
    count("*").alias("total_employees"),
    avg("salary").alias("average_salary"),
    max("salary").alias("max_salary"),
    min("salary").alias("min_salary")
).show()

# Group by department - like SELECT department, COUNT(*), AVG(salary) FROM employees GROUP BY department
print("\n=== GROUP BY department ===")
dept_stats = employees_df.groupBy("department").agg(
    count("*").alias("employee_count"),
    avg("salary").alias("avg_salary"),
    max("salary").alias("max_salary")
)
dept_stats.show()

# COMMAND ----------

# More complex grouping - like GROUP BY department, position
print("=== GROUP BY department, position ===")
detailed_stats = employees_df.groupBy("department", "position").agg(
    count("*").alias("count"),
    avg("salary").alias("avg_salary")
).orderBy("department", "position")

detailed_stats.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Working with Null Values - Like IS NULL, COALESCE
# MAGIC 
# MAGIC **SQL Perspective**: Handling nulls in PySpark is similar to SQL NULL handling.

# COMMAND ----------

# Create data with null values for demonstration
data_with_nulls = [
    (1, "Alice", "Engineering", 95000),
    (2, "Bob", None, 85000),  # Null department
    (3, "Carol", "Marketing", None),  # Null salary
    (4, None, "Sales", 75000),  # Null name
    (5, "David", "Engineering", 90000)
]

null_demo_df = spark.createDataFrame(
    data_with_nulls, 
    ["id", "name", "department", "salary"]
)

print("=== Data with nulls ===")
null_demo_df.show()

# COMMAND ----------

# Filter for null values - like WHERE column IS NULL
print("=== WHERE department IS NULL ===")
null_demo_df.filter(col("department").isNull()).show()

# Filter for non-null values - like WHERE column IS NOT NULL
print("\n=== WHERE salary IS NOT NULL ===")
null_demo_df.filter(col("salary").isNotNull()).show()

# Fill null values - like COALESCE or ISNULL
print("\n=== Fill nulls with default values ===")
filled_df = null_demo_df.fillna({
    "name": "Unknown",
    "department": "Unassigned", 
    "salary": 0
})
filled_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Distinct Values - Like SELECT DISTINCT
# MAGIC 
# MAGIC **SQL Perspective**: `.distinct()` and `.dropDuplicates()` work like DISTINCT in SQL.

# COMMAND ----------

# Get distinct departments - like SELECT DISTINCT department FROM employees
print("=== SELECT DISTINCT department ===")
distinct_depts = employees_df.select("department").distinct()
distinct_depts.show()

# Count distinct values - like SELECT COUNT(DISTINCT department)
print("\n=== COUNT(DISTINCT department) ===")
dept_count = employees_df.select("department").distinct().count()
print(f"Number of distinct departments: {dept_count}")

# Remove duplicates based on specific columns
print("\n=== Remove duplicates ===")
employees_df.dropDuplicates(["department"]).select("department", "name").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Understanding Lazy Evaluation
# MAGIC 
# MAGIC **Key Concept**: PySpark uses lazy evaluation - operations aren't executed until you call an "action".
# MAGIC 
# MAGIC - **Transformations** (lazy): select, filter, withColumn, groupBy, etc.
# MAGIC - **Actions** (trigger execution): show, count, collect, write, etc.

# COMMAND ----------

print("=== Demonstrating Lazy Evaluation ===")

# These are all transformations (lazy) - no execution yet
step1 = employees_df.filter(col("salary") > 70000)
print("✅ Filter created (lazy)")

step2 = step1.select("name", "department", "salary")  
print("✅ Select created (lazy)")

step3 = step2.orderBy(col("salary").desc())
print("✅ OrderBy created (lazy)")

# Now we trigger execution with an action
print("\n⚡ Executing with .show() (action):")
step3.show()

print("✅ All transformations executed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. DataFrame vs SQL - Side by Side Comparison
# MAGIC 
# MAGIC Let's see the same operations in both DataFrame API and SQL:

# COMMAND ----------

# Create temporary view for SQL operations
employees_df.createOrReplaceTempView("employees_sql")

print("=== DataFrame API vs SQL Comparison ===")

# Example 1: Filter and Select
print("\n--- DataFrame API ---")
employees_df.filter(col("salary") > 80000).select("name", "salary").show(3)

print("--- SQL ---")
spark.sql("""
    SELECT name, salary 
    FROM employees_sql 
    WHERE salary > 80000
""").show(3)

# COMMAND ----------

# Example 2: Group By with Aggregations
print("\n--- DataFrame API ---")
employees_df.groupBy("department").agg(
    count("*").alias("emp_count"),
    avg("salary").alias("avg_salary")
).show()

print("--- SQL ---")
spark.sql("""
    SELECT department, 
           COUNT(*) as emp_count,
           AVG(salary) as avg_salary
    FROM employees_sql 
    GROUP BY department
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Practice Exercises

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1: Basic DataFrame Operations
# MAGIC Create a DataFrame with sales data and perform basic operations.

# COMMAND ----------

# Create sales DataFrame for exercises
sales_data = [
    (1, "Alice", "North", "Laptop", 1200.0, "2023-01-15"),
    (2, "Bob", "South", "Phone", 800.0, "2023-01-16"),
    (3, "Carol", "East", "Tablet", 600.0, "2023-01-17"),
    (4, "David", "West", "Laptop", 1300.0, "2023-01-18"),
    (5, "Eve", "North", "Phone", 750.0, "2023-01-19"),
    (6, "Frank", "South", "Tablet", 650.0, "2023-01-20")
]

sales_schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("salesperson", StringType(), True),
    StructField("region", StringType(), True),
    StructField("product", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("sale_date", StringType(), True)
])

sales_df = spark.createDataFrame(sales_data, sales_schema)

# Convert sale_date to proper date type
sales_df = sales_df.withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd"))

print("Sales DataFrame created:")
sales_df.show()

# COMMAND ----------

# TODO Exercise 1: Complete these tasks using DataFrame API

# Task 1: Select salesperson and amount for sales > $700
print("=== Task 1: Sales > $700 ===")
# YOUR CODE HERE
task1_result = sales_df.filter(col("amount") > 700).select("salesperson", "amount")
task1_result.show()

# Task 2: Group by region and calculate total sales
print("\n=== Task 2: Sales by region ===")
# YOUR CODE HERE  
task2_result = sales_df.groupBy("region").agg(sum("amount").alias("total_sales"))
task2_result.show()

# Task 3: Add a commission column (10% of amount) and category based on amount
print("\n=== Task 3: Add commission and category ===")
# YOUR CODE HERE
task3_result = sales_df.withColumn("commission", col("amount") * 0.1) \
                     .withColumn("category", 
                                when(col("amount") > 1000, "High")
                                .when(col("amount") > 600, "Medium")
                                .otherwise("Low"))
task3_result.select("salesperson", "amount", "commission", "category").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2: Complex Operations
# MAGIC Combine multiple operations to answer business questions.

# COMMAND ----------

# Create temporary view for SQL comparison
sales_df.createOrReplaceTempView("sales")

print("=== Exercise 2: Complex Operations ===")

# Task 1: Find top performing salesperson by total sales (DataFrame API)
print("\n--- Task 1: Top performer (DataFrame) ---")
top_performer = sales_df.groupBy("salesperson") \
                       .agg(sum("amount").alias("total_sales")) \
                       .orderBy(col("total_sales").desc()) \
                       .limit(1)
top_performer.show()

# Same query in SQL for comparison
print("--- Task 1: Top performer (SQL) ---")
spark.sql("""
    SELECT salesperson, SUM(amount) as total_sales
    FROM sales
    GROUP BY salesperson
    ORDER BY total_sales DESC
    LIMIT 1
""").show()

# COMMAND ----------

# Task 2: Regional performance with product breakdown
print("\n--- Task 2: Regional performance by product ---")

regional_product = sales_df.groupBy("region", "product") \
                          .agg(
                              count("*").alias("sales_count"),
                              sum("amount").alias("total_revenue"),
                              avg("amount").alias("avg_sale")
                          ) \
                          .orderBy("region", col("total_revenue").desc())

regional_product.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC Congratulations! You've learned PySpark DataFrame fundamentals:
# MAGIC 
# MAGIC ✅ **Creating DataFrames** - like creating tables  
# MAGIC ✅ **Selecting columns** - like SELECT clauses  
# MAGIC ✅ **Filtering data** - like WHERE conditions  
# MAGIC ✅ **Sorting** - like ORDER BY  
# MAGIC ✅ **Aggregations** - like GROUP BY and aggregate functions  
# MAGIC ✅ **Column operations** - adding and transforming columns  
# MAGIC ✅ **Lazy evaluation** - understanding transformations vs actions  
# MAGIC ✅ **DataFrame vs SQL** - using both approaches  
# MAGIC 
# MAGIC ### Key Takeaways:
# MAGIC - DataFrames are distributed tables you can manipulate
# MAGIC - Operations are lazy until you call an action
# MAGIC - You can use either DataFrame API or SQL - they're equivalent
# MAGIC - Schema definition is important for data quality
# MAGIC - Transformations create new DataFrames (immutable)
# MAGIC 
# MAGIC **Next**: Learn about temporary views and advanced SQL integration with PySpark!

# COMMAND ----------

# MAGIC %md
# MAGIC ## PySpark DataFrame Cheat Sheet
# MAGIC 
# MAGIC | Operation | DataFrame API | SQL Equivalent |
# MAGIC |-----------|---------------|----------------|
# MAGIC | **Create** | `spark.createDataFrame()` | `CREATE TABLE` |
# MAGIC | **Select** | `.select("col1", "col2")` | `SELECT col1, col2` |
# MAGIC | **Filter** | `.filter(col("x") > 5)` | `WHERE x > 5` |
# MAGIC | **Sort** | `.orderBy("column")` | `ORDER BY column` |
# MAGIC | **Group** | `.groupBy("col").count()` | `GROUP BY col` |
# MAGIC | **Aggregate** | `.agg(sum("col"))` | `SELECT SUM(col)` |
# MAGIC | **Add Column** | `.withColumn("new", expr)` | `SELECT *, expr AS new` |
# MAGIC | **Rename** | `.withColumnRenamed("old", "new")` | `SELECT col AS new` |
# MAGIC | **Distinct** | `.distinct()` | `SELECT DISTINCT` |
# MAGIC | **Count** | `.count()` | `SELECT COUNT(*)` |
# MAGIC | **Show** | `.show()` | `SELECT * LIMIT n` |