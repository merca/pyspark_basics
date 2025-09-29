# Databricks notebook source
# MAGIC %md
# MAGIC # 04. Advanced PySpark Operations
# MAGIC 
# MAGIC This notebook covers advanced PySpark operations that data engineers and analysts use in real-world scenarios. We'll explore complex joins, window functions, user-defined functions (UDFs), and advanced data transformations.
# MAGIC 
# MAGIC ## Topics Covered:
# MAGIC - Complex joins (multiple DataFrames, self-joins)
# MAGIC - Window functions and analytics
# MAGIC - User-defined functions (UDFs) and when to use them
# MAGIC - Advanced aggregations and pivoting
# MAGIC - Working with arrays and nested data
# MAGIC - Performance optimization techniques

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup - Create Complex Sample Data

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pandas as pd
from datetime import date, datetime, timedelta

# Create comprehensive sample datasets for advanced operations

# Employee data with more details
employees_data = [
    (1, "John Doe", "Engineering", "Senior Engineer", 95000, date(2020, 1, 15), "john.doe@company.com", 101, "NYC"),
    (2, "Jane Smith", "Marketing", "Marketing Manager", 75000, date(2019, 3, 10), "jane.smith@company.com", 102, "LA"),
    (3, "Mike Johnson", "Engineering", "Data Engineer", 85000, date(2021, 6, 1), "mike.johnson@company.com", 101, "NYC"),
    (4, "Sarah Wilson", "Sales", "Sales Rep", 65000, date(2022, 2, 20), "sarah.wilson@company.com", 103, "Chicago"),
    (5, "Tom Brown", "Engineering", "Senior Engineer", 98000, date(2018, 11, 5), "tom.brown@company.com", 101, "NYC"),
    (6, "Lisa Davis", "Marketing", "Analyst", 55000, date(2023, 1, 10), "lisa.davis@company.com", 102, "LA"),
    (7, "David Miller", "Sales", "Sales Manager", 80000, date(2020, 8, 15), "david.miller@company.com", 103, "Chicago"),
    (8, "Amy Taylor", "Engineering", "Data Scientist", 105000, date(2021, 4, 3), "amy.taylor@company.com", 101, "NYC")
]

employees_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("position", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("hire_date", DateType(), True),
    StructField("email", StringType(), True),
    StructField("office_id", IntegerType(), True),
    StructField("city", StringType(), True)
])

employees_df = spark.createDataFrame(employees_data, employees_schema)

# Office information
offices_data = [
    (101, "New York Office", "123 Broadway, NY", "East", 150),
    (102, "Los Angeles Office", "456 Sunset Blvd, LA", "West", 100),  
    (103, "Chicago Office", "789 Michigan Ave, Chicago", "Central", 75)
]

offices_schema = StructType([
    StructField("office_id", IntegerType(), True),
    StructField("office_name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("region", StringType(), True),
    StructField("capacity", IntegerType(), True)
])

offices_df = spark.createDataFrame(offices_data, offices_schema)

# Sales data with more complexity
sales_data = [
    (101, 1, "2023-01-15", 1200.50, "Product A", "Electronics", "Q1", 4.5),
    (102, 4, "2023-01-16", 850.00, "Product B", "Software", "Q1", 4.2),
    (103, 7, "2023-01-17", 2300.75, "Product C", "Hardware", "Q1", 4.8),
    (104, 4, "2023-02-15", 1800.00, "Product A", "Electronics", "Q1", 4.3),
    (105, 7, "2023-02-16", 950.50, "Product B", "Software", "Q1", 4.1),
    (106, 1, "2023-03-15", 1350.25, "Product C", "Hardware", "Q1", 4.6),
    (107, 4, "2023-04-15", 1150.00, "Product A", "Electronics", "Q2", 4.4),
    (108, 7, "2023-05-16", 2100.75, "Product D", "Software", "Q2", 4.7),
    (109, 1, "2023-06-15", 875.50, "Product C", "Hardware", "Q2", 4.2),
    (110, 4, "2023-07-15", 1300.00, "Product A", "Electronics", "Q3", 4.5),
    (111, 7, "2023-08-16", 1750.25, "Product B", "Software", "Q3", 4.3),
    (112, 1, "2023-09-15", 2200.50, "Product D", "Software", "Q3", 4.9)
]

sales_schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("employee_id", IntegerType(), True),
    StructField("sale_date", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quarter", StringType(), True),
    StructField("customer_rating", DoubleType(), True)
])

sales_df = spark.createDataFrame(sales_data, sales_schema)
sales_df = sales_df.withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd"))

# Product data
products_data = [
    ("Product A", "Electronics", 999.99, date(2022, 1, 1), True),
    ("Product B", "Software", 299.99, date(2022, 3, 15), True),
    ("Product C", "Hardware", 1499.99, date(2021, 6, 1), True),
    ("Product D", "Software", 499.99, date(2023, 1, 10), True)
]

products_schema = StructType([
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("list_price", DoubleType(), True),
    StructField("launch_date", DateType(), True),
    StructField("active", BooleanType(), True)
])

products_df = spark.createDataFrame(products_data, products_schema)

print("✅ Advanced sample datasets created!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Complex Joins - Beyond Basic Inner Joins
# MAGIC 
# MAGIC **Real-world scenario**: Often you need to join multiple tables and handle different join scenarios.

# COMMAND ----------

# Multiple table joins - like complex SQL JOINs
print("=== Multiple Table Joins ===")

# Join employees with offices and sales
comprehensive_employee_data = employees_df \
    .join(offices_df, "office_id", "left") \
    .join(sales_df.groupBy("employee_id").agg(
        count("*").alias("total_sales"),
        sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_sale_amount"),
        max("sale_date").alias("last_sale_date")
    ), "employee_id", "left")

comprehensive_employee_data.show()

# COMMAND ----------

# Self-joins - compare employees within same department
print("=== Self-Join: Compare Salaries Within Department ===")

# Create aliases for self-join
emp1 = employees_df.alias("emp1") 
emp2 = employees_df.alias("emp2")

salary_comparisons = emp1.join(emp2, 
    (col("emp1.department") == col("emp2.department")) & 
    (col("emp1.employee_id") != col("emp2.employee_id"))
).select(
    col("emp1.name").alias("employee1"),
    col("emp1.salary").alias("salary1"),
    col("emp2.name").alias("employee2"), 
    col("emp2.salary").alias("salary2"),
    col("emp1.department"),
    (col("emp1.salary") - col("emp2.salary")).alias("salary_diff")
).filter(col("salary_diff") > 0)  # Only show higher earners

salary_comparisons.show()

# COMMAND ----------

# Complex join conditions - business logic in joins
print("=== Complex Join Conditions ===")

# Join sales with products, but only for high-value sales
# Use aliases to avoid ambiguous column references
sales_alias = sales_df.alias("s")
products_alias = products_df.alias("p") 
employees_alias = employees_df.alias("e")

high_value_sales = sales_alias \
    .join(products_alias, col("s.product_name") == col("p.product_name")) \
    .join(employees_alias, col("s.employee_id") == col("e.employee_id")) \
    .filter(
        (col("s.amount") > col("p.list_price") * 0.8) &  # Sale amount > 80% of list price
        (col("s.customer_rating") >= 4.5)                # High customer rating
    ) \
    .select(
        col("e.name").alias("salesperson"),
        col("p.product_name"),
        col("p.category").alias("product_category"),  # Use product category to avoid ambiguity
        col("s.amount"),
        col("p.list_price"),
        col("s.customer_rating"),
        (col("s.amount") / col("p.list_price") * 100).alias("price_percentage")
    )

high_value_sales.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Window Functions - Advanced Analytics
# MAGIC 
# MAGIC **SQL equivalent**: Window functions in PySpark work like OVER clauses in SQL.

# COMMAND ----------

# Window functions for ranking and analytics
print("=== Window Functions: Rankings and Analytics ===")

# Define window specifications
dept_window = Window.partitionBy("department").orderBy(col("salary").desc())
overall_window = Window.orderBy(col("salary").desc())
running_window = Window.partitionBy("department").orderBy("hire_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

employee_analytics = employees_df.select(
    "name",
    "department", 
    "salary",
    "hire_date",
    
    # Rankings within department
    row_number().over(dept_window).alias("dept_rank"),
    rank().over(dept_window).alias("dept_rank_tied"),
    dense_rank().over(dept_window).alias("dept_dense_rank"),
    
    # Percentiles
    percent_rank().over(dept_window).alias("salary_percentile_in_dept"),
    
    # Comparisons
    lag("salary", 1).over(dept_window).alias("next_lower_salary"),
    lead("salary", 1).over(dept_window).alias("next_higher_salary"),
    
    # Department statistics
    avg("salary").over(Window.partitionBy("department")).alias("dept_avg_salary"),
    max("salary").over(Window.partitionBy("department")).alias("dept_max_salary"),
    
    # Running totals
    sum("salary").over(running_window).alias("running_salary_total"),
    count("*").over(running_window).alias("employees_hired_so_far")
)

employee_analytics.show()

# COMMAND ----------

# Advanced window functions - moving averages and trends
print("=== Moving Averages and Trends ===")

# Sales trend analysis with moving averages
sales_trends = sales_df.select(
    "sale_date",
    "amount",
    "employee_id",
    "product_name"
).withColumn(
    "month", date_format("sale_date", "yyyy-MM")
)

monthly_sales = sales_trends.groupBy("month").agg(
    sum("amount").alias("monthly_total"),
    count("*").alias("monthly_count"),
    avg("amount").alias("monthly_avg")
)

# Window for time-based calculations
time_window = Window.orderBy("month")

sales_with_trends = monthly_sales.select(
    "month",
    "monthly_total",
    "monthly_count",
    "monthly_avg",
    
    # Moving averages
    avg("monthly_total").over(time_window.rowsBetween(-2, 0)).alias("3_month_avg_total"),
    
    # Period-over-period change
    lag("monthly_total", 1).over(time_window).alias("prev_month_total"),
    ((col("monthly_total") - lag("monthly_total", 1).over(time_window)) / 
     lag("monthly_total", 1).over(time_window) * 100).alias("month_over_month_growth"),
    
    # Cumulative totals
    sum("monthly_total").over(Window.orderBy("month").rowsBetween(Window.unboundedPreceding, 0)).alias("ytd_total")
)

sales_with_trends.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. User-Defined Functions (UDFs)
# MAGIC 
# MAGIC **When to use UDFs**: For complex logic that can't be expressed with built-in functions.

# COMMAND ----------

# Simple UDF - calculate bonus based on complex rules
print("=== User-Defined Functions ===")

def calculate_complex_bonus(salary, department, performance_score):
    """Calculate bonus based on complex business rules"""
    base_rate = 0.10  # 10% base bonus
    
    # Department multipliers
    dept_multipliers = {
        "Engineering": 1.2,
        "Sales": 1.1, 
        "Marketing": 1.0
    }
    
    # Performance adjustments
    if performance_score >= 4.8:
        perf_multiplier = 1.5
    elif performance_score >= 4.5:
        perf_multiplier = 1.2
    elif performance_score >= 4.0:
        perf_multiplier = 1.0
    else:
        perf_multiplier = 0.8
    
    # Salary tier adjustments
    if salary > 95000:
        salary_multiplier = 1.3
    elif salary > 75000:
        salary_multiplier = 1.1
    else:
        salary_multiplier = 1.0
    
    final_rate = base_rate * dept_multipliers.get(department, 1.0) * perf_multiplier * salary_multiplier
    return float(salary * final_rate)

# Register UDF
bonus_udf = udf(calculate_complex_bonus, DoubleType())

# Apply UDF (first add performance scores to employees)
employees_with_performance = employees_df.withColumn(
    "performance_score", 
    when(col("department") == "Engineering", 4.6)
    .when(col("department") == "Sales", 4.3)
    .otherwise(4.1)
)

employees_with_bonus = employees_with_performance.withColumn(
    "calculated_bonus",
    bonus_udf(col("salary"), col("department"), col("performance_score"))
)

employees_with_bonus.select("name", "department", "salary", "performance_score", "calculated_bonus").show()

# COMMAND ----------

# UDF for text processing
print("=== Text Processing UDF ===")

def extract_email_domain(email):
    """Extract domain from email address"""
    if email and "@" in email:
        return email.split("@")[1]
    return "unknown"

def format_name_title(name, position):
    """Format name with title"""
    if not name:
        return "Unknown"
    
    title_mapping = {
        "Senior Engineer": "Sr. Eng.",
        "Data Engineer": "Data Eng.",
        "Marketing Manager": "Marketing Mgr.",
        "Sales Rep": "Sales Rep.",
        "Sales Manager": "Sales Mgr.",
        "Data Scientist": "Data Sci.",
        "Analyst": "Analyst"
    }
    
    title = title_mapping.get(position, position)
    return f"{name} ({title})"

# Register UDFs
domain_udf = udf(extract_email_domain, StringType())
title_udf = udf(format_name_title, StringType())

# Apply text processing UDFs
employees_formatted = employees_df.withColumn(
    "email_domain", domain_udf(col("email"))
).withColumn(
    "formatted_name", title_udf(col("name"), col("position"))
)

employees_formatted.select("name", "position", "formatted_name", "email", "email_domain").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Advanced Aggregations and Pivoting

# COMMAND ----------

# Pivot operations - like Excel pivot tables
print("=== Pivot Operations ===")

# Sales by quarter and category
sales_pivot = sales_df.groupBy("quarter") \
    .pivot("category") \
    .agg(
        sum("amount").alias("total_sales"),
        count("*").alias("transaction_count")
    )

sales_pivot.show()

# COMMAND ----------

# Advanced aggregations - multiple metrics
print("=== Advanced Aggregations ===")

# Complex department analysis
dept_analysis = employees_df.join(
    sales_df.groupBy("employee_id").agg(
        sum("amount").alias("total_sales"),
        count("*").alias("sales_count"),
        avg("customer_rating").alias("avg_rating")
    ), "employee_id", "left"
).join(offices_df, "office_id", "left")

comprehensive_dept_stats = dept_analysis.groupBy("department", "region").agg(
    # Basic counts and averages
    count("employee_id").alias("employee_count"),
    avg("salary").alias("avg_salary"),
    sum("total_sales").alias("dept_total_sales"),
    
    # Statistical measures
    stddev("salary").alias("salary_stddev"),
    min("hire_date").alias("earliest_hire"),
    max("hire_date").alias("latest_hire"),
    
    # Conditional aggregations
    sum(when(col("total_sales").isNotNull(), col("total_sales")).otherwise(0)).alias("sales_revenue"),
    count(when(col("total_sales") > 3000, 1)).alias("high_performers"),
    
    # Collect lists (be careful with large datasets)
    collect_list("name").alias("employee_names")
)

comprehensive_dept_stats.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Working with Arrays and Nested Data

# COMMAND ----------

# Working with arrays and complex data types
print("=== Arrays and Complex Data Types ===")

# Create sample data with arrays
order_data = [
    (1, "Alice", ["Product A", "Product B"], [1200.50, 850.00], "2023-01-15"),
    (2, "Bob", ["Product C"], [2300.75], "2023-01-16"),
    (3, "Carol", ["Product A", "Product B", "Product D"], [1200.50, 850.00, 499.99], "2023-01-17")
]

order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer", StringType(), True),
    StructField("products", ArrayType(StringType()), True),
    StructField("amounts", ArrayType(DoubleType()), True),
    StructField("order_date", StringType(), True)
])

orders_df = spark.createDataFrame(order_data, order_schema)

# Array operations
orders_analysis = orders_df.select(
    "order_id",
    "customer",
    "products",
    "amounts",
    
    # Array functions
    size("products").alias("product_count"),
    array_contains("products", "Product A").alias("contains_product_a"),
    
    # Aggregate array values
    expr("aggregate(amounts, 0D, (acc, x) -> acc + x)").alias("total_amount"),
    
    # Explode arrays (creates multiple rows)
    explode("products").alias("individual_product")
).drop("products")  # Remove original array column after explode

print("Orders with exploded products:")
orders_analysis.show()

# COMMAND ----------

# Working with nested structures
print("=== Nested Structures ===")

# Create data with nested structures
nested_data = [
    (1, "John", {"street": "123 Main St", "city": "NYC", "zip": "10001"}, 
     {"base": 95000, "bonus": 9500, "stock": 5000}),
    (2, "Jane", {"street": "456 Oak Ave", "city": "LA", "zip": "90210"}, 
     {"base": 75000, "bonus": 7500, "stock": 3000})
]

nested_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("zip", StringType(), True)
    ])),
    StructField("compensation", StructType([
        StructField("base", IntegerType(), True),
        StructField("bonus", IntegerType(), True),
        StructField("stock", IntegerType(), True)
    ]))
])

nested_df = spark.createDataFrame(nested_data, nested_schema)

# Access nested fields
nested_analysis = nested_df.select(
    "name",
    col("address.city").alias("city"),
    col("address.zip").alias("zip_code"),
    col("compensation.base").alias("base_salary"),
    col("compensation.bonus").alias("bonus_amount"),
    (col("compensation.base") + col("compensation.bonus") + col("compensation.stock")).alias("total_comp")
)

nested_analysis.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Performance Optimization Techniques

# COMMAND ----------

# Caching strategies (Classic vs Serverless compatibility)
print("=== Performance Optimization ===")

# Caching is NOT SUPPORTED on serverless compute
try:
    # Cache frequently accessed DataFrames (Classic compute)
    employees_df.cache()
    sales_df.cache()
    print("✅ DataFrames cached (Classic compute)")
    cache_supported = True
except Exception as e:
    print(f"❌ Caching not supported: {type(e).__name__}")
    print("✅ This is expected on serverless compute")
    print("   ➡️ Serverless automatically manages memory optimization")
    cache_supported = False

# Repartitioning strategies (check RDD access first)
try:
    print("\nOriginal partitions:")
    print(f"Employees: {employees_df.rdd.getNumPartitions()}")
    print(f"Sales: {sales_df.rdd.getNumPartitions()}")
    print("✅ Running on Classic compute - RDD operations available")
except Exception as e:
    print(f"\n❌ RDD access not available: {type(e).__name__}")
    print("✅ Running on Serverless compute - RDD operations not supported")
    print("   ➡️ Use DataFrame operations for optimal performance")

# Repartition by key for joins
employees_partitioned = employees_df.repartition("department")
sales_by_employee = sales_df.repartition("employee_id")

print(f"Employees repartitioned: {employees_partitioned.rdd.getNumPartitions()}")

# COMMAND ----------

# Broadcast joins for small tables
print("=== Broadcast Joins ===")

# Broadcast the smaller offices DataFrame
from pyspark.sql.functions import broadcast

# Efficient join with broadcast
efficient_join = employees_df.join(
    broadcast(offices_df), "office_id"
).join(
    broadcast(products_df), 
    employees_df.department == products_df.category,  # Example business rule join
    "left"
)

print("Broadcast join completed - check execution plan:")
efficient_join.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Advanced Data Quality and Validation

# COMMAND ----------

# Data quality checks with advanced operations
print("=== Data Quality Checks ===")

def data_quality_report(df, df_name):
    """Generate comprehensive data quality report"""
    
    # Basic stats
    total_rows = df.count()
    total_columns = len(df.columns)
    
    # Null checks for each column
    null_counts = []
    for col_name in df.columns:
        null_count = df.filter(col(col_name).isNull()).count()
        null_pct = (null_count / total_rows) * 100 if total_rows > 0 else 0
        null_counts.append((col_name, null_count, null_pct))
    
    # Duplicate checks
    duplicate_count = df.count() - df.dropDuplicates().count()
    
    print(f"\n=== Data Quality Report for {df_name} ===")
    print(f"Total Rows: {total_rows}")
    print(f"Total Columns: {total_columns}")
    print(f"Duplicate Rows: {duplicate_count}")
    
    print("\nNull Analysis:")
    for col_name, null_count, null_pct in null_counts:
        print(f"  {col_name}: {null_count} nulls ({null_pct:.1f}%)")
    
    return null_counts

# Run quality checks
employee_quality = data_quality_report(employees_df, "Employees")
sales_quality = data_quality_report(sales_df, "Sales")

# COMMAND ----------

# Advanced validation with business rules
print("=== Business Rule Validation ===")

# Define business rules as functions
def validate_salary_ranges(df):
    """Validate that salaries are within expected ranges by department"""
    rules = {
        "Engineering": (70000, 120000),
        "Marketing": (45000, 90000), 
        "Sales": (50000, 100000)
    }
    
    violations = []
    for dept, (min_sal, max_sal) in rules.items():
        violations_df = df.filter(
            (col("department") == dept) & 
            ((col("salary") < min_sal) | (col("salary") > max_sal))
        )
        if violations_df.count() > 0:
            violations.append((dept, violations_df.collect()))
    
    return violations

def validate_date_consistency(df):
    """Check for date consistency issues"""
    future_dates = df.filter(col("hire_date") > current_date()).count()
    return future_dates

# Run validations
salary_violations = validate_salary_ranges(employees_df)
date_issues = validate_date_consistency(employees_df)

print(f"Salary range violations: {len(salary_violations)}")
print(f"Future hire dates: {date_issues}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Real-World Complex Analysis Example

# COMMAND ----------

# Comprehensive business analysis combining multiple advanced techniques
print("=== 🚀 Complex Business Intelligence Analysis ===")

# Multi-step analysis combining everything we've learned
complex_analysis = employees_df \
    .join(offices_df, "office_id") \
    .join(
        sales_df.groupBy("employee_id").agg(
            sum("amount").alias("total_revenue"),
            count("*").alias("sales_count"),
            avg("customer_rating").alias("avg_rating"),
            collect_list("product_name").alias("products_sold")
        ), "employee_id", "left"
    ) \
    .withColumn("total_revenue", coalesce("total_revenue", lit(0))) \
    .withColumn("sales_count", coalesce("sales_count", lit(0))) \
    .withColumn(
        "performance_tier",
        when(col("total_revenue") > 5000, "Top Performer")
        .when(col("total_revenue") > 2000, "High Performer")  
        .when(col("total_revenue") > 0, "Performer")
        .otherwise("No Sales")
    )

# Add window functions for rankings
window_spec = Window.partitionBy("region").orderBy(col("total_revenue").desc())

final_analysis = complex_analysis.withColumn(
    "regional_rank", 
    row_number().over(window_spec)
).withColumn(
    "revenue_percentile",
    percent_rank().over(Window.orderBy("total_revenue"))
).withColumn(
    "roi_estimate",
    when(col("total_revenue") > 0, col("total_revenue") / col("salary") * 100).otherwise(0)
)

# Apply UDF for custom scoring
def calculate_employee_score(salary, revenue, rating, tenure_years):
    """Calculate comprehensive employee score"""
    revenue_score = min(revenue / 1000, 10)  # Max 10 points
    salary_efficiency = min(revenue / salary * 10 if salary > 0 else 0, 5)  # Max 5 points  
    rating_score = (rating or 0) # Direct rating
    tenure_bonus = min(tenure_years * 0.5, 2)  # Max 2 points for tenure
    
    return float(revenue_score + salary_efficiency + rating_score + tenure_bonus)

score_udf = udf(calculate_employee_score, DoubleType())

final_analysis = final_analysis.withColumn(
    "tenure_years",
    datediff(current_date(), "hire_date") / 365.25
).withColumn(
    "employee_score",
    score_udf("salary", "total_revenue", "avg_rating", "tenure_years")
)

# Show final comprehensive analysis
final_analysis.select(
    "name", "department", "region", "performance_tier",
    "total_revenue", "regional_rank", "employee_score"
).orderBy(col("employee_score").desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Practice Exercises

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1: Advanced Window Functions
# MAGIC Create a sales leaderboard with complex analytics

# COMMAND ----------

# TODO: Exercise 1 - Create sales leaderboard
print("=== Exercise 1: Sales Leaderboard ===")

# Your task: Create a comprehensive sales leaderboard that shows:
# 1. Quarterly rankings by revenue
# 2. Month-over-month growth
# 3. Performance vs department average
# 4. Streak analysis (consecutive months with sales)

# Solution:
quarterly_window = Window.partitionBy("quarter").orderBy(col("total_amount").desc())
monthly_window = Window.partitionBy("employee_id").orderBy("month")

sales_leaderboard = sales_df.withColumn("month", date_format("sale_date", "yyyy-MM")) \
    .groupBy("employee_id", "quarter", "month").agg(
        sum("amount").alias("total_amount"),
        count("*").alias("sales_count"),
        avg("customer_rating").alias("avg_rating")
    ).join(employees_df, "employee_id") \
    .withColumn("quarterly_rank", rank().over(quarterly_window)) \
    .withColumn("prev_month_sales", lag("total_amount").over(monthly_window)) \
    .withColumn("mom_growth", 
        when(col("prev_month_sales").isNotNull(),
             (col("total_amount") - col("prev_month_sales")) / col("prev_month_sales") * 100)
        .otherwise(None)
    ) \
    .withColumn("dept_avg_sales", 
        avg("total_amount").over(Window.partitionBy("department", "quarter"))
    ) \
    .withColumn("vs_dept_avg", col("total_amount") - col("dept_avg_sales"))

sales_leaderboard.select(
    "name", "department", "quarter", "quarterly_rank", 
    "total_amount", "mom_growth", "vs_dept_avg"
).filter(col("quarterly_rank") <= 3).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2: Complex Data Transformation
# MAGIC Transform and enrich the data for reporting

# COMMAND ----------

# TODO: Exercise 2 - Complex transformation pipeline
print("=== Exercise 2: Data Transformation Pipeline ===")

# Create a transformation pipeline that:
# 1. Combines all datasets
# 2. Handles missing values intelligently  
# 3. Creates derived metrics
# 4. Applies business rules

# Solution:
transformation_pipeline = employees_df \
    .join(offices_df, "office_id", "left") \
    .join(
        sales_df.groupBy("employee_id").agg(
            sum("amount").alias("lifetime_sales"),
            count("*").alias("total_transactions"),
            avg("customer_rating").alias("customer_satisfaction"),
            min("sale_date").alias("first_sale_date"),
            max("sale_date").alias("last_sale_date")
        ), "employee_id", "left"
    ) \
    .fillna({
        "lifetime_sales": 0,
        "total_transactions": 0,
        "customer_satisfaction": 0
    }) \
    .withColumn("tenure_months", 
        months_between(current_date(), "hire_date")
    ) \
    .withColumn("sales_per_month",
        when(col("tenure_months") > 0, col("lifetime_sales") / col("tenure_months"))
        .otherwise(0)
    ) \
    .withColumn("employee_tier",
        when((col("lifetime_sales") > 5000) & (col("customer_satisfaction") > 4.5), "Elite")
        .when(col("lifetime_sales") > 2000, "Advanced")
        .when(col("lifetime_sales") > 0, "Active") 
        .otherwise("New")
    ) \
    .withColumn("compensation_ratio",
        col("lifetime_sales") / col("salary")
    )

# Show transformation results
transformation_pipeline.select(
    "name", "department", "employee_tier", "lifetime_sales", 
    "sales_per_month", "compensation_ratio"
).orderBy(col("compensation_ratio").desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC Congratulations! You've mastered advanced PySpark operations:
# MAGIC 
# MAGIC ✅ **Complex Joins** - Multiple tables, self-joins, business logic joins  
# MAGIC ✅ **Window Functions** - Rankings, running totals, trend analysis  
# MAGIC ✅ **User-Defined Functions** - Custom business logic and text processing  
# MAGIC ✅ **Advanced Aggregations** - Pivot tables, conditional aggregations  
# MAGIC ✅ **Arrays & Nested Data** - Complex data structures and operations  
# MAGIC ✅ **Performance Optimization** - Caching, partitioning, broadcast joins  
# MAGIC ✅ **Data Quality** - Validation and business rule checking  
# MAGIC ✅ **Real-world Analytics** - Complex multi-step analysis pipelines  
# MAGIC 
# MAGIC ### Key Takeaways:
# MAGIC - Window functions are powerful for analytics and ranking
# MAGIC - UDFs enable custom business logic but use sparingly for performance
# MAGIC - Caching and partitioning strategies significantly impact performance
# MAGIC - Complex joins require careful consideration of join types and conditions
# MAGIC - Data quality checks should be built into your pipelines
# MAGIC - Combine multiple techniques for comprehensive business intelligence
# MAGIC 
# MAGIC **Next**: Practice all concepts with real-world exercises and scenarios!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced PySpark Quick Reference
# MAGIC 
# MAGIC | Operation | Code Example | Use Case |
# MAGIC |-----------|--------------|----------|
# MAGIC | **Window Functions** | `row_number().over(Window.partitionBy("dept"))` | Rankings, analytics |
# MAGIC | **Self-Join** | `df1.alias("a").join(df2.alias("b"), condition)` | Compare within dataset |
# MAGIC | **UDF** | `udf(function, returnType)` | Custom business logic |
# MAGIC | **Pivot** | `.pivot("column").agg(sum("value"))` | Cross-tabulation |
# MAGIC | **Broadcast** | `broadcast(small_df)` | Optimize small table joins |
# MAGIC | **Array Functions** | `explode(col("array_col"))` | Work with arrays |
# MAGIC | **Nested Access** | `col("struct.field")` | Access nested fields |
# MAGIC | **Cache** | `.cache()` | Performance (Classic only) |
# MAGIC | **Repartition** | `.repartition("key")` | Optimize for joins |
# MAGIC | **Complex Agg** | `.agg(sum(when(condition, value)))` | Conditional aggregation |