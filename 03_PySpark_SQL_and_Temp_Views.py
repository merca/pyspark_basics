# Databricks notebook source
# MAGIC %md
# MAGIC # 03. PySpark SQL and Temporary Views
# MAGIC 
# MAGIC This notebook shows how to seamlessly work with both SQL and DataFrame API in PySpark. Learn to create temporary views, use `spark.sql()`, and bridge between both approaches.
# MAGIC 
# MAGIC ## Topics Covered:
# MAGIC - Creating temporary views from DataFrames
# MAGIC - Using `spark.sql()` for SQL queries
# MAGIC - Mixing SQL and DataFrame operations
# MAGIC - Global vs local temporary views
# MAGIC - Complex SQL operations (CTEs, window functions, subqueries)
# MAGIC - Performance considerations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup - Load Sample Data

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import date, datetime

# Create sample datasets that we'll use throughout this notebook

# Employee data
employees_data = [
    (1, "John Doe", "Engineering", "Senior Engineer", 95000, date(2020, 1, 15), "john.doe@company.com"),
    (2, "Jane Smith", "Marketing", "Marketing Manager", 75000, date(2019, 3, 10), "jane.smith@company.com"),
    (3, "Mike Johnson", "Engineering", "Data Engineer", 85000, date(2021, 6, 1), "mike.johnson@company.com"),
    (4, "Sarah Wilson", "Sales", "Sales Rep", 65000, date(2022, 2, 20), "sarah.wilson@company.com"),
    (5, "Tom Brown", "Engineering", "Senior Engineer", 98000, date(2018, 11, 5), "tom.brown@company.com"),
    (6, "Lisa Davis", "Marketing", "Analyst", 55000, date(2023, 1, 10), "lisa.davis@company.com"),
    (7, "David Miller", "Sales", "Sales Manager", 80000, date(2020, 8, 15), "david.miller@company.com"),
    (8, "Amy Taylor", "Engineering", "Data Scientist", 105000, date(2021, 4, 3), "amy.taylor@company.com")
]

employees_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("position", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("hire_date", DateType(), True),
    StructField("email", StringType(), True)
])

employees_df = spark.createDataFrame(employees_data, employees_schema)

# Sales data
sales_data = [
    (101, 1, "2023-01-15", 1200.50, "Product A", "Q1"),
    (102, 4, "2023-01-16", 850.00, "Product B", "Q1"),
    (103, 7, "2023-01-17", 2300.75, "Product C", "Q1"),
    (104, 4, "2023-02-15", 1800.00, "Product A", "Q1"),
    (105, 7, "2023-02-16", 950.50, "Product B", "Q1"),
    (106, 1, "2023-03-15", 1350.25, "Product C", "Q1"),
    (107, 4, "2023-04-15", 1150.00, "Product A", "Q2"),
    (108, 7, "2023-05-16", 2100.75, "Product B", "Q2"),
    (109, 1, "2023-06-15", 875.50, "Product C", "Q2")
]

sales_schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("employee_id", IntegerType(), True),
    StructField("sale_date", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("product", StringType(), True),
    StructField("quarter", StringType(), True)
])

sales_df = spark.createDataFrame(sales_data, sales_schema)
sales_df = sales_df.withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd"))

print("✅ Sample datasets created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Creating Temporary Views
# MAGIC 
# MAGIC **Temporary views** allow you to query DataFrames using SQL syntax. Think of them as creating a "virtual table" that exists only in your Spark session.

# COMMAND ----------

# Create temporary views - like creating views in a database
employees_df.createOrReplaceTempView("employees")
sales_df.createOrReplaceTempView("sales")

print("✅ Temporary views created:")
print("- employees (from employees_df)")  
print("- sales (from sales_df)")

# List all temporary views in current session
print("\nAll temporary views in session:")
spark.catalog.listTables()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Basic SQL Queries with spark.sql()
# MAGIC 
# MAGIC Now you can use familiar SQL syntax on your DataFrames!

# COMMAND ----------

# Simple SELECT query - just like in any SQL database
print("=== Simple SELECT query ===")
result = spark.sql("""
    SELECT name, department, salary 
    FROM employees 
    ORDER BY salary DESC
""")

result.show()

# COMMAND ----------

# Aggregation query - GROUP BY with aggregate functions
print("=== Department statistics ===")
dept_stats = spark.sql("""
    SELECT 
        department,
        COUNT(*) as employee_count,
        AVG(salary) as avg_salary,
        MIN(salary) as min_salary,
        MAX(salary) as max_salary
    FROM employees
    GROUP BY department
    ORDER BY avg_salary DESC
""")

dept_stats.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Joining Data with SQL
# MAGIC 
# MAGIC **SQL Joins** work exactly as you'd expect - join DataFrames using familiar SQL syntax.

# COMMAND ----------

# Inner join - get employees with their sales
print("=== Employee sales (INNER JOIN) ===")
employee_sales = spark.sql("""
    SELECT 
        e.name,
        e.department,
        s.sale_date,
        s.product,
        s.amount
    FROM employees e
    INNER JOIN sales s ON e.employee_id = s.employee_id
    ORDER BY e.name, s.sale_date
""")

employee_sales.show()

# COMMAND ----------

# Left join - get all employees, with sales if they exist
print("=== All employees with sales (LEFT JOIN) ===")
all_employees_sales = spark.sql("""
    SELECT 
        e.name,
        e.department,
        COUNT(s.sale_id) as total_sales,
        COALESCE(SUM(s.amount), 0) as total_revenue
    FROM employees e
    LEFT JOIN sales s ON e.employee_id = s.employee_id
    GROUP BY e.employee_id, e.name, e.department
    ORDER BY total_revenue DESC
""")

all_employees_sales.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Complex SQL: CTEs (Common Table Expressions)
# MAGIC 
# MAGIC **CTEs** let you write complex queries with temporary named result sets - very powerful for complex analytics.

# COMMAND ----------

# CTE example - multi-step analysis  
print("=== Complex analysis with CTE ===")
# Note: Using try_divide() to handle division by zero safely
complex_analysis = spark.sql("""
    WITH employee_performance AS (
        SELECT 
            e.employee_id,
            e.name,
            e.department,
            e.salary,
            COUNT(s.sale_id) as sale_count,
            COALESCE(SUM(s.amount), 0) as total_sales,
            CASE 
                WHEN COALESCE(SUM(s.amount), 0) > 3000 THEN 'High Performer'
                WHEN COALESCE(SUM(s.amount), 0) > 1500 THEN 'Medium Performer'
                ELSE 'Low Performer'
            END as performance_tier
        FROM employees e
        LEFT JOIN sales s ON e.employee_id = s.employee_id
        GROUP BY e.employee_id, e.name, e.department, e.salary
    ),
    
    department_summary AS (
        SELECT 
            department,
            COUNT(*) as total_employees,
            AVG(salary) as avg_salary,
            SUM(total_sales) as dept_total_sales
        FROM employee_performance
        GROUP BY department
    )
    
    SELECT 
        ep.name,
        ep.department,
        ep.salary,
        ep.total_sales,
        ep.performance_tier,
        ds.dept_total_sales,
        ROUND(try_divide(ep.total_sales * 100, ds.dept_total_sales), 2) as pct_of_dept_sales
    FROM employee_performance ep
    JOIN department_summary ds ON ep.department = ds.department
    ORDER BY ep.department, ep.total_sales DESC
""")

complex_analysis.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Division by Zero in SQL
# MAGIC 
# MAGIC **Important**: When dividing values that might be zero, use `try_divide()` or CASE statements to avoid errors.

# COMMAND ----------

# Alternative approaches to handle division by zero
print("=== Division by Zero Handling Examples ===")

# Method 1: try_divide() function (recommended)
print("Method 1: Using try_divide()")
method1 = spark.sql("""
    SELECT 
        department,
        SUM(total_sales) as dept_sales,
        COUNT(*) as emp_count,
        -- try_divide returns NULL if divisor is 0
        try_divide(SUM(total_sales), COUNT(*)) as avg_sales_per_employee
    FROM (
        SELECT 
            e.department,
            COALESCE(SUM(s.amount), 0) as total_sales
        FROM employees e
        LEFT JOIN sales s ON e.employee_id = s.employee_id
        GROUP BY e.employee_id, e.department
    )
    GROUP BY department
""")
method1.show()

# Method 2: CASE WHEN approach
print("Method 2: Using CASE WHEN")
method2 = spark.sql("""
    SELECT 
        department,
        SUM(total_sales) as dept_sales,
        COUNT(*) as emp_count,
        CASE 
            WHEN COUNT(*) = 0 THEN NULL
            ELSE SUM(total_sales) / COUNT(*)
        END as avg_sales_per_employee
    FROM (
        SELECT 
            e.department,
            COALESCE(SUM(s.amount), 0) as total_sales
        FROM employees e
        LEFT JOIN sales s ON e.employee_id = s.employee_id
        GROUP BY e.employee_id, e.department
    )
    GROUP BY department
""")
method2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Window Functions in SQL
# MAGIC 
# MAGIC **Window functions** are powerful for analytics - calculate running totals, ranks, percentiles, etc.

# COMMAND ----------

# Window functions - ranking and running totals
print("=== Window functions - Rankings and Running Totals ===")
window_analysis = spark.sql("""
    SELECT 
        name,
        department,
        salary,
        
        -- Ranking within department
        ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank,
        RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank_tied,
        
        -- Percentiles
        PERCENT_RANK() OVER (PARTITION BY department ORDER BY salary) as salary_percentile,
        
        -- Running totals
        SUM(salary) OVER (PARTITION BY department ORDER BY salary) as running_total,
        
        -- Compare to department average
        AVG(salary) OVER (PARTITION BY department) as dept_avg_salary,
        salary - AVG(salary) OVER (PARTITION BY department) as diff_from_avg
        
    FROM employees
    ORDER BY department, salary DESC
""")

window_analysis.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Mixing SQL and DataFrame Operations
# MAGIC 
# MAGIC **Powerful Feature**: You can seamlessly mix SQL queries with DataFrame operations!

# COMMAND ----------

# Start with SQL, continue with DataFrame operations
print("=== Mixing SQL and DataFrame API ===")

# Step 1: Use SQL to get initial results
sql_result = spark.sql("""
    SELECT 
        department,
        COUNT(*) as employee_count,
        AVG(salary) as avg_salary
    FROM employees
    GROUP BY department
""")

# Step 2: Continue with DataFrame operations
final_result = sql_result.filter(col("employee_count") > 2) \
                        .withColumn("salary_category", 
                                  when(col("avg_salary") > 80000, "High Pay")
                                  .otherwise("Standard Pay")) \
                        .orderBy(col("avg_salary").desc())

print("Final result combining SQL + DataFrame operations:")
final_result.show()

# COMMAND ----------

# Reverse: Start with DataFrame, use SQL on result
print("=== DataFrame → SQL ===")

# Step 1: DataFrame operations
df_result = employees_df.filter(col("salary") > 70000) \
                       .select("name", "department", "salary", "hire_date")

# Step 2: Create temp view from DataFrame result
df_result.createOrReplaceTempView("high_earners")

# Step 3: Use SQL on the temp view
sql_on_df = spark.sql("""
    SELECT 
        department,
        COUNT(*) as high_earner_count,
        MIN(hire_date) as earliest_hire,
        MAX(hire_date) as latest_hire
    FROM high_earners
    GROUP BY department
""")

sql_on_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Advanced SQL Features

# COMMAND ----------

# Subqueries and correlated queries
print("=== Subqueries ===")
subquery_example = spark.sql("""
    SELECT 
        name,
        department,
        salary,
        (SELECT AVG(salary) FROM employees WHERE department = e.department) as dept_avg
    FROM employees e
    WHERE salary > (
        SELECT AVG(salary) 
        FROM employees 
        WHERE department = e.department
    )
    ORDER BY department, salary DESC
""")

print("Employees earning above their department average:")
subquery_example.show()

# COMMAND ----------

# CASE WHEN statements and conditional logic
print("=== CASE WHEN and Conditional Logic ===")
conditional_analysis = spark.sql("""
    SELECT 
        name,
        department,
        salary,
        hire_date,
        
        CASE 
            WHEN salary > 90000 THEN 'Senior Level'
            WHEN salary > 70000 THEN 'Mid Level'
            ELSE 'Junior Level'
        END as level,
        
        CASE 
            WHEN YEAR(hire_date) >= 2022 THEN 'New Hire'
            WHEN YEAR(hire_date) >= 2020 THEN 'Recent Hire'  
            ELSE 'Veteran'
        END as tenure_category,
        
        DATEDIFF(CURRENT_DATE(), hire_date) as days_employed,
        
        CASE 
            WHEN department = 'Engineering' AND salary > 95000 THEN salary * 0.15
            WHEN department = 'Sales' AND salary > 75000 THEN salary * 0.12  
            ELSE salary * 0.10
        END as bonus_estimate
        
    FROM employees
    ORDER BY salary DESC
""")

conditional_analysis.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Working with Dates and Strings in SQL

# COMMAND ----------

# Date and string functions
print("=== Date and String Functions ===")
date_string_functions = spark.sql("""
    SELECT 
        name,
        email,
        hire_date,
        
        -- String functions
        UPPER(name) as name_upper,
        LOWER(email) as email_lower,
        SUBSTRING(email, 1, POSITION('@' IN email) - 1) as username,
        LENGTH(name) as name_length,
        
        -- Date functions
        YEAR(hire_date) as hire_year,
        MONTH(hire_date) as hire_month,
        DAYOFWEEK(hire_date) as day_of_week,
        DATE_FORMAT(hire_date, 'MMMM yyyy') as hire_month_year,
        DATEDIFF(CURRENT_DATE(), hire_date) as days_since_hire,
        DATE_ADD(hire_date, 90) as end_of_probation,
        
        -- Conditional date logic
        CASE 
            WHEN DATEDIFF(CURRENT_DATE(), hire_date) > 1000 THEN 'Senior Employee'
            WHEN DATEDIFF(CURRENT_DATE(), hire_date) > 365 THEN 'Experienced'
            ELSE 'New Employee'
        END as experience_level
        
    FROM employees
    ORDER BY hire_date
""")

date_string_functions.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Global vs Local Temporary Views

# COMMAND ----------

# Local temporary view (default) - only exists in current SparkSession
employees_df.createOrReplaceTempView("local_employees")

# Global temporary view - can be accessed across SparkSessions (in same application)
employees_df.createGlobalTempView("global_employees")

print("✅ Views created:")
print("- local_employees (local temp view)")
print("- global_employees (global temp view)")

# Access global temp view (note the global_temp database prefix)
print("\n=== Accessing global temp view ===")
global_query = spark.sql("""
    SELECT department, COUNT(*) as count
    FROM global_temp.global_employees  
    GROUP BY department
""")
global_query.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Performance Tips for SQL Queries

# COMMAND ----------

# Caching frequently used views
employees_df.cache()
employees_df.createOrReplaceTempView("cached_employees")

print("✅ DataFrame cached and view created")

# Using EXPLAIN to see query plans
print("\n=== Query Execution Plan ===")
spark.sql("""
    SELECT department, AVG(salary) 
    FROM cached_employees 
    GROUP BY department
""").explain(True)  # Set to True for detailed plan

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Practical Exercise: Sales Analytics Dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC Let's build a comprehensive sales analytics report using SQL:

# COMMAND ----------

# Create a comprehensive sales analytics report
print("=== 📊 Sales Analytics Dashboard ===")

sales_dashboard = spark.sql("""
    WITH monthly_sales AS (
        SELECT 
            DATE_FORMAT(sale_date, 'yyyy-MM') as month,
            product,
            COUNT(*) as transaction_count,
            SUM(amount) as monthly_revenue
        FROM sales
        GROUP BY DATE_FORMAT(sale_date, 'yyyy-MM'), product
    ),
    
    employee_metrics AS (
        SELECT 
            e.name,
            e.department,
            COUNT(s.sale_id) as total_transactions,
            SUM(s.amount) as total_revenue,
            AVG(s.amount) as avg_transaction,
            MIN(s.sale_date) as first_sale,
            MAX(s.sale_date) as last_sale
        FROM employees e
        LEFT JOIN sales s ON e.employee_id = s.employee_id
        GROUP BY e.employee_id, e.name, e.department
    ),
    
    product_performance AS (
        SELECT 
            product,
            COUNT(*) as total_sales,
            SUM(amount) as product_revenue,
            AVG(amount) as avg_price,
            COUNT(DISTINCT employee_id) as sellers_count
        FROM sales
        GROUP BY product
    )
    
    SELECT 
        'EMPLOYEE_PERFORMANCE' as report_type,
        em.name as metric_name,
        CAST(em.total_revenue as STRING) as metric_value,
        em.department as category
    FROM employee_metrics em
    WHERE em.total_revenue > 0
    
    UNION ALL
    
    SELECT 
        'PRODUCT_PERFORMANCE' as report_type,
        pp.product as metric_name,
        CAST(pp.product_revenue as STRING) as metric_value,
        'Products' as category
    FROM product_performance pp
    
    ORDER BY report_type, CAST(metric_value as DOUBLE) DESC
""")

sales_dashboard.show(20)

# COMMAND ----------

# Create specific reports using the temp views
print("\n=== 📈 Top Performers Report ===")
top_performers = spark.sql("""
    SELECT 
        e.name as employee,
        e.department,
        COUNT(s.sale_id) as sales_count,
        ROUND(SUM(s.amount), 2) as total_revenue,
        ROUND(AVG(s.amount), 2) as avg_sale,
        RANK() OVER (ORDER BY SUM(s.amount) DESC) as revenue_rank
    FROM employees e
    INNER JOIN sales s ON e.employee_id = s.employee_id
    GROUP BY e.employee_id, e.name, e.department
    HAVING COUNT(s.sale_id) > 1
    ORDER BY total_revenue DESC
""")

top_performers.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Practice Exercises

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1: Customer Analysis
# MAGIC Create customer data and analyze it with SQL

# COMMAND ----------

# Create customer data for the exercise
customer_data = [
    (1, "Alice Johnson", "alice@email.com", "Premium", "2022-01-15", "New York"),
    (2, "Bob Smith", "bob@email.com", "Standard", "2021-03-20", "Los Angeles"), 
    (3, "Carol Davis", "carol@email.com", "Premium", "2023-01-10", "Chicago"),
    (4, "David Wilson", "david@email.com", "Basic", "2022-06-15", "Houston"),
    (5, "Eve Brown", "eve@email.com", "Premium", "2021-11-30", "Phoenix")
]

customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("tier", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("city", StringType(), True)
])

customers_df = spark.createDataFrame(customer_data, customer_schema)
customers_df = customers_df.withColumn("signup_date", to_date(col("signup_date"), "yyyy-MM-dd"))
customers_df.createOrReplaceTempView("customers")

print("Customer data created and temp view 'customers' available")
customers_df.show()

# COMMAND ----------

# TODO: Exercise 1 Tasks - Write SQL queries for the following:

# Task 1: Customer tier distribution
print("=== Task 1: Customer tier distribution ===")
task1_result = spark.sql("""
    SELECT 
        tier,
        COUNT(*) as customer_count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM customers), 2) as percentage
    FROM customers
    GROUP BY tier
    ORDER BY customer_count DESC
""")
task1_result.show()

# Task 2: Customers by signup year
print("\n=== Task 2: Customers by signup year ===")
task2_result = spark.sql("""
    SELECT 
        YEAR(signup_date) as signup_year,
        COUNT(*) as new_customers,
        tier,
        COUNT(*) as customers_by_tier
    FROM customers
    GROUP BY YEAR(signup_date), tier
    ORDER BY signup_year, tier
""")
task2_result.show()

# Task 3: Premium customers with account age
print("\n=== Task 3: Premium customers with account age ===")
task3_result = spark.sql("""
    SELECT 
        name,
        city,
        signup_date,
        DATEDIFF(CURRENT_DATE(), signup_date) as account_age_days,
        CASE 
            WHEN DATEDIFF(CURRENT_DATE(), signup_date) > 730 THEN 'Loyal Customer'
            WHEN DATEDIFF(CURRENT_DATE(), signup_date) > 365 THEN 'Established Customer'
            ELSE 'New Customer'
        END as customer_category
    FROM customers
    WHERE tier = 'Premium'
    ORDER BY signup_date
""")
task3_result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2: Advanced Analytics
# MAGIC Combine multiple datasets for comprehensive analysis

# COMMAND ----------

# TODO: Exercise 2 - Create a comprehensive business intelligence query

print("=== Exercise 2: Comprehensive Business Intelligence ===")

# Your task: Write a SQL query that combines employees, sales, and customers data
# to create a business intelligence report showing:
# 1. Employee performance by department
# 2. Product performance by quarter  
# 3. Revenue trends over time

bi_report = spark.sql("""
    WITH employee_performance AS (
        SELECT 
            e.department,
            e.name,
            COUNT(s.sale_id) as total_sales,
            SUM(s.amount) as revenue,
            AVG(s.amount) as avg_sale_amount
        FROM employees e
        LEFT JOIN sales s ON e.employee_id = s.employee_id
        GROUP BY e.department, e.employee_id, e.name
    ),
    
    quarterly_performance AS (
        SELECT 
            quarter,
            product,
            COUNT(*) as q_sales_count,
            SUM(amount) as q_revenue,
            AVG(amount) as q_avg_amount
        FROM sales
        GROUP BY quarter, product
    )
    
    SELECT 
        'DEPARTMENT_PERFORMANCE' as metric_type,
        ep.department as category,
        ROUND(SUM(ep.revenue), 2) as total_value,
        COUNT(DISTINCT ep.name) as employee_count,
        ROUND(AVG(ep.revenue), 2) as avg_per_employee
    FROM employee_performance ep
    WHERE ep.revenue > 0
    GROUP BY ep.department
    
    UNION ALL
    
    SELECT 
        'QUARTERLY_PRODUCT' as metric_type,
        CONCAT(qp.quarter, '_', qp.product) as category,
        ROUND(qp.q_revenue, 2) as total_value,
        qp.q_sales_count as employee_count,
        ROUND(qp.q_avg_amount, 2) as avg_per_employee
    FROM quarterly_performance qp
    
    ORDER BY metric_type, total_value DESC
""")

bi_report.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC Congratulations! You've mastered PySpark SQL and temporary views:
# MAGIC 
# MAGIC ✅ **Temporary Views** - Bridge between DataFrames and SQL  
# MAGIC ✅ **spark.sql()** - Execute SQL queries on DataFrames  
# MAGIC ✅ **Complex SQL** - CTEs, window functions, subqueries  
# MAGIC ✅ **Joins** - Inner, left, right joins with SQL syntax  
# MAGIC ✅ **Mixed Operations** - Combining SQL and DataFrame API  
# MAGIC ✅ **Advanced Features** - Date/string functions, conditional logic  
# MAGIC ✅ **Performance** - Caching, query optimization  
# MAGIC ✅ **Real-world Analytics** - Business intelligence queries  
# MAGIC 
# MAGIC ### Key Takeaways:
# MAGIC - Temporary views let you use SQL on any DataFrame
# MAGIC - You can seamlessly mix SQL and DataFrame operations
# MAGIC - SQL in PySpark supports most standard SQL features
# MAGIC - Global temp views persist across SparkSessions
# MAGIC - Use EXPLAIN to understand query performance
# MAGIC - Cache frequently used views for better performance
# MAGIC 
# MAGIC **Next**: Learn advanced PySpark operations including complex joins, window functions, and UDFs!

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL in PySpark Quick Reference
# MAGIC 
# MAGIC | Operation | Code Example | Description |
# MAGIC |-----------|--------------|-------------|
# MAGIC | **Create View** | `df.createOrReplaceTempView("table")` | Create temporary view |
# MAGIC | **Global View** | `df.createGlobalTempView("table")` | Create global temp view |
# MAGIC | **Run SQL** | `spark.sql("SELECT * FROM table")` | Execute SQL query |
# MAGIC | **List Views** | `spark.catalog.listTables()` | Show all temp views |
# MAGIC | **Drop View** | `spark.catalog.dropTempView("table")` | Remove temp view |
# MAGIC | **Explain Query** | `spark.sql("query").explain()` | Show execution plan |
# MAGIC | **Cache View** | `df.cache()` | Cache DataFrame/view |
# MAGIC | **Window Function** | `ROW_NUMBER() OVER (...)` | Window operations |
# MAGIC | **CTE** | `WITH cte AS (...) SELECT ...` | Common table expressions |
# MAGIC | **Mix APIs** | SQL → `.filter()` → SQL | Combine approaches |