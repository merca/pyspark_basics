# Databricks notebook source
# MAGIC %md
# MAGIC # 01. Python Basics for SQL Users
# MAGIC 
# MAGIC This notebook introduces Python fundamentals from a SQL perspective. We'll draw parallels between Python concepts and SQL concepts you already know.
# MAGIC 
# MAGIC ## Topics Covered:
# MAGIC - Variables and data types (like columns and data types in SQL)
# MAGIC - Collections (like tables and result sets)
# MAGIC - Control flow (like CASE WHEN and conditional logic)
# MAGIC - Functions (like stored procedures and UDFs)
# MAGIC - Data manipulation with Python

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Variables and Data Types
# MAGIC 
# MAGIC **SQL Perspective**: In SQL, you have columns with specific data types (INT, VARCHAR, DATE, etc.).
# MAGIC In Python, variables can hold different types of data, and the type can change dynamically.

# COMMAND ----------

# Variables in Python - think of these as column values
employee_id = 123                    # Integer (like INT in SQL)
employee_name = "John Doe"           # String (like VARCHAR in SQL)  
salary = 75000.50                    # Float (like DECIMAL in SQL)
is_active = True                     # Boolean (like BOOLEAN in SQL)
hire_date = "2023-01-15"            # String representing date (we'll convert later)

# Display values - like SELECT in SQL
print(f"Employee ID: {employee_id}")
print(f"Employee Name: {employee_name}")
print(f"Salary: ${salary:,.2f}")
print(f"Is Active: {is_active}")
print(f"Hire Date: {hire_date}")

# COMMAND ----------

# Check data types - like DESCRIBE table in SQL
print("Data Types:")
print(f"employee_id is {type(employee_id)}")
print(f"employee_name is {type(employee_name)}")
print(f"salary is {type(salary)}")
print(f"is_active is {type(is_active)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Lists - Like Result Sets
# MAGIC 
# MAGIC **SQL Perspective**: A list in Python is like a result set from a SELECT query.
# MAGIC It's an ordered collection of items.

# COMMAND ----------

# Lists are like query results with multiple rows
employee_names = ["John Doe", "Jane Smith", "Mike Johnson", "Sarah Wilson"]
employee_salaries = [75000, 85000, 92000, 68000]
departments = ["Engineering", "Marketing", "Engineering", "Sales"]

print("Employee Names (like SELECT name FROM employees):")
for name in employee_names:
    print(f"- {name}")

print(f"\nTotal employees: {len(employee_names)}")  # Like COUNT(*) in SQL

# COMMAND ----------

# Accessing list elements - like accessing specific rows
print("First employee:", employee_names[0])     # Like LIMIT 1
print("Last employee:", employee_names[-1])     # Like ORDER BY ... DESC LIMIT 1
print("First 2 employees:", employee_names[0:2]) # Like LIMIT 2

# List operations - like SQL functions
print(f"Average salary: ${sum(employee_salaries) / len(employee_salaries):,.2f}")  # Like AVG()
print(f"Max salary: ${max(employee_salaries):,.2f}")       # Like MAX()
print(f"Min salary: ${min(employee_salaries):,.2f}")       # Like MIN()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Dictionaries - Like Key-Value Pairs (Records)
# MAGIC 
# MAGIC **SQL Perspective**: A dictionary is like a single row with column names as keys.
# MAGIC Think of it as one record from your table.

# COMMAND ----------

# Dictionary - like a single row from a table
employee_record = {
    "id": 123,
    "name": "John Doe", 
    "department": "Engineering",
    "salary": 75000,
    "is_active": True
}

# Accessing values - like SELECT column_name
print(f"Employee: {employee_record['name']}")
print(f"Salary: ${employee_record['salary']:,}")

# All keys - like column names
print("Available fields:", list(employee_record.keys()))

# All values - like the data in a row
print("Values:", list(employee_record.values()))

# COMMAND ----------

# List of dictionaries - like a complete table
employees = [
    {"id": 1, "name": "John Doe", "department": "Engineering", "salary": 95000},
    {"id": 2, "name": "Jane Smith", "department": "Marketing", "salary": 75000},
    {"id": 3, "name": "Mike Johnson", "department": "Engineering", "salary": 85000},
    {"id": 4, "name": "Sarah Wilson", "department": "Sales", "salary": 65000}
]

print("Employees table (like SELECT * FROM employees):")
for emp in employees:
    print(f"ID: {emp['id']}, Name: {emp['name']}, Dept: {emp['department']}, Salary: ${emp['salary']:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Control Flow - Like CASE WHEN and IF statements
# MAGIC 
# MAGIC **SQL Perspective**: Python's if/elif/else is like CASE WHEN in SQL.

# COMMAND ----------

# IF statements - like CASE WHEN in SQL
salary = 85000

# Simple if (like CASE WHEN salary > 80000 THEN 'High' END)
if salary > 80000:
    salary_level = "High"
elif salary > 60000:
    salary_level = "Medium"  
else:
    salary_level = "Low"

print(f"Salary ${salary:,} is classified as: {salary_level}")

# COMMAND ----------

# One-liner conditional (like CASE WHEN)
salary_category = "High" if salary > 80000 else "Medium" if salary > 60000 else "Low"
print(f"Salary category: {salary_category}")

# Working with our employees data
print("Salary categories for all employees:")
for emp in employees:
    category = "High" if emp['salary'] > 80000 else "Medium" if emp['salary'] > 60000 else "Low"
    print(f"{emp['name']}: ${emp['salary']:,} ({category})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Loops - Like Processing Query Results
# MAGIC 
# MAGIC **SQL Perspective**: Loops in Python are like processing each row in a result set.

# COMMAND ----------

# For loops - like processing each row from SELECT
print("Processing each employee (like a cursor in SQL):")

for employee in employees:
    # This is like processing each row
    bonus = employee['salary'] * 0.1  # Calculate 10% bonus
    total_comp = employee['salary'] + bonus
    
    print(f"{employee['name']}: Base ${employee['salary']:,}, "
          f"Bonus ${bonus:,}, Total ${total_comp:,}")

# COMMAND ----------

# List comprehensions - like SELECT with transformations
# This is a powerful Python feature for data transformation

# Like: SELECT name FROM employees WHERE department = 'Engineering'
engineering_names = [emp['name'] for emp in employees if emp['department'] == 'Engineering']
print("Engineering employees:", engineering_names)

# Like: SELECT name, salary * 1.1 FROM employees  
salary_with_raise = [{
    'name': emp['name'], 
    'new_salary': emp['salary'] * 1.1
} for emp in employees]

print("Salaries with 10% raise:")
for emp in salary_with_raise:
    print(f"{emp['name']}: ${emp['new_salary']:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Functions - Like Stored Procedures and UDFs
# MAGIC 
# MAGIC **SQL Perspective**: Functions in Python are like stored procedures or user-defined functions in SQL.

# COMMAND ----------

# Simple function - like a scalar UDF
def calculate_bonus(salary, bonus_rate=0.1):
    """Calculate employee bonus - like a UDF in SQL"""
    return salary * bonus_rate

# Test the function
test_salary = 75000
bonus = calculate_bonus(test_salary)
print(f"Bonus for ${test_salary:,}: ${bonus:,}")

# Using different bonus rate
senior_bonus = calculate_bonus(test_salary, 0.15)  # 15% for seniors
print(f"Senior bonus for ${test_salary:,}: ${senior_bonus:,}")

# COMMAND ----------

# More complex function - like a table-valued function
def classify_employee(employee_dict):
    """Classify employee based on salary and department"""
    salary = employee_dict['salary']
    dept = employee_dict['department']
    
    # Salary classification
    if salary > 90000:
        salary_level = "Senior"
    elif salary > 70000:
        salary_level = "Mid-level"
    else:
        salary_level = "Junior"
    
    # Department classification
    if dept == "Engineering":
        dept_type = "Technical"
    elif dept in ["Marketing", "Sales"]:
        dept_type = "Business"
    else:
        dept_type = "Other"
    
    return {
        'name': employee_dict['name'],
        'salary_level': salary_level,
        'dept_type': dept_type,
        'classification': f"{salary_level} {dept_type}"
    }

# Apply function to all employees
print("Employee Classifications:")
for emp in employees:
    result = classify_employee(emp)
    print(f"{result['name']}: {result['classification']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Working with Dates - Like Date Functions in SQL
# MAGIC 
# MAGIC **SQL Perspective**: Python datetime is like DATE functions in SQL (DATE_ADD, DATEDIFF, etc.).

# COMMAND ----------

from datetime import datetime, date, timedelta

# Current date - like CURRENT_DATE in SQL
today = date.today()
current_time = datetime.now()

print(f"Today: {today}")
print(f"Current timestamp: {current_time}")

# Date arithmetic - like DATE_ADD in SQL
one_week_later = today + timedelta(days=7)
one_month_ago = today - timedelta(days=30)

print(f"One week from now: {one_week_later}")
print(f"One month ago: {one_month_ago}")

# COMMAND ----------

# Working with date strings (common in data processing)
hire_dates = ["2020-01-15", "2019-03-10", "2021-06-01", "2022-02-20"]

print("Processing hire dates:")
for date_str in hire_dates:
    # Convert string to date - like CAST(string AS DATE)
    hire_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    
    # Calculate tenure - like DATEDIFF
    tenure_days = (today - hire_date).days
    tenure_years = tenure_days / 365.25
    
    print(f"Hired: {hire_date}, Tenure: {tenure_years:.1f} years")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. String Operations - Like String Functions in SQL
# MAGIC 
# MAGIC **SQL Perspective**: Python string methods are like UPPER(), LOWER(), SUBSTR(), CONCAT() in SQL.

# COMMAND ----------

# String operations - like SQL string functions
employee_name = "  John Doe  "

print(f"Original: '{employee_name}'")
print(f"Upper: '{employee_name.upper()}'")           # Like UPPER()
print(f"Lower: '{employee_name.lower()}'")           # Like LOWER()
print(f"Trimmed: '{employee_name.strip()}'")         # Like TRIM()
print(f"Length: {len(employee_name.strip())}")       # Like LEN() or LENGTH()

# String formatting - like CONCAT()
first_name = "John"
last_name = "Doe"
full_name = f"{first_name} {last_name}"              # Like CONCAT(first_name, ' ', last_name)
print(f"Full name: {full_name}")

# String splitting - like splitting delimited data
email = "john.doe@company.com"
username, domain = email.split("@")                  # Like SUBSTRING operations
print(f"Username: {username}, Domain: {domain}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Error Handling - Like TRY/CATCH in SQL
# MAGIC 
# MAGIC **SQL Perspective**: Python's try/except is like TRY/CATCH blocks in SQL.

# COMMAND ----------

# Error handling - like TRY/CATCH in SQL
def safe_divide(a, b):
    """Safely divide two numbers"""
    try:
        result = a / b
        return result
    except ZeroDivisionError:
        print(f"Error: Cannot divide {a} by zero!")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

# Test the function
print("Safe division examples:")
print(f"10 / 2 = {safe_divide(10, 2)}")
print(f"10 / 0 = {safe_divide(10, 0)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Mini Exercise: Employee Data Processing
# MAGIC 
# MAGIC Let's combine what we've learned to process employee data, similar to what you'd do with SQL queries.

# COMMAND ----------

# Extended employee data
extended_employees = [
    {"id": 1, "name": "John Doe", "department": "Engineering", "salary": 95000, "hire_date": "2020-01-15"},
    {"id": 2, "name": "Jane Smith", "department": "Marketing", "salary": 75000, "hire_date": "2019-03-10"},
    {"id": 3, "name": "Mike Johnson", "department": "Engineering", "salary": 85000, "hire_date": "2021-06-01"},
    {"id": 4, "name": "Sarah Wilson", "department": "Sales", "salary": 65000, "hire_date": "2022-02-20"},
    {"id": 5, "name": "Tom Brown", "department": "Engineering", "salary": 98000, "hire_date": "2018-11-05"},
    {"id": 6, "name": "Lisa Davis", "department": "Marketing", "salary": 55000, "hire_date": "2023-01-10"}
]

def process_employee_data(employees_list):
    """Process employee data - like a complex SQL query with multiple operations"""
    
    processed_data = []
    total_salary = 0
    departments = {}
    
    for emp in employees_list:
        # Date processing
        hire_date = datetime.strptime(emp['hire_date'], "%Y-%m-%d").date()
        tenure_years = (date.today() - hire_date).days / 365.25
        
        # Salary classification
        if emp['salary'] > 90000:
            salary_band = "Senior"
            bonus_rate = 0.15
        elif emp['salary'] > 70000:
            salary_band = "Mid-level"
            bonus_rate = 0.12
        else:
            salary_band = "Junior"
            bonus_rate = 0.08
        
        # Calculate bonus and total compensation
        bonus = emp['salary'] * bonus_rate
        total_comp = emp['salary'] + bonus
        
        # Department statistics
        dept = emp['department']
        if dept not in departments:
            departments[dept] = {'count': 0, 'total_salary': 0}
        departments[dept]['count'] += 1
        departments[dept]['total_salary'] += emp['salary']
        
        # Create processed record
        processed_emp = {
            'name': emp['name'],
            'department': dept,
            'salary_band': salary_band,
            'tenure_years': round(tenure_years, 1),
            'base_salary': emp['salary'],
            'bonus': round(bonus, 2),
            'total_compensation': round(total_comp, 2)
        }
        
        processed_data.append(processed_emp)
        total_salary += emp['salary']
    
    return processed_data, departments, total_salary

# Process the data
processed_employees, dept_stats, company_total = process_employee_data(extended_employees)

# Display results
print("=== EMPLOYEE PROCESSING RESULTS ===")
print(f"Total company payroll: ${company_total:,}")
print(f"Average salary: ${company_total / len(extended_employees):,.2f}")

print("\n=== INDIVIDUAL EMPLOYEE DATA ===")
for emp in processed_employees:
    print(f"{emp['name']} ({emp['department']}):")
    print(f"  - Band: {emp['salary_band']}, Tenure: {emp['tenure_years']} years")
    print(f"  - Compensation: ${emp['base_salary']:,} base + ${emp['bonus']:,} bonus = ${emp['total_compensation']:,}")
    print()

print("=== DEPARTMENT STATISTICS ===")
for dept, stats in dept_stats.items():
    avg_salary = stats['total_salary'] / stats['count']
    print(f"{dept}: {stats['count']} employees, Avg salary: ${avg_salary:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Practice Exercises
# MAGIC 
# MAGIC Try these exercises to reinforce your learning:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1: Sales Data Analysis
# MAGIC Create a function that analyzes sales data similar to SQL queries.

# COMMAND ----------

# Sample sales data
sales_data = [
    {"sale_id": 101, "product": "Laptop", "amount": 1200, "region": "North", "month": "2023-01"},
    {"sale_id": 102, "product": "Phone", "amount": 800, "region": "South", "month": "2023-01"},
    {"sale_id": 103, "product": "Tablet", "amount": 600, "region": "North", "month": "2023-02"},
    {"sale_id": 104, "product": "Laptop", "amount": 1300, "region": "West", "month": "2023-02"},
    {"sale_id": 105, "product": "Phone", "amount": 750, "region": "South", "month": "2023-02"}
]

# TODO: Create a function that:
# 1. Calculates total sales by region (like GROUP BY region)
# 2. Finds the best-selling product (like finding MAX)
# 3. Calculates average sale amount
# 4. Returns results in a dictionary

def analyze_sales(sales_list):
    """
    YOUR CODE HERE
    Analyze sales data and return summary statistics
    """
    # Initialize variables
    regional_sales = {}
    product_sales = {}
    total_amount = 0
    
    # Process each sale
    for sale in sales_list:
        # Region totals
        region = sale['region']
        if region not in regional_sales:
            regional_sales[region] = 0
        regional_sales[region] += sale['amount']
        
        # Product totals  
        product = sale['product']
        if product not in product_sales:
            product_sales[product] = 0
        product_sales[product] += sale['amount']
        
        # Running total
        total_amount += sale['amount']
    
    # Find best selling product
    best_product = max(product_sales.items(), key=lambda x: x[1])
    
    return {
        'regional_sales': regional_sales,
        'product_sales': product_sales,
        'best_product': best_product[0],
        'best_product_sales': best_product[1],
        'total_sales': total_amount,
        'average_sale': total_amount / len(sales_list)
    }

# Test your function
results = analyze_sales(sales_data)
print("Sales Analysis Results:")
print(f"Total Sales: ${results['total_sales']:,}")
print(f"Average Sale: ${results['average_sale']:,.2f}")
print(f"Best Product: {results['best_product']} (${results['best_product_sales']:,})")

print("\nRegional Sales:")
for region, amount in results['regional_sales'].items():
    print(f"  {region}: ${amount:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2: Data Filtering and Transformation
# MAGIC Practice filtering and transforming data like you would with WHERE clauses and SELECT statements.

# COMMAND ----------

# Customer data
customers = [
    {"id": 1, "name": "Alice Johnson", "age": 28, "city": "New York", "status": "active"},
    {"id": 2, "name": "Bob Smith", "age": 34, "city": "Los Angeles", "status": "inactive"},
    {"id": 3, "name": "Carol Brown", "age": 45, "city": "Chicago", "status": "active"},
    {"id": 4, "name": "David Wilson", "age": 23, "city": "New York", "status": "active"},
    {"id": 5, "name": "Eve Davis", "age": 39, "city": "Los Angeles", "status": "active"}
]

# TODO: Write code to:
# 1. Filter active customers in New York (like WHERE status = 'active' AND city = 'New York')
# 2. Create a new list with customer names in uppercase
# 3. Calculate average age of active customers
# 4. Group customers by city

# Your solution here:
print("Exercise 2 Solutions:")

# 1. Filter active customers in New York
ny_active = [cust for cust in customers if cust['status'] == 'active' and cust['city'] == 'New York']
print(f"Active customers in New York: {len(ny_active)}")
for cust in ny_active:
    print(f"  - {cust['name']}")

# 2. Customer names in uppercase
uppercase_names = [cust['name'].upper() for cust in customers]
print(f"\nCustomer names (uppercase): {uppercase_names}")

# 3. Average age of active customers
active_customers = [cust for cust in customers if cust['status'] == 'active']
avg_age = sum(cust['age'] for cust in active_customers) / len(active_customers)
print(f"\nAverage age of active customers: {avg_age:.1f}")

# 4. Group by city
cities = {}
for cust in customers:
    city = cust['city']
    if city not in cities:
        cities[city] = []
    cities[city].append(cust['name'])

print("\nCustomers by city:")
for city, names in cities.items():
    print(f"  {city}: {', '.join(names)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC Congratulations! You've learned Python fundamentals from a SQL perspective:
# MAGIC 
# MAGIC ✅ **Variables and data types** (like column types)  
# MAGIC ✅ **Lists and dictionaries** (like result sets and records)  
# MAGIC ✅ **Control flow** (like CASE WHEN statements)  
# MAGIC ✅ **Functions** (like stored procedures)  
# MAGIC ✅ **Data processing** (like complex queries)  
# MAGIC ✅ **Error handling** (like TRY/CATCH)  
# MAGIC 
# MAGIC ### Key Takeaways:
# MAGIC - Python variables are like values in SQL columns
# MAGIC - Lists are like result sets from queries
# MAGIC - Dictionaries are like individual records/rows
# MAGIC - Functions help organize code like stored procedures
# MAGIC - List comprehensions are powerful for data transformation
# MAGIC 
# MAGIC **Next**: Move on to PySpark DataFrames to see how these concepts apply to big data processing!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Python-SQL Quick Reference
# MAGIC 
# MAGIC | Python Concept | SQL Equivalent | Example |
# MAGIC |---------------|----------------|---------|
# MAGIC | `variable = value` | Column assignment | `employee_id = 123` |
# MAGIC | `list[0]` | First row | `employees[0]` |
# MAGIC | `len(list)` | COUNT(*) | `len(employees)` |
# MAGIC | `max(list)` | MAX() | `max(salaries)` |
# MAGIC | `if condition:` | CASE WHEN | `if salary > 80000:` |
# MAGIC | `for item in list:` | Cursor/Row processing | `for emp in employees:` |
# MAGIC | `[x for x in list if condition]` | SELECT WHERE | `[emp for emp in employees if emp['dept'] == 'IT']` |
# MAGIC | `function(args)` | Stored procedure/UDF | `calculate_bonus(salary)` |
# MAGIC | `try:/except:` | TRY/CATCH | Error handling |