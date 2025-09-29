# Databricks notebook source
# MAGIC %md
# MAGIC # 05. Practical Exercises - Real-World Scenarios
# MAGIC 
# MAGIC This notebook contains comprehensive practical exercises that combine all the concepts you've learned. These exercises simulate real-world data engineering and analytics scenarios that you'll encounter in your daily work.
# MAGIC 
# MAGIC ## Exercise Categories:
# MAGIC 1. **Data Pipeline Building** - ETL processes with validation
# MAGIC 2. **Business Analytics** - Complex reporting and KPI calculation
# MAGIC 3. **Data Quality Management** - Cleaning and validation workflows
# MAGIC 4. **Performance Optimization** - Efficient data processing
# MAGIC 5. **Real-time Analytics** - Streaming data scenarios
# MAGIC 6. **Advanced Transformations** - Complex business logic implementation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup - Comprehensive Sample Dataset
# MAGIC 
# MAGIC We'll create a realistic e-commerce dataset with multiple related tables.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pandas as pd
from datetime import date, datetime, timedelta
import random

# Set random seed for reproducible results
random.seed(42)

# Create comprehensive e-commerce dataset

# Customers table
customers_data = []
for i in range(1, 101):  # 100 customers
    signup_date = date(2022, 1, 1) + timedelta(days=random.randint(0, 700))
    tier = random.choices(['Bronze', 'Silver', 'Gold', 'Platinum'], weights=[40, 30, 20, 10])[0]
    city = random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose'])
    customers_data.append((
        i,
        f"Customer_{i:03d}",
        f"customer{i:03d}@email.com",
        tier,
        signup_date,
        city,
        random.choice(['Active', 'Inactive', 'Suspended']) if random.random() > 0.9 else 'Active'
    ))

customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("tier", StringType(), True),
    StructField("signup_date", DateType(), True),
    StructField("city", StringType(), True),
    StructField("status", StringType(), True)
])

customers_df = spark.createDataFrame(customers_data, customers_schema)

# Products table
products_data = [
    (1, "Laptop Pro", "Electronics", 1299.99, date(2022, 1, 15), True),
    (2, "Wireless Headphones", "Electronics", 199.99, date(2022, 2, 1), True),
    (3, "Office Chair", "Furniture", 299.99, date(2022, 1, 20), True),
    (4, "Standing Desk", "Furniture", 499.99, date(2022, 3, 1), True),
    (5, "Smartphone", "Electronics", 899.99, date(2022, 1, 10), True),
    (6, "Tablet", "Electronics", 599.99, date(2022, 2, 15), True),
    (7, "Gaming Mouse", "Electronics", 79.99, date(2022, 1, 25), True),
    (8, "Monitor 4K", "Electronics", 399.99, date(2022, 3, 10), True),
    (9, "Bookshelf", "Furniture", 199.99, date(2022, 2, 20), False),
    (10, "Desk Lamp", "Furniture", 89.99, date(2022, 1, 30), True)
]

products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("launch_date", DateType(), True),
    StructField("active", BooleanType(), True)
])

products_df = spark.createDataFrame(products_data, products_schema)

# Orders table - Generate realistic order data
orders_data = []
order_id = 1

for customer_id in range(1, 101):  # Each customer
    num_orders = random.choices([0, 1, 2, 3, 4, 5], weights=[10, 30, 25, 20, 10, 5])[0]
    
    for _ in range(num_orders):
        order_date = date(2022, 1, 1) + timedelta(days=random.randint(0, 700))
        status = random.choices(['Completed', 'Pending', 'Cancelled', 'Returned'], weights=[70, 15, 10, 5])[0]
        
        orders_data.append((
            order_id,
            customer_id,
            order_date,
            status,
            random.uniform(0.05, 0.25) if random.random() < 0.3 else 0.0  # 30% chance of discount
        ))
        order_id += 1

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", DateType(), True),
    StructField("status", StringType(), True),
    StructField("discount_rate", DoubleType(), True)
])

orders_df = spark.createDataFrame(orders_data, orders_schema)

# Order items table
order_items_data = []
item_id = 1

for order in orders_data:
    order_id = order[0]
    num_items = random.choices([1, 2, 3, 4], weights=[50, 30, 15, 5])[0]
    
    selected_products = random.sample(range(1, 11), min(num_items, 10))
    
    for product_id in selected_products:
        quantity = random.randint(1, 3)
        # Add some price variation (sales, etc.)
        base_price = next(p[3] for p in products_data if p[0] == product_id)
        unit_price = base_price * random.uniform(0.8, 1.0)
        
        order_items_data.append((
            item_id,
            order_id,
            product_id,
            quantity,
            unit_price
        ))
        item_id += 1

order_items_schema = StructType([
    StructField("item_id", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True)
])

order_items_df = spark.createDataFrame(order_items_data, order_items_schema)

print("✅ E-commerce dataset created!")
print(f"Customers: {customers_df.count()}")
print(f"Products: {products_df.count()}")
print(f"Orders: {orders_df.count()}")
print(f"Order Items: {order_items_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Data Pipeline Building 📊
# MAGIC 
# MAGIC **Scenario**: You're building a daily ETL pipeline to create a comprehensive customer analytics mart.
# MAGIC 
# MAGIC **Requirements**:
# MAGIC 1. Join all tables to create a unified view
# MAGIC 2. Calculate customer lifetime value (CLV)
# MAGIC 3. Determine customer segments
# MAGIC 4. Handle data quality issues
# MAGIC 5. Create summary statistics

# COMMAND ----------

# MAGIC %md
# MAGIC ### Your Task: Build the Customer Analytics Pipeline

# COMMAND ----------

def build_customer_analytics_pipeline():
    """
    Build a comprehensive customer analytics pipeline
    
    TODO: Implement the following steps:
    1. Create a unified customer view by joining all tables
    2. Calculate key metrics: total_orders, total_spent, avg_order_value, days_since_last_order
    3. Assign customer segments based on CLV and recency
    4. Handle missing values and data quality issues
    5. Create summary statistics by segment
    """
    
    print("=== Building Customer Analytics Pipeline ===")
    
    # Step 1: Create base customer metrics
    customer_metrics = customers_df.join(
        # Order-level aggregations
        orders_df.filter(col("status") == "Completed")
                .groupBy("customer_id")
                .agg(
                    count("*").alias("total_orders"),
                    max("order_date").alias("last_order_date"),
                    min("order_date").alias("first_order_date"),
                    avg("discount_rate").alias("avg_discount_used")
                ), "customer_id", "left"
    ).join(
        # Order items aggregations
        orders_df.filter(col("status") == "Completed")
                .join(order_items_df, "order_id")
                .groupBy("customer_id")
                .agg(
                    sum(col("quantity") * col("unit_price") * (1 - col("discount_rate"))).alias("total_spent"),
                    avg(col("quantity") * col("unit_price") * (1 - col("discount_rate"))).alias("avg_order_value"),
                    sum("quantity").alias("total_items_purchased")
                ), "customer_id", "left"
    )
    
    # Step 2: Calculate derived metrics
    enriched_customers = customer_metrics.withColumn(
        "days_since_signup", datediff(current_date(), "signup_date")
    ).withColumn(
        "days_since_last_order", 
        when(col("last_order_date").isNotNull(), 
             datediff(current_date(), "last_order_date")).otherwise(null())
    ).withColumn(
        "customer_lifetime_value",
        coalesce(col("total_spent"), lit(0))
    ).withColumn(
        "purchase_frequency",
        when(col("total_orders").isNotNull() & (col("days_since_signup") > 0),
             col("total_orders") / (col("days_since_signup") / 30.0)).otherwise(0)
    )
    
    # Step 3: Customer Segmentation
    segmented_customers = enriched_customers.withColumn(
        "clv_tier",
        when(col("customer_lifetime_value") >= 2000, "High Value")
        .when(col("customer_lifetime_value") >= 500, "Medium Value")  
        .when(col("customer_lifetime_value") > 0, "Low Value")
        .otherwise("No Purchase")
    ).withColumn(
        "recency_tier",
        when(col("days_since_last_order").isNull(), "Never Purchased")
        .when(col("days_since_last_order") <= 30, "Recent")
        .when(col("days_since_last_order") <= 90, "Moderate")
        .otherwise("At Risk")
    ).withColumn(
        "customer_segment",
        concat_ws(" - ", col("clv_tier"), col("recency_tier"))
    )
    
    # Step 4: Data Quality Handling
    clean_customers = segmented_customers.fillna({
        "total_orders": 0,
        "total_spent": 0,
        "avg_order_value": 0,
        "total_items_purchased": 0,
        "avg_discount_used": 0,
        "purchase_frequency": 0
    })
    
    return clean_customers

# Execute the pipeline
customer_analytics = build_customer_analytics_pipeline()

# Show results
print("\n=== Customer Analytics Results ===")
customer_analytics.select(
    "customer_name", "tier", "customer_segment", "total_orders", 
    "customer_lifetime_value", "days_since_last_order"
).show(10)

# Summary statistics
print("\n=== Segment Summary ===")
segment_summary = customer_analytics.groupBy("customer_segment").agg(
    count("*").alias("customer_count"),
    avg("customer_lifetime_value").alias("avg_clv"),
    avg("total_orders").alias("avg_orders"),
    avg("purchase_frequency").alias("avg_frequency")
)
segment_summary.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Business Analytics Dashboard 📈
# MAGIC 
# MAGIC **Scenario**: The business team needs a comprehensive dashboard showing key performance indicators (KPIs) and trends.
# MAGIC 
# MAGIC **Requirements**:
# MAGIC 1. Monthly revenue trends
# MAGIC 2. Product performance analysis
# MAGIC 3. Customer cohort analysis
# MAGIC 4. Geographic revenue distribution
# MAGIC 5. Advanced metrics (churn prediction indicators)

# COMMAND ----------

def create_business_dashboard():
    """
    Create comprehensive business analytics dashboard
    
    TODO: Implement analytics for:
    1. Monthly revenue trends with growth rates
    2. Top performing products by multiple metrics
    3. Customer cohort retention analysis
    4. Geographic performance analysis
    5. Early churn warning indicators
    """
    
    print("=== 📊 Business Analytics Dashboard ===")
    
    # 1. Monthly Revenue Trends
    print("\n--- Monthly Revenue Analysis ---")
    monthly_trends = orders_df.filter(col("status") == "Completed") \
        .join(order_items_df, "order_id") \
        .withColumn("month", date_format("order_date", "yyyy-MM")) \
        .withColumn("revenue", col("quantity") * col("unit_price") * (1 - col("discount_rate"))) \
        .groupBy("month").agg(
            sum("revenue").alias("total_revenue"),
            count("order_id").alias("total_orders"),
            countDistinct("customer_id").alias("unique_customers"),
            avg("revenue").alias("avg_order_value")
        ).orderBy("month")
    
    # Add period-over-period growth
    window_month = Window.orderBy("month")
    monthly_with_growth = monthly_trends.withColumn(
        "prev_month_revenue", lag("total_revenue").over(window_month)
    ).withColumn(
        "revenue_growth_rate",
        when(col("prev_month_revenue").isNotNull(),
             (col("total_revenue") - col("prev_month_revenue")) / col("prev_month_revenue") * 100)
    )
    
    monthly_with_growth.show()
    
    # 2. Product Performance Analysis
    print("\n--- Product Performance Analysis ---")
    product_performance = products_df.join(
        order_items_df.join(orders_df.filter(col("status") == "Completed"), "order_id")
                     .groupBy("product_id").agg(
                         sum(col("quantity") * col("unit_price") * (1 - col("discount_rate"))).alias("total_revenue"),
                         sum("quantity").alias("total_units_sold"),
                         count("order_id").alias("total_orders"),
                         avg(col("unit_price")).alias("avg_selling_price")
                     ), "product_id", "left"
    ).withColumn("revenue_per_unit", col("total_revenue") / col("total_units_sold")) \
     .fillna({"total_revenue": 0, "total_units_sold": 0, "total_orders": 0})
    
    # Rank products by different metrics
    product_rankings = product_performance.withColumn(
        "revenue_rank", rank().over(Window.orderBy(col("total_revenue").desc()))
    ).withColumn(
        "units_rank", rank().over(Window.orderBy(col("total_units_sold").desc()))
    ).withColumn(
        "margin_rank", rank().over(Window.orderBy(col("revenue_per_unit").desc()))
    )
    
    product_rankings.select(
        "product_name", "category", "total_revenue", "total_units_sold",
        "revenue_rank", "units_rank", "margin_rank"
    ).show()
    
    # 3. Customer Cohort Analysis
    print("\n--- Customer Cohort Analysis ---")
    
    # Define cohorts by signup month
    customer_cohorts = customers_df.withColumn(
        "signup_month", date_format("signup_date", "yyyy-MM")
    )
    
    # Calculate monthly purchase behavior per cohort
    cohort_analysis = customer_cohorts.join(
        orders_df.filter(col("status") == "Completed")
                .withColumn("purchase_month", date_format("order_date", "yyyy-MM"))
                .groupBy("customer_id", "purchase_month")
                .agg(sum(col("discount_rate")).alias("discount_used"))  # Just to have an aggregation
                .select("customer_id", "purchase_month"), 
        "customer_id", "left"
    ).groupBy("signup_month", "purchase_month").agg(
        countDistinct("customer_id").alias("active_customers")
    ).withColumn(
        "months_since_signup",
        months_between(col("purchase_month"), col("signup_month"))
    )
    
    # Calculate cohort retention rates
    cohort_sizes = customer_cohorts.groupBy("signup_month").agg(
        count("*").alias("cohort_size")
    )
    
    retention_rates = cohort_analysis.join(cohort_sizes, "signup_month") \
        .withColumn("retention_rate", col("active_customers") / col("cohort_size") * 100) \
        .filter(col("months_since_signup") >= 0)
    
    retention_rates.select(
        "signup_month", "months_since_signup", "active_customers", 
        "cohort_size", "retention_rate"
    ).orderBy("signup_month", "months_since_signup").show()
    
    # 4. Geographic Analysis
    print("\n--- Geographic Performance ---")
    geographic_performance = customers_df.join(
        orders_df.filter(col("status") == "Completed")
                .join(order_items_df, "order_id")
                .groupBy("customer_id").agg(
                    sum(col("quantity") * col("unit_price") * (1 - col("discount_rate"))).alias("customer_revenue"),
                    count("order_id").alias("customer_orders")
                ), "customer_id", "left"
    ).groupBy("city").agg(
        count("customer_id").alias("total_customers"),
        sum("customer_revenue").alias("city_revenue"),
        avg("customer_revenue").alias("avg_customer_value"),
        sum("customer_orders").alias("total_orders")
    ).fillna({"city_revenue": 0, "total_orders": 0}) \
     .withColumn("revenue_per_customer", col("city_revenue") / col("total_customers")) \
     .orderBy(col("city_revenue").desc())
    
    geographic_performance.show()
    
    return {
        "monthly_trends": monthly_with_growth,
        "product_performance": product_rankings,
        "cohort_retention": retention_rates,
        "geographic_performance": geographic_performance
    }

# Execute dashboard creation
dashboard_data = create_business_dashboard()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Data Quality Management 🔍
# MAGIC 
# MAGIC **Scenario**: You need to implement a comprehensive data quality framework that can detect, report, and fix data issues.
# MAGIC 
# MAGIC **Requirements**:
# MAGIC 1. Detect various data quality issues
# MAGIC 2. Create automated data profiling
# MAGIC 3. Implement data validation rules
# MAGIC 4. Generate data quality reports
# MAGIC 5. Create data cleaning procedures

# COMMAND ----------

def comprehensive_data_quality_check():
    """
    Implement comprehensive data quality management system
    
    TODO: Create functions that:
    1. Profile all datasets for basic statistics
    2. Detect data quality issues (nulls, duplicates, outliers)
    3. Validate business rules
    4. Generate comprehensive quality reports
    5. Implement automated data cleaning
    """
    
    print("=== 🔍 Data Quality Management System ===")
    
    def profile_dataset(df, dataset_name):
        """Generate comprehensive data profile"""
        print(f"\n--- Data Profile: {dataset_name} ---")
        
        total_rows = df.count()
        total_cols = len(df.columns)
        
        print(f"Dataset: {dataset_name}")
        print(f"Rows: {total_rows:,}")
        print(f"Columns: {total_cols}")
        
        # Column-level profiling
        quality_issues = []
        
        for col_name in df.columns:
            col_type = dict(df.dtypes)[col_name]
            null_count = df.filter(col(col_name).isNull()).count()
            null_pct = (null_count / total_rows) * 100 if total_rows > 0 else 0
            
            distinct_count = df.select(col_name).distinct().count()
            
            print(f"  {col_name} ({col_type}): {null_count} nulls ({null_pct:.1f}%), {distinct_count} distinct values")
            
            if null_pct > 10:
                quality_issues.append(f"{col_name}: High null rate ({null_pct:.1f}%)")
            
            if distinct_count == 1 and total_rows > 1:
                quality_issues.append(f"{col_name}: No variation in data")
        
        # Duplicate check
        duplicate_rows = total_rows - df.dropDuplicates().count()
        if duplicate_rows > 0:
            quality_issues.append(f"Dataset has {duplicate_rows} duplicate rows")
        
        return quality_issues
    
    def validate_business_rules():
        """Validate specific business rules"""
        print(f"\n--- Business Rule Validation ---")
        
        violations = []
        
        # Rule 1: Order amounts should be positive
        negative_amounts = order_items_df.filter(col("unit_price") <= 0).count()
        if negative_amounts > 0:
            violations.append(f"Found {negative_amounts} items with non-positive prices")
        
        # Rule 2: Order dates should not be in the future
        future_orders = orders_df.filter(col("order_date") > current_date()).count()
        if future_orders > 0:
            violations.append(f"Found {future_orders} orders with future dates")
        
        # Rule 3: Customer emails should be unique
        duplicate_emails = customers_df.groupBy("email").count().filter(col("count") > 1).count()
        if duplicate_emails > 0:
            violations.append(f"Found {duplicate_emails} duplicate email addresses")
        
        # Rule 4: Completed orders should have items
        orders_without_items = orders_df.filter(col("status") == "Completed") \
            .join(order_items_df, "order_id", "left_anti").count()
        if orders_without_items > 0:
            violations.append(f"Found {orders_without_items} completed orders without items")
        
        # Rule 5: Products in orders should exist in products table
        invalid_products = order_items_df.join(products_df, "product_id", "left_anti").count()
        if invalid_products > 0:
            violations.append(f"Found {invalid_products} order items referencing non-existent products")
        
        return violations
    
    def detect_outliers():
        """Detect statistical outliers"""
        print(f"\n--- Outlier Detection ---")
        
        outliers = []
        
        # Price outliers in order items
        price_stats = order_items_df.agg(
            avg("unit_price").alias("avg_price"),
            stddev("unit_price").alias("stddev_price")
        ).collect()[0]
        
        if price_stats.stddev_price:
            upper_bound = price_stats.avg_price + (3 * price_stats.stddev_price)
            lower_bound = max(0, price_stats.avg_price - (3 * price_stats.stddev_price))
            
            price_outliers = order_items_df.filter(
                (col("unit_price") > upper_bound) | (col("unit_price") < lower_bound)
            ).count()
            
            if price_outliers > 0:
                outliers.append(f"Found {price_outliers} items with outlier prices")
        
        # Quantity outliers
        qty_stats = order_items_df.agg(
            avg("quantity").alias("avg_qty"),
            stddev("quantity").alias("stddev_qty")
        ).collect()[0]
        
        if qty_stats.stddev_qty:
            qty_upper = qty_stats.avg_qty + (3 * qty_stats.stddev_qty)
            qty_outliers = order_items_df.filter(col("quantity") > qty_upper).count()
            
            if qty_outliers > 0:
                outliers.append(f"Found {qty_outliers} items with unusual quantities")
        
        return outliers
    
    def generate_quality_report():
        """Generate comprehensive quality report"""
        print(f"\n=== 📋 DATA QUALITY REPORT ===")
        
        # Profile each dataset
        datasets = [
            (customers_df, "Customers"),
            (products_df, "Products"), 
            (orders_df, "Orders"),
            (order_items_df, "Order Items")
        ]
        
        all_issues = []
        for df, name in datasets:
            issues = profile_dataset(df, name)
            all_issues.extend([f"{name}: {issue}" for issue in issues])
        
        # Business rule violations
        rule_violations = validate_business_rules()
        all_issues.extend([f"Business Rule: {violation}" for violation in rule_violations])
        
        # Outlier detection
        outlier_issues = detect_outliers()
        all_issues.extend([f"Outlier: {issue}" for issue in outlier_issues])
        
        # Summary
        print(f"\n=== QUALITY SUMMARY ===")
        print(f"Total Issues Found: {len(all_issues)}")
        
        if all_issues:
            print("\nIssues Detail:")
            for i, issue in enumerate(all_issues, 1):
                print(f"{i}. {issue}")
        else:
            print("✅ No data quality issues detected!")
        
        return all_issues
    
    def clean_data():
        """Implement automated data cleaning procedures"""
        print(f"\n--- Automated Data Cleaning ---")
        
        # Clean customers data
        cleaned_customers = customers_df.filter(
            col("email").isNotNull() & 
            col("email").contains("@") &
            col("customer_name").isNotNull()
        ).dropDuplicates(["email"])
        
        # Clean orders data
        cleaned_orders = orders_df.filter(
            col("order_date") <= current_date()
        )
        
        # Clean order items - remove negative prices and zero quantities
        cleaned_order_items = order_items_df.filter(
            (col("unit_price") > 0) & 
            (col("quantity") > 0)
        )
        
        # Clean products data  
        cleaned_products = products_df.filter(
            col("product_name").isNotNull() &
            col("price") > 0
        )
        
        print("Data cleaning completed:")
        print(f"Customers: {customers_df.count()} → {cleaned_customers.count()}")
        print(f"Orders: {orders_df.count()} → {cleaned_orders.count()}")  
        print(f"Order Items: {order_items_df.count()} → {cleaned_order_items.count()}")
        print(f"Products: {products_df.count()} → {cleaned_products.count()}")
        
        return {
            "customers": cleaned_customers,
            "orders": cleaned_orders,
            "order_items": cleaned_order_items,
            "products": cleaned_products
        }
    
    # Execute quality management
    quality_issues = generate_quality_report()
    cleaned_data = clean_data()
    
    return quality_issues, cleaned_data

# Execute data quality management
quality_report, clean_datasets = comprehensive_data_quality_check()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Performance Optimization Challenge ⚡
# MAGIC 
# MAGIC **Scenario**: You have a large dataset and need to optimize query performance for a real-time analytics system.
# MAGIC 
# MAGIC **Requirements**:
# MAGIC 1. Optimize joins for large datasets
# MAGIC 2. Implement effective caching strategies
# MAGIC 3. Use broadcast joins appropriately
# MAGIC 4. Optimize aggregations with partitioning
# MAGIC 5. Implement incremental processing

# COMMAND ----------

def performance_optimization_workshop():
    """
    Performance optimization techniques workshop
    
    TODO: Implement optimizations:
    1. Analyze and optimize join strategies
    2. Implement smart caching for frequently accessed data
    3. Use broadcast joins for reference data
    4. Partition data effectively for queries
    5. Create incremental processing pipeline
    """
    
    print("=== ⚡ Performance Optimization Workshop ===")
    
    # Create temporary views for all datasets
    customers_df.createOrReplaceTempView("customers")
    products_df.createOrReplaceTempView("products")
    orders_df.createOrReplaceTempView("orders")
    order_items_df.createOrReplaceTempView("order_items")
    
    def analyze_query_performance():
        """Analyze and compare different query strategies"""
        print("\n--- Query Performance Analysis ---")
        
        # Strategy 1: Basic join without optimization
        print("Strategy 1: Basic joins")
        basic_query = """
        SELECT c.customer_name, p.product_name, oi.quantity, oi.unit_price
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        JOIN order_items oi ON o.order_id = oi.order_id  
        JOIN products p ON oi.product_id = p.product_id
        WHERE o.status = 'Completed'
        """
        
        basic_result = spark.sql(basic_query)
        print("Basic query plan:")
        basic_result.explain()
        
        # Strategy 2: Using broadcast for small tables
        print("\nStrategy 2: Broadcast optimization")
        
        # Caching is NOT SUPPORTED on serverless compute
        try:
            products_df.cache()  # Cache reference data (Classic compute)
            print("✅ Products DataFrame cached (Classic compute)")
        except Exception as e:
            print(f"❌ Caching not supported: {type(e).__name__}")
            print("✅ This is expected on serverless compute")
            print("   ➡️ Serverless automatically manages memory optimization")
        
        optimized_query = customers_df.join(
            orders_df.filter(col("status") == "Completed"), "customer_id"
        ).join(
            order_items_df, "order_id"
        ).join(
            broadcast(products_df), "product_id"  # Broadcast small table
        )
        
        print("Optimized query plan:")
        optimized_query.explain()
        
        return basic_result, optimized_query
    
    def implement_smart_caching():
        """Implement intelligent caching strategies"""
        print("\n--- Smart Caching Implementation ---")
        
        # Cache frequently accessed aggregations
        customer_summary = customers_df.join(
            orders_df.filter(col("status") == "Completed")
                    .groupBy("customer_id")
                    .agg(
                        count("*").alias("total_orders"),
                        sum(col("discount_rate")).alias("total_discounts")
                    ), "customer_id", "left"
        ).join(
            orders_df.filter(col("status") == "Completed")
                    .join(order_items_df, "order_id")
                    .groupBy("customer_id")
                    .agg(
                        sum(col("quantity") * col("unit_price")).alias("total_spent")
                    ), "customer_id", "left"
        )
        
        # Cache this expensive computation (Classic vs Serverless compatibility)
        try:
            customer_summary.cache()
            customer_summary.count()  # Trigger caching
            print("✅ Customer summary cached (Classic compute)")
            cache_enabled = True
        except Exception as e:
            print(f"❌ Caching not supported: {type(e).__name__}")
            print("✅ This is expected on serverless compute")
            print("   ➡️ Serverless automatically manages memory optimization")
            cache_enabled = False
        
        # Create partitioned version for better performance
        customer_summary_partitioned = customer_summary.repartition("city")
        if cache_enabled:
            try:
                customer_summary_partitioned.cache()
                print("✅ Partitioned customer summary cached (Classic compute)")
            except:
                print("❌ Partitioned caching not supported (Serverless)")
        else:
            print("✅ Partitioned customer summary created (Serverless optimized)")
        
        print("✅ Partitioned customer summary cached")
        
        return customer_summary
    
    def create_incremental_processing():
        """Create incremental processing pipeline"""
        print("\n--- Incremental Processing Pipeline ---")
        
        # Simulate processing only recent data
        recent_cutoff = date.today() - timedelta(days=30)
        
        # Process only recent orders
        recent_orders_pipeline = orders_df.filter(col("order_date") >= lit(recent_cutoff)) \
            .join(order_items_df, "order_id") \
            .join(broadcast(products_df), "product_id") \
            .groupBy("product_id", "product_name", "category").agg(
                sum(col("quantity") * col("unit_price")).alias("recent_revenue"),
                sum("quantity").alias("recent_quantity"),
                count("order_id").alias("recent_orders")
            )
        
        # This would typically be merged with historical data
        print("Recent orders processing pipeline created")
        recent_orders_pipeline.show(5)
        
        return recent_orders_pipeline
    
    def optimize_aggregations():
        """Optimize complex aggregations"""
        print("\n--- Aggregation Optimization ---")
        
        # Pre-aggregate data at different levels for faster queries
        
        # Daily aggregations
        daily_sales = orders_df.filter(col("status") == "Completed") \
            .join(order_items_df, "order_id") \
            .join(broadcast(products_df), "product_id") \
            .withColumn("order_date", col("order_date").cast("date")) \
            .groupBy("order_date", "category").agg(
                sum(col("quantity") * col("unit_price")).alias("daily_revenue"),
                sum("quantity").alias("daily_quantity"),
                count("order_id").alias("daily_orders")
            )
        
        # Cache daily aggregations (Classic vs Serverless compatibility)
        try:
            daily_sales.cache()
            daily_sales.count()
            print("✅ Daily sales aggregations cached (Classic compute)")
            daily_cached = True
        except Exception as e:
            print(f"❌ Daily sales caching not supported: {type(e).__name__}")
            print("✅ This is expected on serverless compute")
            daily_cached = False
        
        # Monthly aggregations (built from daily)
        monthly_sales = daily_sales.withColumn("month", date_format("order_date", "yyyy-MM")) \
            .groupBy("month", "category").agg(
                sum("daily_revenue").alias("monthly_revenue"),
                sum("daily_quantity").alias("monthly_quantity"),
                sum("daily_orders").alias("monthly_orders")
            )
        
        if daily_cached:
            try:
                monthly_sales.cache()
                print("✅ Monthly sales aggregations cached (Classic compute)")
            except:
                print("❌ Monthly sales caching not supported (Serverless)")
        else:
            print("✅ Monthly sales aggregations created (Serverless optimized)")
        
        print("✅ Multi-level aggregations cached")
        monthly_sales.show()
        
        return daily_sales, monthly_sales
    
    def partition_strategy_analysis():
        """Analyze different partitioning strategies"""
        print("\n--- Partitioning Strategy Analysis ---")
        
        # Show current partitions
        print(f"Current partitions - Customers: {customers_df.rdd.getNumPartitions()}")
        print(f"Current partitions - Orders: {orders_df.rdd.getNumPartitions()}")
        
        # Strategy 1: Partition by frequently joined key
        customers_by_city = customers_df.repartition("city")
        orders_by_customer = orders_df.repartition("customer_id")
        
        print(f"Repartitioned - Customers by city: {customers_by_city.rdd.getNumPartitions()}")
        print(f"Repartitioned - Orders by customer: {orders_by_customer.rdd.getNumPartitions()}")
        
        # Strategy 2: Co-locate data for joins
        colocated_join = customers_by_city.join(orders_by_customer, "customer_id")
        
        print("Co-located join strategy implemented")
        
        return customers_by_city, orders_by_customer
    
    # Execute optimization workshop
    basic_result, optimized_result = analyze_query_performance()
    cached_customer_summary = implement_smart_caching()
    recent_pipeline = create_incremental_processing()
    daily_agg, monthly_agg = optimize_aggregations()
    part_customers, part_orders = partition_strategy_analysis()
    
    return {
        "basic_result": basic_result,
        "optimized_result": optimized_result,
        "cached_summary": cached_customer_summary,
        "incremental_pipeline": recent_pipeline,
        "daily_aggregations": daily_agg,
        "monthly_aggregations": monthly_agg
    }

# Execute performance optimization
optimization_results = performance_optimization_workshop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Advanced Analytics & Machine Learning Prep 🤖
# MAGIC 
# MAGIC **Scenario**: Prepare datasets and features for machine learning models to predict customer churn and lifetime value.
# MAGIC 
# MAGIC **Requirements**:
# MAGIC 1. Feature engineering for ML models
# MAGIC 2. Create customer behavior features
# MAGIC 3. Time-series feature extraction
# MAGIC 4. Data preparation for modeling
# MAGIC 5. Advanced statistical analysis

# COMMAND ----------

def advanced_analytics_prep():
    """
    Advanced analytics and ML preparation
    
    TODO: Implement:
    1. Comprehensive feature engineering
    2. Customer behavior pattern analysis
    3. Time-series features for trends
    4. Statistical analysis and correlations
    5. Data preparation for ML models
    """
    
    print("=== 🤖 Advanced Analytics & ML Preparation ===")
    
    def engineer_customer_features():
        """Engineer comprehensive customer features"""
        print("\n--- Customer Feature Engineering ---")
        
        # Base customer features
        customer_features = customers_df.withColumn(
            "account_age_days", datediff(current_date(), "signup_date")
        ).withColumn(
            "tier_score", 
            when(col("tier") == "Platinum", 4)
            .when(col("tier") == "Gold", 3)
            .when(col("tier") == "Silver", 2)
            .otherwise(1)
        )
        
        # Transactional features
        transaction_features = orders_df.filter(col("status") == "Completed") \
            .join(order_items_df, "order_id") \
            .groupBy("customer_id").agg(
                # Purchase behavior
                count("order_id").alias("total_orders"),
                sum(col("quantity") * col("unit_price")).alias("total_spent"),
                avg(col("quantity") * col("unit_price")).alias("avg_order_value"),
                min("order_date").alias("first_purchase_date"),
                max("order_date").alias("last_purchase_date"),
                
                # Product diversity
                countDistinct("product_id").alias("unique_products"),
                sum("quantity").alias("total_items"),
                avg("quantity").alias("avg_items_per_order"),
                
                # Discount usage
                avg("discount_rate").alias("avg_discount_rate"),
                sum(when(col("discount_rate") > 0, 1).otherwise(0)).alias("discount_orders")
            )
        
        # Recency, Frequency, Monetary (RFM) analysis
        rfm_features = transaction_features.withColumn(
            "recency_days", datediff(current_date(), "last_purchase_date")
        ).withColumn(
            "purchase_frequency", col("total_orders") / (datediff(current_date(), "first_purchase_date") / 30.0)
        ).withColumn(
            "monetary_value", col("total_spent")
        )
        
        # Advanced behavioral features
        behavioral_features = rfm_features.withColumn(
            "avg_days_between_orders",
            datediff("last_purchase_date", "first_purchase_date") / col("total_orders")
        ).withColumn(
            "product_diversity_score", 
            col("unique_products") / col("total_orders")
        ).withColumn(
            "discount_dependency",
            col("discount_orders") / col("total_orders")
        ).withColumn(
            "customer_value_score",
            (col("total_spent") / 1000) + (col("purchase_frequency") * 2) + col("tier_score")
        )
        
        # Join all features
        final_features = customer_features.join(behavioral_features, "customer_id", "left") \
            .fillna({
                "total_orders": 0, "total_spent": 0, "avg_order_value": 0,
                "unique_products": 0, "total_items": 0, "avg_items_per_order": 0,
                "avg_discount_rate": 0, "discount_orders": 0, "recency_days": 999,
                "purchase_frequency": 0, "monetary_value": 0, "avg_days_between_orders": 0,
                "product_diversity_score": 0, "discount_dependency": 0, "customer_value_score": 0
            })
        
        return final_features
    
    def create_time_series_features():
        """Create time-series based features"""
        print("\n--- Time Series Feature Engineering ---")
        
        # Monthly customer activity
        monthly_activity = orders_df.filter(col("status") == "Completed") \
            .join(order_items_df, "order_id") \
            .withColumn("month", date_format("order_date", "yyyy-MM")) \
            .groupBy("customer_id", "month").agg(
                sum(col("quantity") * col("unit_price")).alias("monthly_spend"),
                count("order_id").alias("monthly_orders")
            )
        
        # Calculate trends using window functions
        customer_window = Window.partitionBy("customer_id").orderBy("month")
        
        trends = monthly_activity.withColumn(
            "prev_month_spend", lag("monthly_spend").over(customer_window)
        ).withColumn(
            "spend_trend",
            when(col("prev_month_spend").isNotNull(),
                 col("monthly_spend") - col("prev_month_spend")).otherwise(0)
        ).withColumn(
            "spend_volatility",
            abs(col("spend_trend"))
        )
        
        # Aggregate trends per customer
        trend_features = trends.groupBy("customer_id").agg(
            avg("spend_trend").alias("avg_spend_trend"),
            stddev("monthly_spend").alias("spend_volatility"),
            count("month").alias("active_months"),
            max("monthly_spend").alias("max_monthly_spend"),
            min("monthly_spend").alias("min_monthly_spend")
        )
        
        return trend_features
    
    def churn_prediction_features():
        """Create features specifically for churn prediction"""
        print("\n--- Churn Prediction Features ---")
        
        # Define churn (no purchase in last 90 days)
        cutoff_date = date.today() - timedelta(days=90)
        
        churn_features = orders_df.filter(col("status") == "Completed") \
            .groupBy("customer_id").agg(
                max("order_date").alias("last_order_date")
            ).withColumn(
                "is_churned", 
                when(col("last_order_date") < lit(cutoff_date), 1).otherwise(0)
            ).withColumn(
                "days_since_last_order",
                datediff(current_date(), "last_order_date")
            )
        
        # Leading indicators of churn
        churn_indicators = orders_df.filter(col("status") == "Completed") \
            .join(order_items_df, "order_id") \
            .filter(col("order_date") >= lit(cutoff_date - timedelta(days=180))) \
            .groupBy("customer_id").agg(
                count("order_id").alias("recent_orders"),
                avg(col("quantity") * col("unit_price")).alias("recent_avg_order_value"),
                countDistinct(date_format("order_date", "yyyy-MM")).alias("active_recent_months")
            )
        
        final_churn_features = churn_features.join(churn_indicators, "customer_id", "left") \
            .fillna({
                "recent_orders": 0, "recent_avg_order_value": 0, "active_recent_months": 0
            })
        
        return final_churn_features
    
    def statistical_analysis():
        """Perform statistical analysis on features"""
        print("\n--- Statistical Analysis ---")
        
        # Get all features
        customer_features = engineer_customer_features()
        
        # Calculate correlations between key metrics
        correlation_features = ["total_orders", "total_spent", "avg_order_value", 
                              "unique_products", "recency_days", "customer_value_score"]
        
        # Convert to pandas for correlation analysis
        sample_data = customer_features.select(*correlation_features).sample(0.1).toPandas()
        
        if len(sample_data) > 10:
            correlations = sample_data.corr()
            print("Key feature correlations:")
            print(correlations.round(3))
        
        # Segmentation analysis
        segments = customer_features.withColumn(
            "clv_segment",
            when(col("total_spent") > 2000, "High")
            .when(col("total_spent") > 500, "Medium")
            .when(col("total_spent") > 0, "Low")
            .otherwise("None")
        ).withColumn(
            "frequency_segment",
            when(col("total_orders") > 5, "Frequent")
            .when(col("total_orders") > 2, "Moderate")
            .when(col("total_orders") > 0, "Occasional")
            .otherwise("None")
        )
        
        segment_analysis = segments.groupBy("clv_segment", "frequency_segment").agg(
            count("*").alias("customer_count"),
            avg("customer_value_score").alias("avg_value_score"),
            avg("recency_days").alias("avg_recency")
        )
        
        segment_analysis.show()
        
        return customer_features, segment_analysis
    
    def prepare_ml_dataset():
        """Prepare final dataset for machine learning"""
        print("\n--- ML Dataset Preparation ---")
        
        # Combine all features
        customer_features = engineer_customer_features()
        time_features = create_time_series_features()
        churn_features = churn_prediction_features()
        
        # Create final ML dataset
        ml_dataset = customer_features.join(time_features, "customer_id", "left") \
            .join(churn_features, "customer_id", "left") \
            .fillna({
                "avg_spend_trend": 0, "spend_volatility": 0, "active_months": 0,
                "max_monthly_spend": 0, "min_monthly_spend": 0,
                "is_churned": 0, "days_since_last_order": 999,
                "recent_orders": 0, "recent_avg_order_value": 0, "active_recent_months": 0
            })
        
        # Select features for modeling
        ml_features = [
            "customer_id", "tier_score", "account_age_days", "total_orders", "total_spent",
            "avg_order_value", "unique_products", "recency_days", "purchase_frequency",
            "product_diversity_score", "discount_dependency", "customer_value_score",
            "avg_spend_trend", "spend_volatility", "active_months", "is_churned"
        ]
        
        final_ml_dataset = ml_dataset.select(*ml_features)
        
        print(f"ML dataset prepared with {final_ml_dataset.count()} customers and {len(ml_features)-1} features")
        
        # Show sample
        final_ml_dataset.show(5)
        
        # Feature statistics
        final_ml_dataset.describe().show()
        
        return final_ml_dataset
    
    # Execute advanced analytics preparation
    customer_features, segment_analysis = statistical_analysis()
    ml_dataset = prepare_ml_dataset()
    
    return {
        "customer_features": customer_features,
        "segment_analysis": segment_analysis,
        "ml_dataset": ml_dataset
    }

# Execute advanced analytics prep
analytics_results = advanced_analytics_prep()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Final Integration Challenge 🏆
# MAGIC 
# MAGIC **Scenario**: Create a complete end-to-end data pipeline that combines all the concepts you've learned.
# MAGIC 
# MAGIC **Requirements**:
# MAGIC 1. Build a production-ready data pipeline
# MAGIC 2. Implement comprehensive monitoring and alerting
# MAGIC 3. Create automated quality checks
# MAGIC 4. Generate executive summary reports
# MAGIC 5. Optimize for real-world performance

# COMMAND ----------

def final_integration_challenge():
    """
    Final integration challenge - complete data pipeline
    
    TODO: Create a production-ready pipeline that:
    1. Processes data with quality checks
    2. Creates business intelligence dashboards  
    3. Generates automated alerts
    4. Produces executive reports
    5. Monitors pipeline health
    """
    
    print("=== 🏆 Final Integration Challenge ===")
    
    def production_data_pipeline():
        """Complete production data pipeline"""
        print("\n--- Production Data Pipeline ---")
        
        pipeline_start = datetime.now()
        
        # Step 1: Data ingestion with validation
        def ingest_and_validate():
            print("Step 1: Data Ingestion & Validation")
            
            # Validate data freshness
            latest_order = orders_df.agg(max("order_date")).collect()[0][0]
            days_since_latest = (date.today() - latest_order).days if latest_order else 999
            
            if days_since_latest > 7:
                print(f"⚠️  WARNING: Latest data is {days_since_latest} days old")
            
            # Data quality checks
            quality_score = 100
            
            # Check completeness
            null_customers = customers_df.filter(col("customer_name").isNull()).count()
            if null_customers > 0:
                quality_score -= 10
                print(f"❌ Found {null_customers} customers with missing names")
            
            invalid_emails = customers_df.filter(~col("email").contains("@")).count()
            if invalid_emails > 0:
                quality_score -= 15
                print(f"❌ Found {invalid_emails} invalid email addresses")
            
            # Check referential integrity
            orphaned_orders = orders_df.join(customers_df, "customer_id", "left_anti").count()
            if orphaned_orders > 0:
                quality_score -= 20
                print(f"❌ Found {orphaned_orders} orders with invalid customers")
            
            print(f"Data Quality Score: {quality_score}/100")
            return quality_score >= 80
        
        # Step 2: Data transformation
        def transform_data():
            print("Step 2: Data Transformation")
            
            # Create customer 360 view
            customer_360 = customers_df.join(
                orders_df.filter(col("status") == "Completed")
                        .groupBy("customer_id")
                        .agg(
                            count("*").alias("total_orders"),
                            sum("discount_rate").alias("total_discount_used"),
                            max("order_date").alias("last_order_date"),
                            min("order_date").alias("first_order_date")
                        ), "customer_id", "left"
            ).join(
                orders_df.filter(col("status") == "Completed")
                        .join(order_items_df, "order_id")
                        .groupBy("customer_id")
                        .agg(
                            sum(col("quantity") * col("unit_price")).alias("lifetime_value"),
                            countDistinct("product_id").alias("unique_products_bought")
                        ), "customer_id", "left"
            ).fillna({
                "total_orders": 0, "total_discount_used": 0,
                "lifetime_value": 0, "unique_products_bought": 0
            })
            
            # Cache the result (Classic vs Serverless compatibility)
            try:
                customer_360.cache()
                print("✅ Customer 360 view cached (Classic compute)")
            except Exception as e:
                print(f"❌ Customer 360 caching not supported: {type(e).__name__}")
                print("✅ This is expected on serverless compute")
                print("   ➡️ Serverless automatically optimizes query performance")
            
            return customer_360
        
        # Step 3: Business intelligence
        def generate_business_insights(customer_360):
            print("Step 3: Business Intelligence Generation")
            
            # Executive KPIs
            total_customers = customer_360.count()
            active_customers = customer_360.filter(col("total_orders") > 0).count()
            total_revenue = customer_360.agg(sum("lifetime_value")).collect()[0][0] or 0
            avg_clv = total_revenue / active_customers if active_customers > 0 else 0
            
            # Growth metrics
            recent_signups = customers_df.filter(
                col("signup_date") >= (date.today() - timedelta(days=30))
            ).count()
            
            # Alert conditions
            alerts = []
            if active_customers / total_customers < 0.5:
                alerts.append("Low customer activation rate")
            
            if avg_clv < 500:
                alerts.append("Customer lifetime value below target")
            
            if recent_signups < 5:
                alerts.append("Low new customer acquisition")
            
            insights = {
                "total_customers": total_customers,
                "active_customers": active_customers,
                "activation_rate": active_customers / total_customers if total_customers > 0 else 0,
                "total_revenue": total_revenue,
                "avg_clv": avg_clv,
                "recent_signups": recent_signups,
                "alerts": alerts
            }
            
            return insights
        
        # Step 4: Executive reporting
        def create_executive_report(insights, customer_360):
            print("Step 4: Executive Report Generation")
            
            report_date = date.today().strftime("%Y-%m-%d")
            
            # Top-level metrics
            print(f"\n=== EXECUTIVE DASHBOARD - {report_date} ===")
            print(f"📊 Total Customers: {insights['total_customers']:,}")
            print(f"🎯 Active Customers: {insights['active_customers']:,}")
            print(f"📈 Activation Rate: {insights['activation_rate']:.1%}")
            print(f"💰 Total Revenue: ${insights['total_revenue']:,.2f}")
            print(f"💵 Avg Customer LTV: ${insights['avg_clv']:,.2f}")
            print(f"🆕 New Signups (30d): {insights['recent_signups']:,}")
            
            # Customer segments
            segment_summary = customer_360.withColumn(
                "segment",
                when(col("lifetime_value") > 1000, "VIP")
                .when(col("lifetime_value") > 500, "Premium")
                .when(col("lifetime_value") > 100, "Standard")
                .when(col("lifetime_value") > 0, "Basic")
                .otherwise("Inactive")
            ).groupBy("segment").agg(
                count("*").alias("count"),
                sum("lifetime_value").alias("segment_revenue")
            ).orderBy(col("segment_revenue").desc())
            
            print("\n📊 Customer Segments:")
            segment_summary.show()
            
            # Top customers
            top_customers = customer_360.filter(col("lifetime_value") > 0) \
                .orderBy(col("lifetime_value").desc()) \
                .select("customer_name", "tier", "lifetime_value", "total_orders") \
                .limit(5)
            
            print("\n🏆 Top 5 Customers:")
            top_customers.show()
            
            # Geographic performance
            geo_performance = customer_360.groupBy("city").agg(
                count("*").alias("customers"),
                sum("lifetime_value").alias("city_revenue")
            ).filter(col("city_revenue") > 0) \
             .orderBy(col("city_revenue").desc())
            
            print("\n🌍 Geographic Performance:")
            geo_performance.show()
            
            return segment_summary, top_customers, geo_performance
        
        # Step 5: Pipeline monitoring
        def monitor_pipeline(pipeline_start):
            print("Step 5: Pipeline Monitoring")
            
            pipeline_end = datetime.now()
            duration = (pipeline_end - pipeline_start).seconds
            
            print(f"⏱️  Pipeline Duration: {duration} seconds")
            
            # Performance metrics
            if duration > 300:  # 5 minutes
                print("⚠️  Pipeline running slower than expected")
            
            # Memory usage check (simplified)
            cached_tables = spark.catalog.listTables()
            cached_count = len([t for t in cached_tables if 'temp' not in t.name.lower()])
            print(f"📊 Cached Tables: {cached_count}")
            
            return duration
        
        # Execute pipeline
        if ingest_and_validate():
            customer_360 = transform_data()
            insights = generate_business_insights(customer_360)
            segment_summary, top_customers, geo_performance = create_executive_report(insights, customer_360)
            duration = monitor_pipeline(pipeline_start)
            
            # Final alerts
            if insights["alerts"]:
                print("\n🚨 ALERTS:")
                for alert in insights["alerts"]:
                    print(f"  - {alert}")
            else:
                print("\n✅ No alerts - all metrics within target ranges")
            
            return {
                "customer_360": customer_360,
                "insights": insights,
                "reports": {
                    "segments": segment_summary,
                    "top_customers": top_customers,
                    "geographic": geo_performance
                },
                "pipeline_duration": duration
            }
        else:
            print("❌ Pipeline halted due to data quality issues")
            return None
    
    # Execute the complete pipeline
    pipeline_result = production_data_pipeline()
    
    if pipeline_result:
        print("\n🎉 PIPELINE COMPLETED SUCCESSFULLY!")
        return pipeline_result
    else:
        print("\n💥 PIPELINE FAILED - Check data quality issues")
        return None

# Execute final integration challenge
final_result = final_integration_challenge()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎓 Congratulations! Course Summary
# MAGIC 
# MAGIC You have successfully completed the Python/PySpark fundamentals course! Here's what you've accomplished:
# MAGIC 
# MAGIC ### 📚 **Core Concepts Mastered:**
# MAGIC - ✅ **Python Basics** - Variables, functions, data structures with SQL perspective
# MAGIC - ✅ **PySpark DataFrames** - Creation, transformations, actions, and operations
# MAGIC - ✅ **SQL Integration** - Temporary views, spark.sql(), mixed operations
# MAGIC - ✅ **Advanced Operations** - Joins, window functions, UDFs, performance optimization
# MAGIC - ✅ **Real-world Applications** - Complete data pipelines and business analytics
# MAGIC 
# MAGIC ### 🛠️ **Practical Skills Developed:**
# MAGIC - Data pipeline building and ETL processes
# MAGIC - Business analytics and KPI calculation
# MAGIC - Data quality management and validation
# MAGIC - Performance optimization techniques
# MAGIC - Machine learning data preparation
# MAGIC - Production-ready pipeline development
# MAGIC 
# MAGIC ### 🚀 **Next Steps:**
# MAGIC 1. **Practice** - Apply these concepts to your own datasets
# MAGIC 2. **Explore** - Dive deeper into Spark MLlib for machine learning
# MAGIC 3. **Optimize** - Learn about Spark tuning and cluster management
# MAGIC 4. **Stream** - Explore Spark Streaming for real-time processing
# MAGIC 5. **Deploy** - Learn about production deployment patterns
# MAGIC 
# MAGIC ### 📖 **Key Resources:**
# MAGIC - [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
# MAGIC - [Databricks Documentation](https://docs.databricks.com/)
# MAGIC - [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
# MAGIC - [Performance Tuning Guide](https://spark.apache.org/docs/latest/tuning.html)
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC **Remember**: The key to mastering PySpark is practice and experimentation. Start with simple datasets and gradually work up to more complex scenarios. The combination of your SQL knowledge and these new Python/PySpark skills will make you a powerful data professional!
# MAGIC 
# MAGIC **Happy coding! 🎉**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Final Exercise Checklist
# MAGIC 
# MAGIC Use this checklist to track your progress through the exercises:
# MAGIC 
# MAGIC ### Exercise 1: Data Pipeline Building 📊
# MAGIC - [ ] Built customer analytics pipeline
# MAGIC - [ ] Calculated customer lifetime value
# MAGIC - [ ] Implemented customer segmentation
# MAGIC - [ ] Handled data quality issues
# MAGIC - [ ] Generated summary statistics
# MAGIC 
# MAGIC ### Exercise 2: Business Analytics Dashboard 📈
# MAGIC - [ ] Created monthly revenue trends
# MAGIC - [ ] Analyzed product performance
# MAGIC - [ ] Implemented cohort analysis
# MAGIC - [ ] Built geographic analysis
# MAGIC - [ ] Generated executive insights
# MAGIC 
# MAGIC ### Exercise 3: Data Quality Management 🔍
# MAGIC - [ ] Implemented data profiling
# MAGIC - [ ] Created quality validation rules
# MAGIC - [ ] Built automated quality checks
# MAGIC - [ ] Generated quality reports
# MAGIC - [ ] Implemented data cleaning procedures
# MAGIC 
# MAGIC ### Exercise 4: Performance Optimization ⚡
# MAGIC - [ ] Optimized join strategies
# MAGIC - [ ] Implemented smart caching
# MAGIC - [ ] Used broadcast joins effectively
# MAGIC - [ ] Created partitioning strategies
# MAGIC - [ ] Built incremental processing
# MAGIC 
# MAGIC ### Exercise 5: Advanced Analytics & ML Prep 🤖
# MAGIC - [ ] Engineered comprehensive features
# MAGIC - [ ] Created time-series features
# MAGIC - [ ] Built churn prediction features
# MAGIC - [ ] Performed statistical analysis
# MAGIC - [ ] Prepared ML-ready datasets
# MAGIC 
# MAGIC ### Exercise 6: Final Integration Challenge 🏆
# MAGIC - [ ] Built production-ready pipeline
# MAGIC - [ ] Implemented monitoring and alerting
# MAGIC - [ ] Created executive reports
# MAGIC - [ ] Optimized for performance
# MAGIC - [ ] Handled error scenarios
# MAGIC 
# MAGIC **Total Progress: ___/30 items completed**