# PySpark Basics for SQL Users

A comprehensive learning resource for data engineers and analysts transitioning from SQL to Python and PySpark in Databricks.

## 🎯 Overview

This repository contains a series of Databricks notebooks designed to teach Python and PySpark fundamentals to professionals with SQL backgrounds. The content bridges the gap between SQL knowledge and distributed data processing with PySpark.

## 📚 Course Structure

### [00. Setup and Configuration](00_Setup_and_Configuration.py)
- **Duration**: 30 minutes
- **Topics**: 
  - Databricks environment understanding
  - SparkSession configuration
  - Sample data creation
  - Performance basics
- **Key Takeaway**: Foundation setup for all subsequent notebooks

### [01. Python Basics for SQL Users](01_Python_Basics_for_SQL_Users.py)
- **Duration**: 60 minutes
- **Topics**:
  - Variables and data types (SQL column equivalents)
  - Collections (lists, dictionaries)
  - Control flow (CASE WHEN equivalents)
  - Functions (stored procedure equivalents)
  - Data manipulation patterns
- **Key Takeaway**: Python fundamentals from SQL perspective

### [02. PySpark DataFrame Basics](02_PySpark_DataFrame_Basics.py)
- **Duration**: 90 minutes
- **Topics**:
  - DataFrame creation and operations
  - Transformations vs Actions
  - Filtering, selecting, sorting
  - Aggregations and grouping
  - SQL equivalents for all operations
- **Key Takeaway**: Core PySpark DataFrame mastery

### [03. PySpark SQL and Temporary Views](03_PySpark_SQL_and_Temp_Views.py)
- **Duration**: 75 minutes
- **Topics**:
  - Creating temporary views
  - Complex SQL queries with spark.sql()
  - CTEs and window functions
  - Mixing SQL and DataFrame API
  - Advanced SQL features
- **Key Takeaway**: Seamless SQL-PySpark integration

### [04. Advanced PySpark Operations](04_Advanced_PySpark_Operations.py)
- **Duration**: 120 minutes
- **Topics**:
  - Complex joins and self-joins
  - Advanced window functions
  - User-defined functions (UDFs)
  - Arrays and nested data
  - Performance optimization
- **Key Takeaway**: Production-ready PySpark skills

### [05. Practical Exercises](05_Practical_Exercises.py)
- **Duration**: 180 minutes
- **Topics**:
  - Real-world data pipeline building
  - Business analytics dashboards
  - Data quality management
  - Performance optimization
  - ML feature engineering
  - Production pipeline development
- **Key Takeaway**: End-to-end practical application

## 🚀 Getting Started

### Prerequisites
- Basic SQL knowledge (SELECT, JOIN, GROUP BY, etc.)
- Access to Databricks workspace
- Familiarity with data analysis concepts

### Setup Instructions

1. **Clone this repository**:
   ```bash
   git clone https://github.com/merca/pyspark_basics.git
   cd pyspark_basics
   ```

2. **Import notebooks to Databricks**:
   - Upload all `.py` files to your Databricks workspace
   - Or use Databricks CLI: `databricks workspace import-dir . /path/in/workspace`

3. **Start with the setup notebook**:
   - Run `00_Setup_and_Configuration.py` first
   - Follow the notebooks in order (00 → 01 → 02 → 03 → 04 → 05)

## 🎯 Learning Path

```
SQL Knowledge
     ↓
01. Python Basics ────→ Variables, Functions, Control Flow
     ↓
02. DataFrame Basics ──→ Core PySpark Operations  
     ↓
03. SQL Integration ───→ Temporary Views, spark.sql()
     ↓
04. Advanced Ops ──────→ Joins, UDFs, Performance
     ↓
05. Real Projects ────→ End-to-end Applications
     ↓
Production Ready! 🚀
```

## 📊 Sample Datasets

The notebooks use realistic sample datasets including:
- **E-commerce data**: Customers, products, orders, order items
- **Employee data**: HR records, sales performance, office locations
- **Time-series data**: Monthly trends, cohort analysis

All datasets are generated programmatically within the notebooks.

## 🔧 Key Concepts Covered

### Python Fundamentals
- ✅ Variables and data types
- ✅ Lists and dictionaries (SQL table equivalents)
- ✅ Control flow (CASE WHEN equivalents)
- ✅ Functions (stored procedure equivalents)
- ✅ Error handling (TRY/CATCH equivalents)

### PySpark Core
- ✅ DataFrame creation and schema definition
- ✅ Transformations vs Actions (lazy evaluation)
- ✅ Filtering and selection (WHERE clauses)
- ✅ Aggregations and grouping (GROUP BY)
- ✅ Joins (INNER, LEFT, RIGHT, FULL)

### Advanced Topics
- ✅ Window functions and analytics
- ✅ User-defined functions (UDFs)
- ✅ Complex data types (arrays, structs)
- ✅ Performance optimization
- ✅ Data quality management

### Production Skills
- ✅ Pipeline development patterns
- ✅ Error handling and monitoring
- ✅ Performance tuning strategies
- ✅ Data validation frameworks
- ✅ Business intelligence reporting

## 🎨 Teaching Approach

### SQL-First Perspective
Every concept is introduced with SQL equivalents:
- **Python lists** → SQL result sets
- **Dictionaries** → Table rows
- **DataFrames** → Database tables
- **Transformations** → Query building
- **Actions** → Query execution

### Practical Examples
Real-world scenarios throughout:
- Customer analytics pipelines
- Business intelligence dashboards
- Data quality management
- Performance optimization
- ML feature engineering

### Progressive Complexity
- Start with familiar SQL concepts
- Gradually introduce Python/PySpark
- Build to complex production scenarios
- End with complete project implementations

## 📈 Learning Outcomes

After completing this course, you will be able to:

1. **Transition from SQL to PySpark** seamlessly
2. **Build production data pipelines** in Databricks
3. **Optimize query performance** for large datasets
4. **Implement data quality frameworks** with validation
5. **Create business intelligence dashboards** with PySpark
6. **Prepare data for machine learning** workflows
7. **Apply software engineering best practices** to data projects

## 🛠️ Technologies Used

- **Apache Spark** - Distributed data processing
- **PySpark** - Python API for Spark
- **Databricks** - Cloud analytics platform
- **Python** - Programming language
- **SQL** - Query language foundation

## 📋 Exercise Checklist

Track your progress through the practical exercises:

### Exercise Categories (30 total items)
- [ ] **Data Pipeline Building** (5 items)
- [ ] **Business Analytics** (5 items) 
- [ ] **Data Quality Management** (5 items)
- [ ] **Performance Optimization** (5 items)
- [ ] **Advanced Analytics & ML Prep** (5 items)
- [ ] **Final Integration Challenge** (5 items)

## 🤝 Contributing

Contributions are welcome! Please feel free to:
- Report bugs or issues
- Suggest improvements or new examples
- Add additional exercises or datasets
- Improve documentation

### How to Contribute
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built for data professionals transitioning to modern analytics
- Inspired by the need for practical, SQL-focused PySpark education
- Designed for real-world application in business environments

## 📞 Support

If you have questions or need help:
- Open an issue in this repository
- Review the troubleshooting sections in each notebook
- Check the Databricks documentation for platform-specific questions

---

**Happy Learning! 🎉** 

Transform your SQL skills into powerful distributed data processing capabilities with PySpark!