# PySpark Medallion Architecture Learning Repository

This repository contains a complete implementation of the Medallion Architecture (Bronze, Silver, Gold layers) using PySpark, designed for learning data engineering concepts.

## What's Fixed

✅ **Removed Delta Lake Dependencies**: The original implementation used Delta Lake which requires additional JAR files. This version uses Parquet format instead, making it work out-of-the-box with standard PySpark installations.

✅ **Working Scripts**: All scripts (`bronze_ingestion.py`, `silver_transformation.py`, `gold_aggregation.py`) are fully functional and include proper error handling.

✅ **Complete Notebook**: The `pyspark_medallion_architecture_guide.ipynb` notebook contains working code examples for all exercises.

✅ **Proper Path Management**: All file paths are correctly configured to work within the project structure.

## Project Structure

```
├── conf/
│   └── spark_session_config.py    # Spark session configuration
├── scripts/
│   ├── bronze_ingestion.py         # Raw data ingestion
│   ├── silver_transformation.py    # Data cleaning and enrichment
│   └── gold_aggregation.py         # Data aggregation for analytics
├── data/
│   ├── raw/
│   │   └── sales_data.csv          # Sample sales data
│   ├── bronze/                     # Raw data in Parquet format
│   ├── silver/                     # Cleaned and enriched data
│   └── gold/                       # Aggregated data for analytics
└── notebooks/
    └── pyspark_medallion_architecture_guide.ipynb
```

## How to Use

### Option 1: Run the Complete Notebook
Open and run `notebooks/pyspark_medallion_architecture_guide.ipynb` - it contains all the working code and will execute the entire pipeline.

### Option 2: Run Scripts Individually
From the project root directory:

```bash
# 1. Bronze Layer: Ingest raw data
python scripts/bronze_ingestion.py

# 2. Silver Layer: Clean and enrich data
python scripts/silver_transformation.py

# 3. Gold Layer: Aggregate data for analytics
python scripts/gold_aggregation.py
```

## What Each Layer Does

### Bronze Layer
- Reads raw CSV data
- Saves as Parquet format (preserving original structure)
- No transformations applied

### Silver Layer
- Reads from Bronze layer
- Adds calculated columns (total_price = quantity * price)
- Adds processing timestamp
- Converts transaction_date to proper date format
- Provides data quality and statistics

### Gold Layer
- Reads from Silver layer
- Aggregates data by product and city
- Calculates metrics like total revenue, transaction counts, etc.
- Optimized for analytical queries

## Key Features

- **No External Dependencies**: Works with standard PySpark installation
- **Error Handling**: Proper file existence checks and error messages
- **Comprehensive Examples**: Multiple SQL queries and transformations
- **Educational Comments**: Well-documented code for learning
- **Modular Design**: Separate scripts for each layer

## Sample Queries Included

The notebook includes examples of:
- Filtering data by various criteria
- Grouping and aggregation operations
- Spark SQL queries on the Gold layer
- Summary statistics and analytics

## Requirements

- Python 3.7+
- PySpark 3.0+
- pandas (for data preview)

No additional JAR files or Delta Lake setup required!

## Learning Path

1. Start with the notebook to understand the complete flow
2. Examine each script to understand the implementation details
3. Try modifying the transformations and aggregations
4. Experiment with your own data

Happy learning! 🚀
