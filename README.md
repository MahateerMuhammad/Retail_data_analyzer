# Retail Data Analyzer with PySpark

A PySpark-based retail data processing and analysis tool that cleans, transforms, and analyzes online retail transaction data.

## Overview

This project processes retail transaction data using Apache Spark and stores the results in PostgreSQL. It performs data cleaning, feature engineering, aggregations, and advanced analytics using window functions.

## Features

- **Data Loading**: Reads CSV files with automatic schema inference
- **Data Cleaning**: Removes missing values, duplicates, and invalid records
- **Data Transformation**: Creates new columns for analysis
- **Aggregations**: Calculates revenue by country, customer metrics, and product diversity
- **Window Functions**: Adds ranking and rolling averages
- **Spend Classification**: Categorizes purchases by amount
- **Database Integration**: Stores processed data in PostgreSQL

## Requirements

### Software Dependencies
- Python 3.7+
- Apache Spark 3.x
- Hadoop (for Windows users)
- PostgreSQL

### Python Libraries
```
pyspark
```

### Database
- PostgreSQL server running on localhost:5432
- Database named 'postgres'

## Installation

1. **Install Hadoop** (Windows users)
   - Download and extract Hadoop to `C:\Hadoop`
   - Set environment variables as configured in the code

2. **Install PostgreSQL**
   - Install PostgreSQL server
   - Create database and user as needed
   - Download PostgreSQL JDBC driver

3. **Install Python Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Setup JDBC Driver**
   - Download `postgresql-42.7.1.jar`
   - Place in project's `jars` folder

## Project Structure

```
RETAIL.../
├── artifacts/
├── data/
│   └── Online-Retail.csv
├── jars/
│   └── postgresql-42.7.1.jar
├── screenshots/
├── README.md
├── requirements.txt
├── retail.py
└── tempCodeRunnerFile.py
```

## Configuration

Update the following paths in `retail.py`:

```python
# File paths
csv_path = "data/Online-Retail.csv"
jar_path = "jars/postgresql-42.7.1.jar"

# Database connection
url = "jdbc:postgresql://localhost:5432/postgres"
user = "your_username"
password = "your_password"
```

## Data Schema

The input CSV should contain these columns:
- `InvoiceNo`: Invoice number
- `StockCode`: Product code
- `Description`: Product description
- `Quantity`: Number of items
- `InvoiceDate`: Transaction date (M/d/yyyy H:mm format)
- `UnitPrice`: Price per unit
- `CustomerID`: Customer identifier
- `Country`: Country of purchase

## Usage

Run the analyzer:
```bash
python retail.py
```

## Data Processing Pipeline

1. **Data Loading**
   - Loads CSV with header and schema inference
   - Displays data sample and schema

2. **Data Cleaning**
   - Removes records with missing CustomerID or InvoiceNo
   - Eliminates duplicate records
   - Filters out negative quantities and prices
   - Trims whitespace from descriptions

3. **Data Transformation**
   - Converts InvoiceDate to timestamp format
   - Creates InvoiceYearMonth column
   - Calculates TotalPrice (Quantity × UnitPrice)
   - Extracts day of week from date
   - Standardizes descriptions to lowercase

4. **Feature Engineering**
   - Ranks purchases by amount per customer
   - Calculates 3-day rolling average of prices
   - Classifies spending into Low/Medium/High categories

5. **Analytics**
   - Revenue analysis by country
   - Customer order patterns and basket sizes
   - Product diversity per customer

6. **Data Storage**
   - Saves processed data to PostgreSQL table 'retail'

## Output

The program generates the following analyses:

### Revenue by Country
Shows total revenue for each country, sorted by highest revenue.

### Customer Metrics
- Total orders per customer
- Average basket size per customer
- Unique products purchased per customer

### Spend Categories
- Low: < $50
- Medium: $50 - $200
- High: > $200

## Database Output

The final processed data is stored in PostgreSQL with these additional columns:
- `InvoiceYearMonth`: Year-month format (YYYY-MM)
- `TotalPrice`: Calculated total price
- `InvoiceDay`: Day of week
- `Rank`: Purchase ranking per customer
- `RollingAverage`: 3-day rolling average
- `SpendCategory`: Spending classification

## Error Handling

The application includes error handling for:
- Database connection issues
- File loading problems
- Data processing errors

## Performance Considerations

- Uses Spark's distributed processing for large datasets
- Optimized with proper partitioning for window functions
- Reduced logging to ERROR level for cleaner output

## Troubleshooting

### Common Issues

1. **Hadoop not found**: Ensure HADOOP_HOME is set correctly
2. **JDBC driver missing**: Verify PostgreSQL jar file path
3. **Database connection failed**: Check PostgreSQL service and credentials
4. **Schema errors**: Verify CSV file format matches expected schema

### Log Files

Check Spark logs for detailed error information if processing fails.
