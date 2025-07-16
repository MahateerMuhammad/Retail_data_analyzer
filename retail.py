import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *

def initialize_spark():
    """Initialize Spark session with required configurations"""
    os.environ['HADOOP_HOME'] = 'C:\\Hadoop'
    os.environ['hadoop.home.dir'] = 'C:\\Hadoop'
    
    spark = SparkSession.builder \
        .appName('Practise') \
        .config("spark.jars", "C:\\Users\\Mahateer Muhammad\\Desktop\\retail data analyzer with pyspark\\jars\\postgresql-42.7.1.jar") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def load_data(spark):
    """Load CSV data and return DataFrame"""
    print("="*50)
    print("LOADING DATA")
    print("="*50)
    
    df = spark.read.option("header", "true").csv(
        "C:\\Users\\Mahateer Muhammad\\Desktop\\retail data analyzer with pyspark\\data\\Online-Retail.csv", 
        inferSchema=True
    )
    
    print(f"Data loaded. Total rows: {df.count()}")
    print("Schema:")
    df.printSchema()
    print("First 10 rows:")
    df.show(10, truncate=False)
    return df

def clean_data(df):
    """Clean and preprocess the data"""
    print("="*50)
    print("CLEANING DATA")
    print("="*50)
    
    original_count = df.count()
    print(f"Original rows: {original_count}")
    
    # Drop rows with missing InvoiceNo or CustomerID
    df = df.dropna(subset=['CustomerID','InvoiceNo'])
    after_dropna = df.count()
    print(f"After removing missing CustomerID/InvoiceNo: {after_dropna}")
    
    # Remove duplicates
    df = df.drop_duplicates()
    after_dedup = df.count()
    print(f"After removing duplicates: {after_dedup}")
    
    # Filter positive quantities and prices
    df = df.filter((col("Quantity")>0) & (col('UnitPrice')>0))
    after_filter = df.count()
    print(f"After filtering positive values: {after_filter}")
    
    # Remove trailing spaces from Description
    df = df.withColumn("Description",trim(col("Description")))
    
    print(f"Total rows removed: {original_count - after_filter}")
    return df

def transform_data(df):
    """Apply transformations to create new columns"""
    print("="*50)
    print("TRANSFORMING DATA")
    print("="*50)
    
    # Convert InvoiceDate to timestamp
    df = df.withColumn("InvoiceDate", to_timestamp("InvoiceDate", "M/d/yyyy H:mm"))
    print("Added timestamp format to InvoiceDate")
    
    # Create InvoiceYearMonth column
    df = df.withColumn("InvoiceYearMonth", date_format("InvoiceDate", "yyyy-MM"))
    print("Added InvoiceYearMonth column")
    
    # Create TotalPrice column
    df = df.withColumn("TotalPrice",col("Quantity")*col("UnitPrice"))
    print("Added TotalPrice column")
    
    # Extract day of week from InvoiceDate
    df = df.withColumn("InvoiceDay",date_format("InvoiceDate", "EEEE"))
    print("Added InvoiceDay column")
    
    # Standardize Description to lowercase
    df = df.withColumn("Description",lower(col("Description")))
    print("Converted Description to lowercase")
    
    return df

def calculate_revenue_by_country(df):
    """Compute total revenue per Country and sort in descending order"""
    print("="*50)
    print("REVENUE BY COUNTRY")
    print("="*50)
    
    total_revenue = df.groupBy("Country").sum("TotalPrice").orderBy(col("sum(TotalPrice)").desc())
    total_revenue.show(20, truncate=False)
    return total_revenue

def calculate_orders_and_basket_size(df):
    """Compute number of orders and average basket size per CustomerID"""
    print("="*50)
    print("ORDERS AND BASKET SIZE BY CUSTOMER")
    print("="*50)
    
    orders_basket = df.groupBy("CustomerID").agg(
        count_distinct(col("InvoiceNo")).alias("TotalOrders"),
        avg(col("TotalPrice")).alias("AvgBasketSize")
    )
    orders_basket.show(20, truncate=False)
    return orders_basket

def calculate_unique_products_per_customer(df):
    """Count the number of unique products purchased per customer"""
    print("="*50)
    print("UNIQUE PRODUCTS PER CUSTOMER")
    print("="*50)
    
    unique_products = df.groupBy("CustomerID").agg(
        count_distinct(col("StockCode")).alias("UniqueProducts")
    )
    unique_products.show(20, truncate=False)
    return unique_products

def add_window_functions(df):
    """Add ranking and rolling average using window functions"""
    print("="*50)
    print("ADDING WINDOW FUNCTIONS")
    print("="*50)
    
    # Rank purchases by TotalPrice per customer
    ranking_purchase = Window.partitionBy(col("CustomerID")).orderBy(col("TotalPrice").desc())
    df = df.withColumn("Rank",rank().over(ranking_purchase))
    print("Added Rank column")
    
    # Calculate rolling average of TotalPrice over 3-day window
    rolling_average = Window.orderBy(col("InvoiceDate")).rowsBetween(-2,0)
    df = df.withColumn("RollingAverage",avg("TotalPrice").over(rolling_average))
    print("Added RollingAverage column")
    
    return df

def add_spend_category(df):
    """Add SpendCategory classification based on TotalPrice"""
    print("="*50)
    print("ADDING SPEND CATEGORY")
    print("="*50)
    
    df = df.withColumn("SpendCategory",
        when(col("TotalPrice") < 50, "Low")
        .when(col("TotalPrice") <= 200, "Medium")
        .otherwise("High")
    )
    print("Added SpendCategory column")
    print("Categories: Low (<50), Medium (50-200), High (>200)")
    return df

def write_to_postgresql(df):
    """Write DataFrame to PostgreSQL with error handling"""
    print("="*50)
    print("WRITING TO POSTGRESQL")
    print("="*50)
    
    try:
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/postgres") \
            .option("dbtable", "retail") \
            .option("user", "postgres") \
            .option("password", "Mm200429") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        print("Data written to PostgreSQL successfully")
    except Exception as e:
        print(f"Error writing to PostgreSQL: {e}")

def main():
    """Main function to execute the complete data processing pipeline"""
    print("="*50)
    print("RETAIL DATA ANALYZER")
    print("="*50)
    
    # Initialize Spark
    spark = initialize_spark()
    
    try:
        # Load data
        df = load_data(spark)
        
        # Clean data
        df = clean_data(df)
        
        # Transform data
        df = transform_data(df)
        
        # Add window functions
        df = add_window_functions(df)
        
        # Add spend category
        df = add_spend_category(df)
        
        # Show final result
        print("="*50)
        print("FINAL PROCESSED DATA")
        print("="*50)
        df.show(20, truncate=False)
        
        # Perform aggregations
        calculate_revenue_by_country(df)
        calculate_orders_and_basket_size(df)
        calculate_unique_products_per_customer(df)
        
        # Write to PostgreSQL
        write_to_postgresql(df)
        
        print("="*50)
        print("PROCESSING COMPLETED")
        print("="*50)
        
    finally:
        # Stop Spark session
        spark.stop()
        print("Spark session stopped")

if __name__ == "__main__":
    main()