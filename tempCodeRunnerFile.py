from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *

spark = SparkSession.builder \
    .appName('Practise') \
    .config("spark.jars", "C:\\Users\\Mahateer Muhammad\Desktop\\retail data analyzer with pyspark\\postgresql-42.7.1.jar") \
    .getOrCreate()
df = spark.read.option("header", "true").csv("C:\\Users\Mahateer Muhammad\\Desktop\\retail data analyzer with pyspark\\Online-Retail.csv", inferSchema=True)
df.show(10)

#dropping data with missing InvoiceNo or CustomerID

df = df.dropna(subset=['CustomerID','InvoiceNo'])

df = df.drop_duplicates()

df = df.filter((col("Quantity")>0) & (col('UnitPrice')>0))

#remove trailing spaces
df = df.withColumn("Description",trim(col("Description")))

df = df.withColumn("InvoiceDate", to_timestamp("InvoiceDate", "M/d/yyyy H:mm"))

#Create a new column InvoiceYearMonth as YYYY-MM from InvoiceDate.
df = df.withColumn("InvoiceYearMonth", date_format("InvoiceDate", "yyyy-MM"))


#Create a new column TotalPrice = Quantity * UnitPrice.
df = df.withColumn("TotalPrice",col("Quantity")*col("UnitPrice"))


#Extract DayOfWeek from InvoiceDate (e.g., Monday, Tuesday).
df = df.withColumn("InvoiceDay",date_format("InvoiceDate", "EEEE"))


#Standardize Description to lowercase.
df = df.withColumn("Description",lower(col("Description")))
df.show()

#🔹 4. Aggregations
#Compute total revenue per Country and sort in descending order.
total_revenue = df.groupBy("Country").sum("TotalPrice").orderBy(col("sum(TotalPrice)").desc())
total_revenue.show()

#Compute number of orders and average basket size per CustomerID.
orders_basket = df.groupby("CustomerID").agg(
    count_distinct(col("InvoiceNo")).alias("TotalOrders"),
    avg(col("TotalPrice")).alias("AvgBasketSize")
)
orders_basket.show()

#Count the number of unique products purchased per customer.
unique_products = df.groupby("CustomerID").agg(
    count_distinct(col("StockCode")).alias("UniquePoducts")
)
unique_products.show()


#------------------------------------------------------------------------------------#

#🔹 5. Window Functions
#For each CustomerID, rank their purchases by TotalPrice descending (most expensive first).
# ranking_purchase = Window.partitionBy(col("CustomerID")).orderBy(col("TotalPrice").desc())
# df= df.withColumn("Rank",rank().over(ranking_purchase))
# df.show()

#For each day, calculate the rolling average of TotalPrice over a 3-day window.
#rolling_average = Window.orderBy(col("InvoiceDate")).rowsBetween(-2,0)
#df = df.withColumn("RollingAverage",avg("TotalPrice").over(rolling_average))
#df.show()

#---------------------------------------------------------------------------#
#🔹 6. UDFs
#Write a UDF that labels each TotalPrice value:

#Low (<50), Medium (50-200), High (>200)
# def udf_label(price):
#     if price < 50:
#         return "Low"
#     elif price >= 50 and price <= 200 :
#         return "Medium"  
#     else :
#         return "High" 

# Apply this UDF to classify transactions into a new column SpendCategory.
# spend_udf = udf(udf_label,StringType())


#changed with native python 3.13 doesn't support spark as of now
df = df.withColumn("SpendCategory",
    when(col("TotalPrice") < 50, "Low")
    .when(col("TotalPrice") <= 200, "Medium")
    .otherwise("High")
)

df.show()

df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "retail") \
    .option("user", "postgres") \
    .option("password", "Mm200429") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

