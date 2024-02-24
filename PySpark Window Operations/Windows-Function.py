# Databricks notebook source
# MAGIC %md # Loading CSV into DataFrame

# COMMAND ----------

csv_file = 'dbfs:/FileStore/shared_uploads/yateed1437@gmail.com/sample_sales_pyspark.csv'
df = spark.read.format("csv")\
    .option("inferSchema", "true")\
    .option("header", "true")\
    .load(csv_file)

# COMMAND ----------

df.display(10)

# COMMAND ----------

# MAGIC %md # Window creation and Partitioning

# COMMAND ----------

# MAGIC %md ## Window Example 1

# COMMAND ----------

# Calculate Cumulative sales of each product in each store, We define our window
from pyspark.sql import Window
window = Window.partitionBy("store_code", "product_code").orderBy("sales_date")

# COMMAND ----------

# Calculate cumulative sales, we simply apply the sum function over the window
from pyspark.sql.functions import sum
df = df.withColumn("total_sales", sum("sales_qty").over(window))

# COMMAND ----------

# Let us check the first 30 rows for store "B1" to make sure cumulative values are calculated.
from pyspark.sql.functions import col
df.filter(col("store_code") == "B1")\
    .select("store_code", "product_code", "sales_date", "sales_qty", "total_sales")\
    .display(30)

# The first 15 rows belong to product 89912 so the total sales column shows the cumulative sum of the values in the sales quantity column.
# In the 16th row, the product code changes so the cumulative sum starts over.

# COMMAND ----------

# MAGIC %md ## Window Example 2

# COMMAND ----------

# Once we create the window, we can calculate many different agrgegations.
# For Instance, if we apply the max function over the window defined earlier, the output will be the cumulative maximum price of products in the given store.
# Let us write a code to create the window so that we don't have to search above for it
# Define the Window
window = Window.partitionBy("store_code", "product_code").orderBy("sales_date")

# COMMAND ----------

# cumulative max price
from pyspark.sql.functions import max
df = df.withColumn("max_price", max("price").over(window))

# COMMAND ----------

df.filter(col("store_code") == "B1")\
    .select("store_code", "product_code", "sales_date", "max_price")\
    .display(15)

# The values in the "max_price" column increase or remain the same. 
# In the 7th row, the price actually drops but the max price value remains the same because it shows the cumulative maximum value

# COMMAND ----------

# MAGIC %md ## Window Example 3

# COMMAND ----------

# The lag and lead are some of the commonly used window functions.
# They are used quite frequently when analyzing time series data.
# They return the value that is offset before or after the current row. 
# We can specify the offset by using negative values so lag("sales_qty",1) is the same as ("salese_qty", -1).
# Both of them give us the value in the previous row. Let us test
# Define the window
window = Window.partitionBy("store_code", "product_code").orderBy("sales_date")

# COMMAND ----------

# Previous day sales qty
from pyspark.sql.functions import lag, lead
df = df.withColumn("prev_day_sale_lag", lag("sales_qty", 1).over(window))
df = df.withColumn("prev_day_sale_lead", lead("sales_qty", 1).over(window))

# COMMAND ----------

# Check the output for a different product-store pair
df.filter((col("store_code") == "A1") & (col("product_code") == "95955"))\
    .select("sales_date", "sales_qty", "prev_day_sale_lag", "prev_day_sale_lead")\
    .display(15)

# The value in the first row for the previous day is null because they do not have a previous day.

# COMMAND ----------

# MAGIC %md ## Window Example 4

# COMMAND ----------

# Once we define a window based on a partition (eg. store and product in our case), we can narrow it down by using the rows Between Method.
# Let us say we want to calculate the average sales quantity of the last three days within the window.
window = Window.partitionBy("store_code", "product_code")\
    .orderBy("sales_date")\
    .rowsBetween(-3, -1)

# The first parameter is the start and the second parameter is the end. (In this case, -1 indicates row before the current row)
# We customized the window to cover the last three rows for each row. (In this case, -3 indicates third row before the current row)

# COMMAND ----------

# Calculate Mean
from pyspark.sql.functions import mean
df = df.withColumn("last_3_day_avg", mean("sales_qty").over(window))

# COMMAND ----------

# Display Data
df.filter((col("store_code") == "A1") & (col("product_code") == "95955"))\
    .select("sales_date", "sales_qty", "last_3_day_avg")\
    .display()

# For the fourth row(i.e., "2021-05-04"), the last 3 day average is 13.75, which is average of the values in the previous 3 rows 

# COMMAND ----------

# MAGIC %md ## Window Example 5

# COMMAND ----------

# Consider a case where we need to calculate the cumulative average value of a column within the defined windows.
# For each row, the calculation should cover the rows between the first row of the window and the current row.
# We can do so by defining the starting point with unboundedPreceding.
# Similarly, if we want to go until the end of the window, we can use the unboundedFollowing.
# After a window is defined, We follow the asusual process.
# define a window
window = Window.partitionBy("store_code", "product_code")\
    .orderBy("sales_date")\
    .rowsBetween(Window.unboundedPreceding, -1)

# COMMAND ----------

# calculate mean
df = df.withColumn("cumulative_mean", mean("sales_qty").over(window))

# COMMAND ----------

df.filter((col("store_code") == "A1") & (col("product_code") == "95955"))\
    .select("sales_date", "sales_qty", "cumulative_mean")\
    .display()
