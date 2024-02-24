# Article Source : https://towardsdatascience.com/5-examples-to-master-pyspark-window-operations-26583066e227

## What is a Window in Data Analysis : 
- Window is a set of rows that are related in some ways.
- This relation can be of belonging to the same group or being in the n consecutive days.
- Once we generate the window with the required constraints, we can do calculations or aggregations over it.
- In this article, 5 detailed examples are explored to create windows with partitions, customize these windows and how to do calculations over them.
- PySpark is a Python API for Spark, which is an analytics engine used for the large-scale data processing

Data : Data is actually prepared from a datasets repository. We have to create a sparksession and create a dataframe from this dataset : https://github.com/SonerYldrm/datasets/blob/main/sample_sales_pyspark.csv

## Example 1 :
- We first create a window by partitioning and ordering columns. 
- In our DataFrame, we have store, product and sales information including quantity, price, revenue and date.
- If we want to calculate cumulative sales of each product in each store seperately, we define our window as follows :