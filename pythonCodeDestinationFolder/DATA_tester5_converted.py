# PySpark Code Translation

# SAS Block Start: 1 | DATA STEP
sales_raw_df = spark.read.table("sales_raw")  # Line 2

# Line 3
sales_category_df = sales_raw_df.withColumn("total", sales_raw_df["price"] * sales_raw_df["qty"])  # Line 4

# Line 5
from pyspark.sql.functions import when  # Line 6
sales_category_df = sales_category_df.withColumn("category", when(sales_category_df["total"] > 1000, 'High').otherwise('Normal'))  # Line 7

# Line 8
sales_category_df.createOrReplaceTempView("sales_category")  # Line 9

# NOTES
# Assumed table 'sales_raw' exists
# Used DataFrame API for transformations
# The output DataFrame 'sales_category_df' can be used for further processing or saving