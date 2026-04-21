# SAS Block Start: 1 | PROC SQL
# Translated to PySpark DataFrame API
sales_df = spark.read.table("sales_raw")  # Line 1
filtered_sales_df = sales_df.filter(sales_df.price > 100).select("price", "qty")  # Line 2

# Output the resulting DataFrame
filtered_sales_df.show()  # Line 3

# NOTES
# Assumed table 'sales_raw' exists
# No unsupported features detected in the translation