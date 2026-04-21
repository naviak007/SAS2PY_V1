# PySpark Code Translation

# SAS Block Start: 1 | DATA STEP
sales_logic = (
    spark.read.table("sales_raw")  # Line 2
    .withColumn("total", col("price") * col("qty"))  # Line 4
    .withColumn("priority", when((col("total") > 1000) & (col("qty") >= 5), 1).otherwise(0))  # Line 6
)

# NOTES
# Assumed table 'sales_raw' exists
# Used DataFrame API for transformations