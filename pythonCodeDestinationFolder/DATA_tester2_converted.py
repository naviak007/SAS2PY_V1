# PySpark Code Translation

# SAS Block Start: 1 | DATA STEP
sales_clean = (
    spark.read.table("sales_raw")  # reading source dataset
    .withColumn("total_amount", col("price") * col("qty"))  # calculating total
    .withColumn("category", 
        when(col("total_amount") > 1000, 'High')  # condition for High category
        .otherwise('Normal')  # condition for Normal category
    )
)

# NOTES
# Assumed table 'sales_raw' exists
# Used DataFrame API for transformations