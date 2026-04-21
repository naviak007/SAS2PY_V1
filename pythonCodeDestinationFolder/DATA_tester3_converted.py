# SAS Block Start: 1 | DATA STEP
sales_calc = sales_raw.withColumn("total", col("price") * col("qty")) \
                      .withColumn("avg_price", col("total") / col("qty"))

# NOTES
# Assumed table 'sales_raw' exists
# No missing inputs or unsupported features detected