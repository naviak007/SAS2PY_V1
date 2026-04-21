# SAS Block Start: 1 | DATA STEP
sales_enriched = sales_raw.withColumn("total_amount", sales_raw.price * sales_raw.qty) \
    .withColumn("discount_amount", sales_raw.total_amount * 0.1) \

# Line 5
    .withColumn("category", F.when(sales_raw.total_amount > 1000, 'High') \
    .otherwise('Normal')) \
    
# Line 7
    .withColumn("bulk_order", F.when(sales_raw.qty >= 10, 1).otherwise(0)) 

# Line 8
sales_enriched.createOrReplaceTempView("sales_enriched") 

# NOTES
# Assumed table 'sales_raw' exists
# Used DataFrame API for transformations
# Ensure to import necessary functions from pyspark.sql.functions as F