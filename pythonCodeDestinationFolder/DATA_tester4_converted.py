# SAS Block Start: 1 | DATA STEP
sales_flags = sales_raw

# PySpark equivalent
sales_flags = sales_raw.withColumn("expensive", F.when(sales_raw.price > 1000, 1).otherwise(0)) \
                        .withColumn("bulk", F.when(sales_raw.qty >= 10, 1).otherwise(0))

# NOTE: Assuming sales_raw is already a DataFrame loaded in the environment.

# The RUN statement is implicit in PySpark as transformations are lazy.

NOTES
Assumed DataFrame 'sales_raw' exists and is properly defined.