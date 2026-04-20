# ─────────────────────────────────────────────────────────────
# SAS-to-PySpark Accelerator  v1.0
# Source  : PROC_tester4.sas
# Date    : 2026-03-04 13:52:04
# Blocks  : 1 converted  |  ✅ All converted
# ─────────────────────────────────────────────────────────────

from pyspark.sql.functions import col

spark = SparkSession.builder.appName("PROC_tester4").getOrCreate()


df_sql_result = df_sales.join(df_customers, col("a.id") == col("b.id"))
