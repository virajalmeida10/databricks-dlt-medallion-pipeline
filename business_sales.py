import dlt
from pyspark.sql.functions import *

# Creating a Materialized Business View
# If you choose Streaming View, you wont be able to find correct because it will only process the incremental data
# But to find the business view, you need all the data sitting in fact & dimension table and not just the incremental (changes that come in) data.
@dlt.table(
    name = "business_sales"
)
def business_sales():
    # Taking all the dimesions & fact tables from gold:
    df_fact = spark.read.table("fact_sales")
    df_dimCust = spark.read.table("dim_customers")
    df_dimProd = spark.read.table("dim_products")

    # applying join on those tables:
    df_join = df_fact.join(df_dimCust, df_fact.customer_id == df_dimCust.customer_id, "inner").join(df_dimProd, df_fact.product_id == df_dimProd.product_id, "inner")
    
    # selecting required columns:
    df_prun = df_join.select("region", "category", "total_amount")
    
    # performing aggregations
    df_agg = df_prun.groupBy("region", "category").agg(sum("total_amount").alias("total_sales"))

    return df_agg

# Now once I click on Run Pipeline, all the 3 folders (bronze, silver, gold) will be running including tutorial (commented out currently).