# import dlt

# # For applying Transformation:
# from pyspark.sql.functions import *

# # Creating an End to End basic pipeline
# # We will be using a Source, Transforming it and then storing that source as our end table

# # Staging Area:
# # Creating Streaming Table to read Incremental Data only:
# @dlt.table(
#     name = "staging_orders"
# )
# def staging_orders():
#     df = spark.readStream.table("declarativepipelineordlt.source.orders")
#     return df


# # When you run the above, you would notice that all the other Objects also ran again and no matter you did Dry run or Run pipeline, this will run all the files as they are inside the Source Code folder.


# # Now lets keep "staging_orders" as source & create another object (streaming view) on top of it:
# # In this Object lets transform the data that we have read from the source coming into "staging_orders"
# # Creating Transformed Area (the source table will "staging_orders"):
# @dlt.view(
#     name = "transformed_orders"
# )
# def transformed_orders():
#     df = spark.readStream.table("staging_orders")
#     df = df.withColumn("order_status", lower(col("order_status")))
#     return df

# # For the above in output you will see "transformed_orders" being dependent upon "staging_orders"
# # The Status column will be written as lower case instead of upper case.


# # Now lets create Aggregated Area on top of "transformed_orders":
# # Creating Aggregated Area (the source table will "transformed_orders"):
# @dlt.table(
#     name = "aggregated_orders"
# )
# def aggregated_orders():
#     df = spark.readStream.table("transformed_orders")
#     df = df.groupBy("order_status").count()
#     return df

