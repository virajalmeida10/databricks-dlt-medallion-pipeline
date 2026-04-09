# import dlt

# # Creating Objects independently:

# # Streaming Table
# # creating streaming table "first_stream_table" by getting the data from source orders
# @dlt.table(
#     name = "first_stream_table"
# )
# def first_stream_table():
#     df = spark.readStream.table("declarativepipelineordlt.source.orders")
#     return df

# # Same query doing both Streaming and Batch processing.


# # Materialized View (result of the view will be stored somewhere)
# # Creating Batch processing table view
# @dlt.table(
#     name = "first_mat_view"
# )
# def first_mat_view():
#     df = spark.read.table("declarativepipelineordlt.source.orders")
#     return df

# # spark.readStream.table is only used for streaming table.


# # Create Batch View:
# @dlt.view(
#     name = "first_batch_view"
# )
# def first_batch_view():
#     df = spark.read.table("declarativepipelineordlt.source.orders")
#     return df


# # Create Streaming View:
# @dlt.view(
#     name = "first_streaming_view"
# )
# def first_streaming_view():
#     df = spark.read.table("declarativepipelineordlt.source.orders")
#     return df

