# Copying everything from transform_sales and make some changes:

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Converting price which is currently in decimal format to an integer format in product table:
# Transforming Products Data View:
@dlt.view(
    name = "products_enr_view"
)
def sales_stg_trns():
    df = spark.readStream.table("products_stg")
    df = df.withColumn("price", col("price").cast(IntegerType()))
    return df


# Create Destination Silver Table
dlt.create_streaming_table(
    name = "products_enr"    # Table name
)

dlt.create_auto_cdc_flow(           # Function to create cdc on the table data
    target = "products_enr",           # Table name
    source = "products_enr_view",      # Source table name
    keys = ["product_id"],            # Keys to match the existing data for update
    sequence_by = "last_updated", # To understand whether the value is new or old based on date of its addition or updation
    ignore_null_updates = False,    # To ignore the null values
    apply_as_deletes = None,
    apply_as_truncates = None,
    column_list = None,
    except_column_list = None,
    stored_as_scd_type = 1,         # Define what type of SCD is neede here (this is because upsert tables is also called SCD type 1)
    track_history_column_list = None,
    track_history_except_column_list = None
)