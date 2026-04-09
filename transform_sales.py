# Upsert: Update + Insert the Values

# So in Silver Layer we will have to Upsert the values coming from the source table i.e. insert + update the values.
# This is nothing but the CDC (change data capture)
# Suppose we have (1 fgdh, 2 hsdjh, 3 dsj) which we brought from source to silver and now in the next batch we have (1 fgdh, 4 hsdjh, 5 dsj), now in this case it will update the value for 1 and add the the records for 4&5 in the silver table, so at the end of 2nd run we will have 5 records while we only had 3 records during the 1st run.

# While we write code for this it is important to follow the below steps:
# 1. Target
# 2. Source
# 3. Keys (this is importsnt to match the existing data for update)
# 4. Sequence_by (this is really important. Suppose you have 2 records with same key and different value [1, fds & 1, ryw], and you get another record with same key and different value [1, jhg], then you have to decide which value it should pick to update? It is decided by squence_by column as it knows which value is older and which is new and obviously we have to use the latest value to update the record. So sequence_by simply takes the latest values and updates the data)

# We will start off by creating empty tables for the destnation to get data from bronze to silver.
# You have 2 options:
# 1. Use data from bornze tables directly (i.e. working with the tble in bronze directly).
# 2. Create a view on top of those tables and use the view as the source.

# Working directly wiht the table for this i.e. option 1:

import dlt
from pyspark.sql.functions import *


# Lets create a transformation where I create a new column which will be a combination to (quantity * amount) giving us total_amount
# VIEW IS IMPORTANT TO ADD A TRANSFORMATION ON THE SOURCE DATA AS WE CANNOT APPLY TRANSFORMATION AFTER USING "create_auto_cdc_flow" AS IT IS ONLY TO HANDLE CDC OPERATIONS (INSERT, UPDATE, DELETE) AND CANNOT HANDLE INLINE TRANSFORMATION. SO TRANSFORMATION NEED TO HAPPEN BEFORE USING "create_auto_cdc_flow" AND WRITING TO THE SILVER TABLE "sales_enr"
# Transforming Sales Data View:
@dlt.view(
    name = "sales_enr_view"
)
def sales_stg_trns():
    df = spark.readStream.table("sales_stg")
    df = df.withColumn("total_amount", col("quantity") * col("amount"))
    return df


# Create Destination Silver Table
dlt.create_streaming_table(
    name = "sales_enr"    # Table name
)

dlt.create_auto_cdc_flow(           # Function to create cdc on the table data
    target = "sales_enr",           # Table name
    source = "sales_enr_view",      # Source table name
    keys = ["sales_id"],            # Keys to match the existing data for update
    sequence_by = "sale_timestamp", # To understand whether the value is new or old based on date of its addition or updation
    ignore_null_updates = False,    # To ignore the null values
    apply_as_deletes = None,
    apply_as_truncates = None,
    column_list = None,
    except_column_list = None,
    stored_as_scd_type = 1,         # Define what type of SCD is neede here (this is because upsert tables is also called SCD type 1)
    track_history_column_list = None,
    track_history_except_column_list = None
)

# Now once you dry run you should be able to see another Streaming table sales_enr linked with sales_stg table.
# We could have also created a view on top of the bronze table and used that view as the source for the silver table and you could have seen a vew in between of the 2 tables that you see now.

# But we do not only want to create a streaming table, we also want to create a view beforehand because, we need to capture only the new data and also perform some sort of transformations which we did not do, we simply created upserted table using bronze table sales_stg.
# And to apply some transformations is when you understand the importance of view to apply transformations.
# So lets create a view before actually creating a destination silver table.


# Once we are done with this, we need to create a View again for the Gold layer as we cannot use "sales_enr" table directly as it keeps of updating and we cannot have a table that is updating in gold layer.


# Creating Silver View for Gold Layer?

