import dlt

# We will be implementing SCD Type-2
# In order to create Slowly Changing Dimensions we use something called as Auto CDC flow.

# For Auto CDC flow, lets create an empty Streaming table:
dlt.create_streaming_table(
    name = "dim_products"
)

# Note: we will be using products_enr_view and not products_enr table because, products_enr table can be appended or updated and to work with SCD Type-2 we need to use the view which can only be appended (ie. adding new data) and not updating the existing data.

# Auto CDC Flow:
dlt.create_auto_cdc_flow(           # Function to create cdc on the table data
    target = "dim_products",           # Table name
    source = "products_enr_view",      # Source table name
    keys = ["product_id"],            # Keys to match the existing data for update
    sequence_by = "last_updated", # To understand whether the value is new or old based on date of its addition or updation
    ignore_null_updates = False,    # To ignore the null values
    apply_as_deletes = None,
    apply_as_truncates = None,
    column_list = None,
    except_column_list = None,
    stored_as_scd_type = 2,         
    track_history_column_list = None,
    track_history_except_column_list = None
)




