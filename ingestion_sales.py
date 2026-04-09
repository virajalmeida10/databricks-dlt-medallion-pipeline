import dlt


# sales (east & west) expectations/rules
# sales_id should not be a null:
sales_rules = {
    "rule_1" : "sales_id IS NOT NULL"
}

# Now we will implement this rules in the following "sales_stg" code
# @dlt.expect_or_fail OR @dlt.expect_all_or_drop OR @dlt.expect_all is only possible to be written inside the Streaming tables (@dlt.table) such as products & customers incase of @dlt.append_flow, we have to write it inside the create tble statement as follows:
# expect_all_or_drop=sales_rules


# Creating empty streaming table
dlt.create_streaming_table(
    name = "sales_stg",
    expect_all_or_drop=sales_rules
)

# Creating west sales flow
@dlt.append_flow(target = "sales_stg")
def east_sales():
    df = spark.readStream.table("declarativepipelineordlt.source.sales_east")
    return df # It will send this df to sales_stg table

# Creating east sales flow
@dlt.append_flow(target = "sales_stg")
def west_sales():
    df = spark.readStream.table("declarativepipelineordlt.source.sales_west")
    return df # It will send this df to sales_stg table


