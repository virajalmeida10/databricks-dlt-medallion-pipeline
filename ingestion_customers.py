import dlt


# Customer expectations/rules
# customer_id should not have a null & customer_name should not have a null:
customers_rules = {
    "rule_1" : "customer_id IS NOT NULL",
    "rule_2" : "customer_name IS NOT NULL"
}

# Now we will implement this rules in the following "customers_stg" code


# Ingesting customers
@dlt.table(
    name = "customers_stg"
)

@dlt.expect_all_or_drop(customers_rules)  #It will drop the record if expectation is not met
# @dlt.expect_or_fail OR @dlt.expect_all_or_drop OR @dlt.expect_all is only possible in Streaming tables (@dlt.table) such as products & customers.

def customers_stg():
    df = spark.readStream.table("declarativepipelineordlt.source.customers")
    return df
    








