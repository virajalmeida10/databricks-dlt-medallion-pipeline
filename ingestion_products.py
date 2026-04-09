import dlt


# Products expectations/rules
# product_id should not have a null & price should not be less then 0:
products_rules = {
    "rule_1" : "product_id IS NOT NULL",
    "rule_2" : "price >= 0"
}

# Now we will implement this rules in the following "products_stg" code


# Ingesting products
@dlt.table(
    name = "products_stg"
)

@dlt.expect_all_or_drop(products_rules)  #It will drop the record if expectation is not met
# @dlt.expect_or_fail OR @dlt.expect_all_or_drop OR @dlt.expect_all is only possible in Streaming tables (@dlt.table) such as products & customers.

def products_stg():
    df = spark.readStream.table("declarativepipelineordlt.source.products")
    return df