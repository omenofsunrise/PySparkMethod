from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def get_product_category_pairs(products_df, categories_df):
    joined_df = products_df.join(categories_df, products_df.product_id == categories_df.product_id, "left_outer")

    products_without_categories = joined_df.filter(categories_df.product_id.isNull())

    result_df = joined_df.select(products_df.product_name.alias("product"), categories_df.category_name.alias("category")).union(products_without_categories.select(products_df.product_name, col("category").isNull().alias("category")))

    return result_df
