# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # ETL Process for Sales Data
# MAGIC This notebook performs an ETL process on sales data from various regions, standardizes the data, and writes the results back to Unity Catalog tables.

# COMMAND ----------
# MAGIC %python
# Import necessary libraries
import logging
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType

# COMMAND ----------
# MAGIC %python
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------
# MAGIC %python
try:
    # Load data from Unity Catalog tables
    orders_central_df = spark.table("catalog.sales_db.orders_central")
    orders_west_df = spark.table("catalog.sales_db.orders_west")
    orders_east_df = spark.table("catalog.sales_db.orders_east")
    orders_south_df = spark.table("catalog.sales_db.orders_south_2015")
    quota_df = spark.table("catalog.sales_db.quota")
    returns_df = spark.table("catalog.sales_db.returns")

    # COMMAND ----------
    # MAGIC %md
    # MAGIC ## Data Standardization

    # COMMAND ----------
    # MAGIC %python
    logger.info("Starting data standardization...")

    # Fix Dates for Orders Central
    orders_central_df = orders_central_df.withColumn("Region", F.lit("Central")) \
        .withColumn("Order Date", F.to_date(F.concat_ws("-", orders_central_df["Order Year"], orders_central_df["Order Month"], orders_central_df["Order Day"]))) \
        .withColumn("Ship Date", F.to_date(F.concat_ws("-", orders_central_df["Ship Year"], orders_central_df["Ship Month"], orders_central_df["Ship Day"]))) \
        .drop("Order Year", "Order Month", "Order Day", "Ship Year", "Ship Month", "Ship Day") \
        .withColumnRenamed("Discounts", "Discount") \
        .withColumnRenamed("Product", "Product Name")

    # Remove Nulls
    orders_central_df = orders_central_df.filter(F.col("Order ID").isNotNull())

    # Fix Data Type
    orders_central_df = orders_central_df.withColumn("Discount", F.col("Discount").cast(StringType())) \
        .withColumn("Sales", F.regexp_replace(F.col("Sales"), "[^0-9.]", "").cast(DoubleType()))

    # COMMAND ----------
    # MAGIC %md
    # MAGIC ## Data Transformation

    # COMMAND ----------
    # MAGIC %python
    # Rename States
    state_mapping = {
        "CA": "California", "TX": "Texas", "NY": "New York", "FL": "Florida",
        "IL": "Illinois", "PA": "Pennsylvania", "OH": "Ohio", "GA": "Georgia",
        "NC": "North Carolina"
    }
    orders_central_df = orders_central_df.replace(state_mapping, subset=["State"])

    # Pivot Quotas
    quota_unpivoted_df = quota_df.selectExpr("Region", "stack(4, '2015', 2015, '2016', 2016, '2017', 2017, '2018', 2018) as (Year, Quota)")
    quota_unpivoted_df = quota_unpivoted_df.withColumn("Year", F.col("Year").cast(IntegerType()))

    # Union Operations
    all_orders_df = orders_central_df.union(orders_west_df).union(orders_east_df).union(orders_south_df)

    # Join Operations
    orders_returns_df = returns_df.join(all_orders_df, ["Order ID", "Product ID"], "left")

    # COMMAND ----------
    # MAGIC %md
    # MAGIC ## Custom Calculations

    # COMMAND ----------
    # MAGIC %python
    orders_returns_df = orders_returns_df.withColumn("Returned?", F.when(F.col("Return Reason").isNotNull(), "Yes").otherwise("No")) \
        .withColumn("Days to Ship", F.datediff(F.col("Ship Date"), F.col("Order Date"))) \
        .withColumn("Discount", F.coalesce(F.col("Discount"), F.lit(0)).cast(DoubleType())) \
        .withColumn("Year of Sale", F.year(F.col("Order Date")))

    # Define discount filter for readability
    discount_filter = ~(F.col("Discount") >= 17) & ~(F.col("Discount") <= 18)
    orders_returns_df = orders_returns_df.filter(discount_filter)

    # Clean Notes/Approver
    orders_returns_df = orders_returns_df.withColumn("Return Notes", F.split(F.col("Notes"), "-").getItem(0)) \
        .withColumn("Approver", F.split(F.col("Notes"), "-").getItem(1)) \
        .drop("Notes")

    # Roll Up Sales
    annual_performance_df = orders_returns_df.groupBy("Region", "Year of Sale").agg(
        F.sum("Profit").alias("Total Profit"),
        F.sum("Sales").alias("Total Sales"),
        F.sum("Quantity").alias("Total Quantity"),
        F.avg("Discount").alias("Average Discount")
    )

    # COMMAND ----------
    # MAGIC %md
    # MAGIC ## Quota and Orders Join

    # COMMAND ----------
    # MAGIC %python
    quota_orders_df = annual_performance_df.join(quota_unpivoted_df, ["Region", "Year"], "inner")

    # COMMAND ----------
    # MAGIC %md
    # MAGIC ## Output Generation

    # COMMAND ----------
    # MAGIC %python
    logger.info("Writing output data to Unity Catalog tables...")

    # Drop existing tables if they exist
    spark.sql("DROP TABLE IF EXISTS catalog.sales_db.annual_regional_performance")
    spark.sql("DROP TABLE IF EXISTS catalog.sales_db.superstore_sales")

    # Write to Unity Catalog target tables
    annual_performance_df.write.format("delta").mode("overwrite").saveAsTable("catalog.sales_db.annual_regional_performance")
    orders_returns_df.select("Order ID", "Product ID", "Return Reason", "Return Notes", "Approver").write.format("delta").mode("overwrite").saveAsTable("catalog.sales_db.superstore_sales")

    logger.info("ETL process completed successfully.")

except Exception as e:
    logger.error(f"An error occurred during the ETL process: {e}")
    raise
