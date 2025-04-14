{
 "cells": [
  {
   "cell_type": "code",
   "source": [
    "# COMMAND ----------\n",
    "%python\n",
    "# Import necessary libraries\n",
    "import logging\n",
    "from pyspark.sql import functions as F\n",
    "from pyspark.sql.types import IntegerType, FloatType, StringType, DateType\n"
   ],
   "metadata": {}
  },
  {
   "cell_type": "code",
   "source": [
    "# COMMAND ----------\n",
    "%python\n",
    "# Configure logging\n",
    "logging.basicConfig(level=logging.INFO)\n",
    "logger = logging.getLogger(__name__)\n"
   ],
   "metadata": {}
  },
  {
   "cell_type": "code",
   "source": [
    "# COMMAND ----------\n",
    "%python\n",
    "# Load data from Unity Catalog tables\n",
    "def load_data():\n",
    "    logger.info(\"Loading data from Unity Catalog tables...\")\n",
    "    orders_central_df = spark.table(\"catalog.db.orders_central\")\n",
    "    orders_west_df = spark.table(\"catalog.db.orders_west\")\n",
    "    orders_east_df = spark.table(\"catalog.db.orders_east\")\n",
    "    orders_south_df = spark.table(\"catalog.db.orders_south_2015\")\n",
    "    quota_df = spark.table(\"catalog.db.quota\")\n",
    "    returns_df = spark.table(\"catalog.db.returns\")\n",
    "    return orders_central_df, orders_west_df, orders_east_df, orders_south_df, quota_df, returns_df\n",
    "\n",
    "orders_central_df, orders_west_df, orders_east_df, orders_south_df, quota_df, returns_df = load_data()\n"
   ],
   "metadata": {}
  },
  {
   "cell_type": "code",
   "source": [
    "# COMMAND ----------\n",
    "%python\n",
    "# Data Cleaning and Standardization\n",
    "def clean_and_standardize_data(orders_central_df):\n",
    "    logger.info(\"Cleaning and standardizing data...\")\n",
    "    orders_central_df = orders_central_df.filter(F.col('Order ID').isNotNull())\n",
    "    orders_central_df = orders_central_df.withColumn(\"Sales\", F.col(\"Sales\").cast(FloatType()))\n",
    "    orders_central_df = orders_central_df.withColumn(\"Order Date\", F.to_date(F.concat_ws(\"-\", F.col(\"Order Year\"), F.col(\"Order Month\"), F.col(\"Order Day\"))))\n",
    "    orders_central_df = orders_central_df.withColumn(\"Ship Date\", F.to_date(F.concat_ws(\"-\", F.col(\"Ship Year\"), F.col(\"Ship Month\"), F.col(\"Ship Day\"))))\n",
    "    orders_central_df = orders_central_df.drop(\"Order Year\", \"Order Month\", \"Order Day\", \"Ship Year\", \"Ship Month\", \"Ship Day\")\n",
    "    orders_central_df = orders_central_df.withColumnRenamed(\"Discounts\", \"Discount\").withColumnRenamed(\"Product\", \"Product Name\")\n",
    "    return orders_central_df\n",
    "\n",
    "orders_central_df = clean_and_standardize_data(orders_central_df)\n"
   ],
   "metadata": {}
  },
  {
   "cell_type": "code",
   "source": [
    "# COMMAND ----------\n",
    "%python\n",
    "# Combine all regional order datasets\n",
    "def combine_regional_orders(orders_central_df, orders_west_df, orders_east_df, orders_south_df):\n",
    "    logger.info(\"Combining regional order datasets...\")\n",
    "    orders_df = orders_central_df.unionByName(orders_west_df).unionByName(orders_east_df).unionByName(orders_south_df)\n",
    "    return orders_df\n",
    "\n",
    "orders_df = combine_regional_orders(orders_central_df, orders_west_df, orders_east_df, orders_south_df)\n"
   ],
   "metadata": {}
  },
  {
   "cell_type": "code",
   "source": [
    "# COMMAND ----------\n",
    "%python\n",
    "# Unpivot quota data\n",
    "def unpivot_quota_data(quota_df):\n",
    "    logger.info(\"Unpivoting quota data...\")\n",
    "    quota_df = quota_df.select(\n",
    "        F.col(\"Region\"),\n",
    "        F.expr(\"stack(4, '2015', `2015`, '2016', `2016`, '2017', `2017`, '2018', `2018`)\").alias(\"Year\", \"Quota\")\n",
    "    )\n",
    "    return quota_df\n",
    "\n",
    "quota_df = unpivot_quota_data(quota_df)\n"
   ],
   "metadata": {}
  },
  {
   "cell_type": "code",
   "source": [
    "# COMMAND ----------\n",
    "%python\n",
    "# Add custom calculations\n",
    "def add_custom_calculations(orders_df, returns_df):\n",
    "    logger.info(\"Adding custom calculations...\")\n",
    "    orders_df = orders_df.withColumn(\"Days to Ship\", F.datediff(F.col(\"Ship Date\"), F.col(\"Order Date\")))\n",
    "    orders_df = orders_df.withColumn(\"Returned?\", F.when(F.col(\"Order ID\").isin(returns_df.select(\"Order ID\").rdd.flatMap(lambda x: x).collect()), \"Yes\").otherwise(\"No\"))\n",
    "    return orders_df\n",
    "\n",
    "orders_df = add_custom_calculations(orders_df, returns_df)\n"
   ],
   "metadata": {}
  },
  {
   "cell_type": "code",
   "source": [
    "# COMMAND ----------\n",
    "%python\n",
    "# Apply business rules\n",
    "def apply_business_rules(orders_df):\n",
    "    logger.info(\"Applying business rules...\")\n",
    "    discount_condition = (F.col('Discount') > 17) & (F.col('Discount') < 18)\n",
    "    orders_df = orders_df.filter(~discount_condition)\n",
    "    return orders_df\n",
    "\n",
    "orders_df = apply_business_rules(orders_df)\n"
   ],
   "metadata": {}
  },
  {
   "cell_type": "code",
   "source": [
    "# COMMAND ----------\n",
    "%python\n",
    "# Aggregate sales metrics\n",
    "def aggregate_sales_metrics(orders_df):\n",
    "    logger.info(\"Aggregating sales metrics by region and year...\")\n",
    "    aggregated_df = orders_df.groupBy(\"Region\", F.year(F.col(\"Order Date\")).alias(\"Year of Sale\")).agg(\n",
    "        F.sum(\"Sales\").alias(\"Total Sales\"),\n",
    "        F.sum(\"Profit\").alias(\"Total Profit\"),\n",
    "        F.sum(\"Quantity\").alias(\"Total Quantity\"),\n",
    "        F.avg(\"Discount\").alias(\"Average Discount\")\n",
    "    )\n",
    "    return aggregated_df\n",
    "\n",
    "aggregated_df = aggregate_sales_metrics(orders_df)\n"
   ],
   "metadata": {}
  },
  {
   "cell_type": "code",
   "source": [
    "# COMMAND ----------\n",
    "%python\n",
    "# Join quota with aggregated sales\n",
    "def join_quota_with_sales(aggregated_df, quota_df):\n",
    "    logger.info(\"Joining quota with aggregated sales...\")\n",
    "    final_df = aggregated_df.join(quota_df, (aggregated_df[\"Region\"] == quota_df[\"Region\"]) & (aggregated_df[\"Year of Sale\"] == quota_df[\"Year\"]), \"inner\")\n",
    "    return final_df\n",
    "\n",
    "final_df = join_quota_with_sales(aggregated_df, quota_df)\n"
   ],
   "metadata": {}
  },
  {
   "cell_type": "code",
   "source": [
    "# COMMAND ----------\n",
    "%python\n",
    "# Write final output to Unity Catalog\n",
    "def write_final_output(final_df):\n",
    "    logger.info(\"Writing final output to Unity Catalog...\")\n",
    "    spark.sql(\"DROP TABLE IF EXISTS catalog.db.annual_regional_performance\")\n",
    "    final_df.write.format(\"delta\").mode(\"overwrite\").saveAsTable(\"catalog.db.annual_regional_performance\")\n",
    "\n",
    "write_final_output(final_df)\n",
    "logger.info(\"ETL process completed successfully.\")\n"
   ],
   "metadata": {}
  }
 ],
 "metadata": {
  "kernelspec": {
   "name": "python3",
   "display_name": "Python 3"
  },
  "language_info": {
   "name": "python",
   "version": "3.8.5"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
