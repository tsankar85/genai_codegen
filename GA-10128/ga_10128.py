# Databricks notebook source
# COMMAND ----------

# MAGIC %md
# MAGIC # Import Libraries

# COMMAND ----------

# MAGIC %python
import logging
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType, StringType, DateType
import psycopg2
import mysql.connector
import pyodbc

# COMMAND ----------

# MAGIC %md
# MAGIC # Configure Logging

# COMMAND ----------

# MAGIC %python
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC # Retrieve Credentials

# COMMAND ----------

# MAGIC %python
# Securely retrieve credentials using Databricks utilities
postgres_user = dbutils.secrets.get(scope="my_scope", key="postgres_user")
postgres_password = dbutils.secrets.get(scope="my_scope", key="postgres_password")
mysql_user = dbutils.secrets.get(scope="my_scope", key="mysql_user")
mysql_password = dbutils.secrets.get(scope="my_scope", key="mysql_password")
sqlserver_user = dbutils.secrets.get(scope="my_scope", key="sqlserver_user")
sqlserver_password = dbutils.secrets.get(scope="my_scope", key="sqlserver_password")
snowflake_user = dbutils.secrets.get(scope="my_scope", key="snowflake_user")
snowflake_password = dbutils.secrets.get(scope="my_scope", key="snowflake_password")

# COMMAND ----------

# MAGIC %md
# MAGIC # Load Data from PostgreSQL

# COMMAND ----------

# MAGIC %python
def load_postgres_data():
    try:
        conn_postgres = psycopg2.connect(
            dbname="PolicyDB",
            user=postgres_user,
            password=postgres_password,
            host="postgres_server"
        )
        policy_df = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres_server/PolicyDB") \
            .option("dbtable", "policy_table") \
            .option("user", postgres_user) \
            .option("password", postgres_password) \
            .load()
        logger.info("Loaded policy data from PostgreSQL")
        return policy_df
    except Exception as e:
        logger.error(f"Error loading policy data: {e}")
        return None

policy_df = load_postgres_data()

# COMMAND ----------

# MAGIC %md
# MAGIC # Load Data from MySQL

# COMMAND ----------

# MAGIC %python
def load_mysql_data():
    try:
        conn_mysql = mysql.connector.connect(
            user=mysql_user,
            password=mysql_password,
            host="mysql_server",
            database="ClaimsDB"
        )
        claims_df = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:mysql://mysql_server/ClaimsDB") \
            .option("dbtable", "claims_table") \
            .option("user", mysql_user) \
            .option("password", mysql_password) \
            .load()
        logger.info("Loaded claims data from MySQL")
        return claims_df
    except Exception as e:
        logger.error(f"Error loading claims data: {e}")
        return None

claims_df = load_mysql_data()

# COMMAND ----------

# MAGIC %md
# MAGIC # Load Data from SQL Server

# COMMAND ----------

# MAGIC %python
def load_sqlserver_data():
    try:
        conn_sqlserver = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};SERVER=sqlserver_server;DATABASE=DemographicsDB;UID=' + sqlserver_user + ';PWD=' + sqlserver_password
        )
        demographics_df = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:sqlserver://sqlserver_server/DemographicsDB") \
            .option("dbtable", "customer_demographics_table") \
            .option("user", sqlserver_user) \
            .option("password", sqlserver_password) \
            .load()
        logger.info("Loaded customer demographics data from SQL Server")
        return demographics_df
    except Exception as e:
        logger.error(f"Error loading customer demographics data: {e}")
        return None

demographics_df = load_sqlserver_data()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Integration

# COMMAND ----------

# MAGIC %python
def integrate_data(policy_df, claims_df, demographics_df):
    try:
        # Ensure uniqueness of join keys
        assert policy_df.select("Customer_ID").distinct().count() == policy_df.count(), "Customer_ID is not unique in policy_df"
        assert claims_df.select("Policy_ID").distinct().count() == claims_df.count(), "Policy_ID is not unique in claims_df"

        # Join policy data with customer demographics
        policy_demo_df = policy_df.join(demographics_df, "Customer_ID", "inner")

        # Join the result with claims data
        integrated_df = policy_demo_df.join(claims_df, "Policy_ID", "inner")
        logger.info("Data integration completed")
        return integrated_df
    except Exception as e:
        logger.error(f"Error during data integration: {e}")
        return None

integrated_df = integrate_data(policy_df, claims_df, demographics_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformation Logic Implementation

# COMMAND ----------

# MAGIC %python
def apply_transformations(integrated_df):
    try:
        # Calculate Age
        integrated_df = integrated_df.withColumn("Age", F.datediff(F.current_date(), F.col("Date_of_Birth")) / 365)

        # Calculate Claim_To_Premium_Ratio
        integrated_df = integrated_df.withColumn("Claim_To_Premium_Ratio", F.when(F.col("Total_Premium_Paid") != 0, F.col("Claim_Amount") / F.col("Total_Premium_Paid")).otherwise(0))

        # Calculate Claims_Per_Policy
        integrated_df = integrated_df.withColumn("Claims_Per_Policy", F.when(F.col("Policy_Count") != 0, F.col("Total_Claims") / F.col("Policy_Count")).otherwise(0))

        logger.info("Transformation logic implemented")
        return integrated_df
    except Exception as e:
        logger.error(f"Error during transformation logic implementation: {e}")
        return None

integrated_df = apply_transformations(integrated_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Aggregation and Filtering

# COMMAND ----------

# MAGIC %python
def aggregate_data(integrated_df):
    try:
        # Aggregate data at customer level
        customer_agg_df = integrated_df.groupBy("Customer_ID").agg(
            F.count("Claim_ID").alias("Total_Claims"),
            F.avg("Claim_Amount").alias("Average_Claim_Amount"),
            F.max("Claim_Date").alias("Recent_Claim_Date"),
            F.countDistinct("Policy_ID").alias("Policy_Count")
        )
        logger.info("Aggregation and filtering completed")
        return customer_agg_df
    except Exception as e:
        logger.error(f"Error during aggregation and filtering: {e}")
        return None

customer_agg_df = aggregate_data(integrated_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Advanced Analytics and Scoring

# COMMAND ----------

# MAGIC %python
def apply_advanced_analytics(integrated_df):
    try:
        # Placeholder for API call to retrieve fraud score
        def get_fraud_score(customer_id):
            # Implement API call logic here
            return 0.1

        # Apply fraud score calculation
        integrated_df = integrated_df.withColumn("Fraud_Score", F.lit(0.1))  # Replace with actual logic
        logger.info("Advanced analytics and scoring completed")
        return integrated_df
    except Exception as e:
        logger.error(f"Error during advanced analytics and scoring: {e}")
        return None

integrated_df = apply_advanced_analytics(integrated_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Output to Snowflake

# COMMAND ----------

# MAGIC %python
def write_to_snowflake(integrated_df):
    try:
        # Define schema explicitly using select
        final_df = integrated_df.select(
            "Customer_ID", "Age", "Claim_To_Premium_Ratio", "Claims_Per_Policy", "Fraud_Score"
        )

        # Write the final DataFrame to Snowflake
        final_df.write \
            .format("snowflake") \
            .option("sfURL", "account.snowflakecomputing.com") \
            .option("sfDatabase", "CustomerDW") \
            .option("sfSchema", "Public") \
            .option("sfWarehouse", "COMPUTE_WH") \
            .option("sfRole", "SYSADMIN") \
            .option("sfUser", snowflake_user) \
            .option("sfPassword", snowflake_password) \
            .option("dbtable", "Customer360") \
            .save()
        logger.info("Data written to Snowflake")
    except Exception as e:
        logger.error(f"Error writing data to Snowflake: {e}")

write_to_snowflake(integrated_df)
