# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType
import pyspark.sql.functions as F

# COMMAND ----------

dbutils.widgets.text('catalog_name', 'ecommerce', 'Catalog Name')
dbutils.widgets.text('storage_account_name', 'ngdbrstorage9497', 'Storage Account Name')
dbutils.widgets.text('container_name', 'ecommerce-raw-data', 'Container Name')

# COMMAND ----------

catalog_name = dbutils.widgets.get('catalog_name')
storage_account_name = dbutils.widgets.get('storage_account_name')
container_name = dbutils.widgets.get('container_name')
print(catalog_name, storage_account_name, container_name)

# COMMAND ----------

dbutils.widgets.get('storage_account_name')

# COMMAND ----------

def read_data_from_landing_to_bronze(bronze_table_name):
    print(catalog_name, storage_account_name, container_name)
    adls_path = f'abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{bronze_table_name}/landing/'
    bronze_checkpoint_path = f'abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/bronze/fact_{bronze_table_name}/'
    print(adls_path, bronze_checkpoint_path)
    spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv")  \
    .option("cloudFiles.schemaLocation", bronze_checkpoint_path) \
    .option("cloudFiles.schemaEvolutionMode", "rescue") \
    .option("header", "true") \
    .option("cloudFiles.inferColumnTypes", "true") \
    .option("rescuedDataColumn", "_rescued_data") \
    .option("cloudFiles.includeExistingFiles", "true")  \
    .option("pathGlobFilter", "*.csv") \
    .load(adls_path) \
    .withColumn("ingest_timestamp", F.current_timestamp()) \
    .withColumn("source_file", F.col("_metadata.file_path")) \
    .writeStream \
    .outputMode("append") \
    .option("checkpointLocation", bronze_checkpoint_path) \
    .trigger(availableNow=True) \
    .toTable(f"{catalog_name}.bronze.brz_{bronze_table_name}") \
    .awaitTermination()

# COMMAND ----------

read_data_from_landing_to_bronze('order_returns')
read_data_from_landing_to_bronze('order_shipments')

# COMMAND ----------

display(spark.sql(f'SELECT max(order_dt) FROM {catalog_name}.bronze.brz_order_returns'))

# COMMAND ----------

display(spark.sql(f'SELECT max(order_dt) FROM {catalog_name}.bronze.brz_order_shipments'))