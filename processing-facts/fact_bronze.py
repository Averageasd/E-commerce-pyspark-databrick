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

# landing location of raw data
adls_path = f'abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/order_items/landing/'

# checkpoint folders for streaming (bronze, silver, gold)
bronze_checkpoint_path = f'abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/bronze/fact_order_items/'




# COMMAND ----------

# use autoloader using cloudFiles
# schemaLocation to look at checkpoint locations. will not need to load all data again. 
# rescue. it will not reject data with different schema, it will accept it and put it in a json column.
# includeExistingFiles: include all files not just latest ones
# availableNow=True run right now and terminate. 
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
    .toTable(f"{catalog_name}.bronze.brz_order_items") \
    .awaitTermination()

# COMMAND ----------

display(spark.sql(f'SELECT max(dt) FROM {catalog_name}.bronze.brz_order_items'))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) FROM cloud_files_state("abfss://ecommerce-raw-data@ngdbrstorage9497.dfs.core.windows.net/checkpoint/bronze/fact_order_items/")