# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType
import pyspark.sql.functions as F
from delta.tables import DeltaTable

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

def read_data_from_silver(table):
    df = spark.readStream \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .table(f'{catalog_name}.silver.{table}')
    return df.filter("_change_type IN ('insert', 'update_postimage')")

# COMMAND ----------

def enhance_order_returns(df):
    df = df.withColumn("date_id", F.date_format(F.col("order_dt"), "yyyyMMdd").cast(IntegerType()))
    df = df.withColumn('return_days', F.datediff(F.col("return_ts"), F.col("order_dt")))
    df = df.withColumn('within_policy', F.when(F.col("return_days") <= 15, 1).otherwise(0))
    df = df.withColumn('is_late_return', F.when(F.col("return_days") > 15, 1).otherwise(0))
    return df

def enhance_order_shipments(df):
    df = df.withColumn('carrier_group',F.when(F.col("carrier").isin(['ECOMEXPRESS', 'DELHIVERY', 'XPRESSBEES','BLUEDART']), 'Domestic').otherwise('International'))
    df = df.withColumn('is_weekend_shipment', F.when(F.dayofweek(F.col("order_dt")).isin([1, 7]), True).otherwise(False))
    return df

# COMMAND ----------


def upsert_order_returns_to_gold(microBatchDF, batchId):
    table_name = f'{catalog_name}.gold.gld_order_returns'
    if not spark.catalog.tableExists(table_name):
        print("creating new table")
        microBatchDF.write.format('delta').mode('overwrite').saveAsTable(table_name)
        # change data feed, capture what happens to each data row. 
        # gold layer later only reads the data feed instead of scanning the whole table again
        spark.sql(
            f'ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)'
        )
    else:
        # in-mem silver table we want to write into
        # we then merge each microbatch dataframe into this table
        deltaTable = DeltaTable.forName(spark, table_name)
        deltaTable.alias('gold_table').merge(
        microBatchDF.alias('batch_table'), 
        'gold_table.order_id = batch_table.order_id '
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

# COMMAND ----------

def upsert_order_shipments_to_gold(microBatchDF, batchId):
    table_name = f'{catalog_name}.gold.gld_order_shipments'
    if not spark.catalog.tableExists(table_name):
        print("creating new table")
        microBatchDF.write.format('delta').mode('overwrite').saveAsTable(table_name)
        # change data feed, capture what happens to each data row. 
        # gold layer later only reads the data feed instead of scanning the whole table again
        spark.sql(
            f'ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)'
        )
    else:
        # in-mem silver table we want to write into
        # we then merge each microbatch dataframe into this table
        deltaTable = DeltaTable.forName(spark, table_name)
        deltaTable.alias('gold_table').merge(
        microBatchDF.alias('batch_table'), 
        'gold_table.shipment_id = batch_table.shipment_id AND gold_table.order_id = batch_table.order_id'
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
            

# COMMAND ----------

gold_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/gold/fact_order_returns/"
df = read_data_from_silver('slv_order_returns')
df = enhance_order_returns(df)
df = df.drop("_change_type", "_commit_version", "_commit_timestamp")


# COMMAND ----------

df.writeStream.trigger(availableNow=True).foreachBatch(
    upsert_order_returns_to_gold
).format("delta").option("checkpointLocation", gold_checkpoint_path).option(
    "overwriteSchema", "true"
).outputMode(
    "update"
).trigger(
    once=True
).start().awaitTermination()

# COMMAND ----------

gold_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/gold/fact_order_shipments/"
df = read_data_from_silver('slv_order_shipments')
df = enhance_order_shipments(df)
df = df.drop("_change_type", "_commit_version", "_commit_timestamp")

# COMMAND ----------

df.writeStream.trigger(availableNow=True).foreachBatch(
    upsert_order_shipments_to_gold
).format("delta").option("checkpointLocation", gold_checkpoint_path).option(
    "overwriteSchema", "true"
).outputMode(
    "update"
).trigger(
    once=True
).start().awaitTermination()