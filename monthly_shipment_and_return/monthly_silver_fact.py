# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType
import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql.functions import when, col, regexp_replace

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

# MAGIC %sql
# MAGIC
# MAGIC SELECT min(return_ts) FROM ecommerce.bronze.brz_order_returns

# COMMAND ----------

def read_bronze_table(table_name):
    return (
        spark.readStream \
        .format('delta') \
        .table(f'{catalog_name}.bronze.brz_{table_name}')
    )

# COMMAND ----------

def clean_order_returns(df):
    df = df.dropDuplicates(['order_id', 'order_dt', 'return_ts'])
    df = df.withColumn("order_dt", col("order_dt").cast("date"))
    df = df.withColumn("return_ts", col("return_ts").cast("timestamp"))
    df = df.withColumn(
        "reason", F.upper(F.trim(F.col("reason")))
    )
    df = df.withColumn(
        "processed_time", F.current_timestamp()
    )
    return df

# COMMAND ----------

def clean_shipment_returns(df):
    df = df.withColumn("order_dt", col("order_dt").cast("date"))
    df = df.withColumn("carrier", F.upper(F.trim(F.col("carrier"))))
    df = df.withColumn(
        "processed_time", F.current_timestamp()
    )
    return df



# COMMAND ----------


def upsert_order_returns_to_silver(microBatchDF, batchId):
    table_name = f'{catalog_name}.silver.slv_order_returns'
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
        deltaTable.alias('silver_table').merge(
            microBatchDF.alias('batch_table'), 
            'silver_table.order_id = batch_table.order_id'
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
        

# COMMAND ----------

df = read_bronze_table('order_returns')
df = clean_order_returns(df)

# COMMAND ----------

print(df)

# COMMAND ----------

silver_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/silver/fact_order_returns/"
df.writeStream.trigger(availableNow=True).foreachBatch(
    upsert_order_returns_to_silver
).format("delta").option("checkpointLocation", silver_checkpoint_path).option(
    "mergeSchema", "true"
).outputMode(
    "update"
).trigger(
    once=True
).start().awaitTermination()

# COMMAND ----------

df = read_bronze_table('order_shipments')
df = clean_shipment_returns(df)

# COMMAND ----------

def upsert_order_shipments_to_silver(microBatchDF, batchId):
    table_name = f'{catalog_name}.silver.slv_order_shipments'
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
        deltaTable.alias('silver_table').merge(
            microBatchDF.alias('batch_table'), 
            'silver_table.order_id = batch_table.order_id AND silver_table.shipment_id = batch_table.shipment_id'
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

# COMMAND ----------

silver_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/silver/fact_order_shipments/"
df.writeStream.trigger(availableNow=True).foreachBatch(
    upsert_order_shipments_to_silver
).format("delta").option("checkpointLocation", silver_checkpoint_path).option(
    "mergeSchema", "true"
).outputMode(
    "update"
).trigger(
    once=True
).start().awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM ecommerce.silver.slv_order_shipments LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM ecommerce.silver.slv_order_returns LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT min(return_ts) FROM ecommerce.silver.slv_order_returns

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(order_dt) FROM ecommerce.silver.slv_order_shipments