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

def read_bronze_table(table_name):
    return (
        spark.readStream \
        .format('delta') \
        .table(f'{catalog_name}.bronze.brz_{table_name}')
    )

# COMMAND ----------

def clean_order_items(df):
    df = df.dropDuplicates(['order_id', 'item_seq'])


    df = df.withColumn(
        'quantity',
        when(F.col('quantity') == 'Two', 2).otherwise(F.col('quantity').cast('int'))
    )
    df = df.withColumn(
        'unit_price',
        F.regexp_replace(F.col('unit_price'), "[$]", "").cast("double")
    )

    df = df.withColumn(
        "discount_pct",
        F.regexp_replace("discount_pct", "%", "").cast("double")
    )

    # Transformation : coupon code processing (convert to lower)
    df = df.withColumn(
        "coupon_code", F.lower(F.trim(F.col("coupon_code")))
    )

    # Transformation : channel processing 
    df = df.withColumn(
        "channel",
        F.when(F.col("channel") == "web", "Website")
        .when(F.col("channel") == "app", "Mobile")
        .otherwise(F.col("channel")),
    )

    #Transformation : Add processed time 
    df = df.withColumn(
        "processed_time", F.current_timestamp()
    )
    
    return df

# COMMAND ----------

def upsert_to_silver(df):
    pass

# COMMAND ----------

def upsert_wrapper(table_name):
    pass 

# COMMAND ----------

df = read_bronze_table('order_items')
df = clean_order_items(df)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT quantity FROM ecommerce.bronze.brz_order_items

# COMMAND ----------

# DBTITLE 1,Cell 6
# df = df.dropDuplicates(['order_id', 'item_seq'])

# from pyspark.sql.functions import when, col, regexp_replace
# df = df.withColumn(
#     'quantity',
#     when(F.col('quantity') == 'Two', 2).otherwise(F.col('quantity').cast('int'))
# )
# df = df.withColumn(
#     'unit_price',
#     F.regexp_replace(F.col('unit_price'), "[$]", "").cast("double")
# )

# df = df.withColumn(
#     "discount_pct",
#     F.regexp_replace("discount_pct", "%", "").cast("double")
# )

# # Transformation : coupon code processing (convert to lower)
# df = df.withColumn(
#     "coupon_code", F.lower(F.trim(F.col("coupon_code")))
# )

# # Transformation : channel processing 
# df = df.withColumn(
#     "channel",
#     F.when(F.col("channel") == "web", "Website")
#     .when(F.col("channel") == "app", "Mobile")
#     .otherwise(F.col("channel")),
# )

# #Transformation : Add processed time 
# df = df.withColumn(
#     "processed_time", F.current_timestamp()
# )

# COMMAND ----------

silver_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/silver/fact_order_items/"

# COMMAND ----------

def upsert_to_silver(microBatchDF, batchId):
    table_name = f'{catalog_name}.silver.slv_order_items'
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
            'silver_table.order_id = batch_table.order_id AND silver_table.item_seq = batch_table.item_seq'
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
        

    

# COMMAND ----------

df.writeStream.trigger(availableNow=True).foreachBatch(
    upsert_to_silver
).format("delta").option("checkpointLocation", silver_checkpoint_path).option(
    "mergeSchema", "true"
).outputMode(
    "update"
).trigger(
    once=True
).start().awaitTermination()