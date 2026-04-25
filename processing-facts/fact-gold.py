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

df = spark.readStream \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .table(f'{catalog_name}.silver.slv_order_items')

# COMMAND ----------

df_union = df.filter("_change_type IN ('insert', 'update_postimage')")

# COMMAND ----------

df_union = df_union.withColumn(
    'gross_amount',
    F.col('quantity') * F.col('unit_price')
)

df_union = df_union.withColumn(
    'discount_amount',
    F.ceil(F.col('gross_amount')) * (F.col('discount_pct') / 100)
)

df_union = df_union.withColumn(
    'sale_amount',
    F.col('gross_amount') - F.col('discount_amount') + F.col('tax_amount')
)

# add date_id since we will join with other tables for 
# BI work
df_union = df_union.withColumn("date_id", F.date_format(F.col("dt"), "yyyyMMdd").cast(IntegerType()))

#Coupon flag
#  coupon flag = 1 if coupon_code is not null else 0
df_union = df_union.withColumn(
    "coupon_flag",
    F.when(F.col("coupon_code").isNotNull(), F.lit(1))
     .otherwise(F.lit(0))
)

# COMMAND ----------

orders_gold_df = df_union.select(
    F.col("date_id"),
    F.col("dt").alias("transaction_date"),
    F.col("order_ts").alias("transaction_ts"),
    F.col("order_id").alias("transaction_id"),
    F.col("customer_id"),
    F.col("item_seq").alias("seq_no"),
    F.col("product_id"),
    F.col("channel"),
    F.col("coupon_code"),
    F.col("coupon_flag"),
    F.col("unit_price_currency"),
    F.col("quantity"),
    F.col("unit_price"),
    F.col("gross_amount"),
    F.col("discount_pct").alias("discount_percent"),
    F.col("discount_amount"),
    F.col("tax_amount"),
    F.col("sale_amount").alias("net_amount")
)

# COMMAND ----------

gold_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/gold/fact_order_items/"
print(gold_checkpoint_path)

def upsert_to_gold(microBatchDF, batchId):
    table_name = f"{catalog_name}.gold.gld_fact_order_items"
    if not spark.catalog.tableExists(table_name):
        print("creating new table")
        microBatchDF.write.format("delta").mode("overwrite").saveAsTable(table_name)
        spark.sql(
            f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
        )
    else:
        deltaTable = DeltaTable.forName(spark, table_name)
        deltaTable.alias("gold_table").merge(
            microBatchDF.alias("batch_table"),
            "gold_table.transaction_id = batch_table.transaction_id AND gold_table.seq_no = batch_table.seq_no",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

orders_gold_df.writeStream.trigger(availableNow=True).foreachBatch(
    upsert_to_gold
).format("delta").option("checkpointLocation", gold_checkpoint_path).option(
    "mergeSchema", "true"
).outputMode(
    "update"
).trigger(
    once=True
).start().awaitTermination()

# COMMAND ----------

spark.sql(f"SELECT count(*) FROM {catalog_name}.gold.gld_fact_order_items").show()

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {catalog_name}.gold.gld_fact_order_items LIMIT 10"))

# COMMAND ----------

