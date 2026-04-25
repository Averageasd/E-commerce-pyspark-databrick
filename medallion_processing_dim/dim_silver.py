# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, FloatType
import pyspark.sql.functions as F

catalog_name = 'ecommerce'

# COMMAND ----------

df_bronze = spark.table(f'{catalog_name}.bronze.brz_brands')
df_bronze.show(10)

# COMMAND ----------

df_silver = df_bronze.withColumn('brand_name', F.trim(F.col("brand_name")))
df_silver.show(10)

# COMMAND ----------

df_silver = df_silver.withColumn('brand_code', F.regexp_replace(F.col('brand_name'), r'[^a-zA-Z0-9]', ''))
display(df_silver)

# COMMAND ----------

df_silver.select('category_code').distinct().show()

# COMMAND ----------

word_mapping = {
    'GROCERY': 'GRCY',
    'BOOKS': 'BKS',
    'TOYS': 'TOY'
}
df_silver = df_silver.replace(to_replace=word_mapping, subset=['category_code'])

display(df_silver.select('category_code').distinct())


# COMMAND ----------

df_silver.write.format('delta') \
    .mode('overwrite') \
    .option('mergeSchema', 'true') \
    .saveAsTable(f'{catalog_name}.silver.slv_brands')

# COMMAND ----------

# MAGIC %md
# MAGIC ### DIMENSION: CATEGORY ###

# COMMAND ----------

df_bronze = spark.table(f'{catalog_name}.bronze.brz_category')

# COMMAND ----------

from pyspark.sql.functions import col

df_bronze.groupBy('category_code').count().filter(F.col("count") > 1).show()


# COMMAND ----------

df_silver = df_bronze.dropDuplicates(['category_code'])
display(df_silver)

# COMMAND ----------

df_silver = df_silver.withColumn('category_code', F.upper(F.col('category_code')))
display(df_silver)

# COMMAND ----------

df_silver.write.format('delta') \
    .mode('overwrite') \
    .option('mergeSchema', 'true') \
    .saveAsTable(f'{catalog_name}.silver.slv_category')

# COMMAND ----------

df_bronze = spark.table(f'{catalog_name}.bronze.brz_calendar')
display(df_bronze)

# COMMAND ----------

duplicates = df_bronze.groupBy(df_bronze.columns).count().filter("count > 1")
display(duplicates)

# COMMAND ----------

df_silver = df_bronze.dropDuplicates(['date'])

# COMMAND ----------

df_silver = df_silver.withColumn("day_name", F.initcap(F.col("day_name")))
display(df_silver)

# COMMAND ----------

df_silver = df_silver.withColumn("week_of_year", 
    F.when(F.col("week_of_year") < 0, F.col("week_of_year") * -1)
     .otherwise(F.col("week_of_year"))
)

# COMMAND ----------

df_silver = df_silver.withColumn("quarter", F.concat_ws("", F.concat(F.lit("Q"), F.col("quarter"), F.lit("-"), F.col("year"))))

df_silver = df_silver.withColumn("week_of_year", F.concat_ws("-", F.concat(F.lit("Week"), F.col("week_of_year"), F.lit("-"), F.col("year"))))


# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver.write.format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(f'{catalog_name}.silver.slv_calendar')

# COMMAND ----------

df_bronze = spark.table(f'{catalog_name}.bronze.brz_products')
display(df_bronze)

# COMMAND ----------

df_bronze.select(F.col("brand_code"), F.col("category_code")).distinct().show()

# COMMAND ----------

df_bronze.groupBy(df_bronze.columns).count().filter(F.col("count") > 1).show()

# COMMAND ----------

df_bronze.select('category_code').distinct().show()

# COMMAND ----------

df_bronze.filter(F.col('category_code') == 'hnk').select('brand_code').distinct().show()

# COMMAND ----------

df_bronze.filter(F.col('category_code') == 'ce').select('brand_code').distinct().show()

# COMMAND ----------

df_bronze.filter(F.col('category_code') == 'app').select('brand_code').distinct().show()

# COMMAND ----------

df_bronze.filter(F.col('category_code') == 'grcy').select('brand_code').distinct().show()

# COMMAND ----------

df_bronze.filter(F.col('category_code') == 'bpc').select('brand_code').distinct().show()

# COMMAND ----------

df_bronze.filter(F.col('category_code') == 'spt').select('brand_code').distinct().show()

# COMMAND ----------

df_bronze.filter(F.col('category_code') == 'toy').select('brand_code').distinct().show()

# COMMAND ----------

df_bronze.filter(F.col('category_code') == 'bks').select('brand_code').distinct().show()

# COMMAND ----------

word_mapping = {
    'stcr': 'SteelCraft',
    'hmns': 'HomeNest',
    'aqpr': 'AquaPure',
    'csnv': 'CasaNova',
    'prlv': 'PureLiving',
    'ckmt': 'CookMate',
    'wrmh': 'WarmHearth',
    'acme': 'AcmeTech',
    'phtx': 'Photonix',
    'ecot': 'EcoTone',
    'bytm': 'ByteMax',
    'novw': 'NovaWave',
    'urtl': 'UrbanTrail',
    'ggrn': 'GoodGrain',
    'slke': 'SilkEssence',
    'volt': 'VoltEdge',
    'cblt': 'CobaltWear',
    'arft': 'AeroFit',
    'mosa': 'Mosaic',
    'rkbr': 'RocketBear',
    'gmfg': 'GameForge',
    'grbk': 'GreenBasket',
    'dmlx': 'DermaLux',
    'znth': 'Zenith',
    'btnq': 'Botaniq',
    'skyl': 'SkyLink',
    'ablf': 'AmberLeaf',
    'hrbc': 'HerbaCare',
    'plcb': 'PlayCube',
    'ftfx': 'FitFlex',
    'cotc': 'CottonClub',
    'nthr': 'NorthThread',
    'dlhv': 'DailyHarvest',
    'frfl': 'FreshFields',
    'frmj': 'FarmJoy',
    'ntch': 'NutriChoice',
    'glow': 'GlowOn',
    'alms' : 'AloeMist',
    'trbz': 'TrailBlazer',
    'prsd': 'ProStride',
    'pkpr': 'PeakPro',
    'swcr': 'SwiftCore',
    'smln': 'SummitLine',
    'kdln': 'KiddoLand',
    'tngr': 'TinyGears',
    'hpfn': 'HappyFun',
    'pgtr': 'PageTurner',
    'qlhs': 'QuillHouse',
    'stac': 'StoryArc',
    'lfsp': 'LeafSpine',
    'blin': 'BlueInk',
    'rdmr': 'ReadMore'
}

df_silver = df_bronze.replace(to_replace=word_mapping, subset=['brand_code'])
df_silver = df_silver.withColumn('category_code', F.upper(F.col('category_code')))

# COMMAND ----------

df_silver.select('brand_code').distinct().show()

# COMMAND ----------

df_silver = df_silver.withColumn("weight_grams", F.substring(F.col("weight_grams"), 1, F.length(col("weight_grams")) - 1))
display(df_silver)

# COMMAND ----------

df_silver = df_silver.withColumn('weight_grams', col('weight_grams').cast('int'))
df_silver.select('weight_grams').show(5)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col, expr
df_silver = df_silver.withColumn('length_cm', regexp_replace(F.col('length_cm'), ',', '.').cast('float'))
# df_silver.select('length_cm').show(5)

# COMMAND ----------

df_silver.select(F.col('length_cm')).show(5)

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

df_silver = df_silver.withColumn('width_cm', F.col('width_cm').cast('decimal(10,1)'))
df_silver = df_silver.withColumn('height_cm', F.col('height_cm').cast('decimal(10,1)'))
df_silver.select(F.col('width_cm'), F.col('height_cm')).display(5)

# COMMAND ----------

df_silver = df_silver.withColumn("rating_count", 
    F.when(F.col('rating_count') < 0, F.col("rating_count") * -1)
     .otherwise(F.col("rating_count")))

# COMMAND ----------

df_silver.write.format('delta') \
    .mode('overwrite') \
    .option('mergeSchema', 'true') \
    .saveAsTable(f'{catalog_name}.silver.slv_products')

# COMMAND ----------

df_bronze = spark.table(f'{catalog_name}.bronze.brz_customers')
display(df_bronze)

# COMMAND ----------

from pyspark.sql.functions import split, col
df_silver = df_bronze.withColumn("phone", F.split(col("phone"), "\\.").getItem(0))

df_silver.printSchema()

# COMMAND ----------

df_silver.select('phone').display(5)

# COMMAND ----------

df_silver.groupBy(df_silver.columns).count().filter(F.col("count") > 1).show()

# COMMAND ----------

df_silver.filter(F.col('customer_id').isNull()).show()


# COMMAND ----------

df_silver = df_silver.dropna(subset=["customer_id"])

# COMMAND ----------

df_silver = df_silver.fillna("Not Available", subset=["phone"])

# COMMAND ----------

df_silver.groupBy(df_silver.columns).count().filter(F.col("count") > 1).show()

# COMMAND ----------

df_silver.filter(F.col('customer_id').isNull()).show()


# COMMAND ----------

df_silver.write.format('delta') \
    .mode('overwrite') \
    .option('mergeSchema', 'true') \
    .saveAsTable(f'{catalog_name}.silver.slv_customers')