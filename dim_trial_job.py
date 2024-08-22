import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#reading dataframe
df = spark.read.csv("s3://flipcartdata/dim_data/", header=True, inferSchema=True)

df=df.drop("_c0")
df = df.withColumn("product_id", df["product_id"].cast(IntegerType()))
df = df.withColumn("l0_category_id", df["l0_category_id"].cast(IntegerType()))
df = df.withColumn("l1_category_id", df["l1_category_id"].cast(IntegerType()))
df = df.withColumn("l2_category_id", df["l2_category_id"].cast(IntegerType()))

df = df.withColumn(
    "brand_name",
    when((col("product_type") == "Combo") & col("brand_name").isNull(), "Flipkart Combo")
    .otherwise(col("brand_name"))
).withColumn(
    "manufacturer_name",
    when((col("product_type") == "Combo") & col("manufacturer_name").isNull(), "Flipkart")
    .otherwise(col("manufacturer_name"))
)


df = df.withColumn(
    "brand_name",
    when((col("brand_name").isNull()) & (col("manufacturer_name").isNull()), "Flipkart")
    .otherwise(col("brand_name"))
).withColumn(
    "manufacturer_name",
    when((col("brand_name").isNull()) & (col("manufacturer_name").isNull()), "Local")
    .otherwise(col("manufacturer_name"))
)

df = df.withColumn(
    "manufacturer_name",
    when((col("manufacturer_name").isNull()) & (col("brand_name").isNotNull()), "Local")
    .otherwise(col("manufacturer_name"))
)

df = df.withColumn(
    "brand_name",
    when((col("manufacturer_name").isNotNull()) & (col("brand_name").isNull()), "Local")
    .otherwise(col("brand_name"))
)

# Coalesce to a single partition to ensure a single output file
df = df.coalesce(1)

# Write the merged DataFrame back to S3
df.write.mode("overwrite").parquet("s3://pirodata/dim_table/")

job.commit()
