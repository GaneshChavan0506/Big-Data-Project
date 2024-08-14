import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3 
AmazonS3_node1723543253831 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://flipcartdata/CSV_Data/dim_product.csv"]}, transformation_ctx="AmazonS3_node1723543253831")

# Script generated for node Change Schema
ChangeSchema_node1723543435299 = ApplyMapping.apply(frame=AmazonS3_node1723543253831, mappings=[("product_id", "string", "product_id", "int"), ("product_name", "string", "product_name", "string"), ("unit", "string", "unit", "string"), ("product_type", "string", "product_type", "string"), ("brand_name", "string", "brand_name", "string"), ("manufacturer_name", "string", "manufacturer_name", "string"), ("l0_category", "string", "l0_category", "string"), ("l1_category", "string", "l1_category", "string"), ("l2_category", "string", "l2_category", "string"), ("l0_category_id", "string", "l0_category_id", "int"), ("l1_category_id", "string", "l1_category_id", "int"), ("l2_category_id", "string", "l2_category_id", "int")], transformation_ctx="ChangeSchema_node1723543435299")

# Convert the dynamic frame to a Spark DataFrame
df = ChangeSchema_node1723543435299.toDF()


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
df.write.mode("overwrite").parquet("s3://pirodata/extended-flipkart-dimension/")

job.commit()