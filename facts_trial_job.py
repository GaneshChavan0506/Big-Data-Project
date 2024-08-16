import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, date_format, weekofyear, col
from pyspark.sql import functions as F
from pyspark.sql import Window

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1723532264557 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://flipcartdata/CSV_Data/"], "recurse": True}, transformation_ctx="AmazonS3_node1723532264557")

# Script generated for node Change Schema
ChangeSchema_node1723532407097 = ApplyMapping.apply(frame=AmazonS3_node1723532264557, mappings=[("date_", "string", "date_", "date"), ("city_name", "string", "city_name", "string"), ("order_id", "string", "order_id", "int"), ("cart_id", "string", "cart_id", "int"), ("dim_customer_key", "string", "dim_customer_key", "int"), ("procured_quantity", "string", "procured_quantity", "int"), ("unit_selling_price", "string", "unit_selling_price", "double"), ("total_discount_amount", "string", "total_discount_amount", "double"), ("product_id", "string", "product_id", "int"), ("total_weighted_landing_price", "string", "total_weighted_landing_price", "double")], transformation_ctx="ChangeSchema_node1723532407097")

# Convert the dynamic frame to a Spark DataFrame
df = ChangeSchema_node1723532407097.toDF()

#removing rows from procured quantity which contains 0
df = df.filter(df.product_id != 0)

# Calculate the total quantity sold per product
df = df.withColumn(
    "Total_Quantity_Sold",F.sum("procured_quantity").over(Window.partitionBy("product_id")).cast("bigint"))

# Calculate the total sales per product
df = df.withColumn(
    "Total_Sales",F.sum(F.col("procured_quantity") * F.col("unit_selling_price")).over(Window.partitionBy("product_id")).cast("bigint"))

df = df.withColumn(
    "Total_Revenue",
     ((F.col("procured_quantity") * F.col("unit_selling_price")) - F.col("total_discount_amount")).cast("bigint"))

# Calculate the discount percentage for each row
df = df.withColumn("Discount_Percentage", 
                   F.round((F.col("total_discount_amount") / 
                            (F.col("unit_selling_price") * F.col("procured_quantity"))) * 100))

# Calculate the profit margin for each row
df = df.withColumn("Profit_Margin", 
                   F.round(((((F.col("procured_quantity") * F.col("unit_selling_price")) - 
                              F.col("total_discount_amount")) - 
                             F.col("total_weighted_landing_price")) / 
                            (F.col("procured_quantity") * F.col("unit_selling_price"))) * 100))
                            
#adding new columns related to date functions
df = df.withColumn("year_", year(col("date_"))) \
       .withColumn("month_", month(col("date_"))) \
       .withColumn("monthname_", date_format(col("date_"), "MMMM")) \
       .withColumn("day_", dayofmonth(col("date_"))) \
       .withColumn("weekday_", date_format(col("date_"), "EEEE")) \
       .withColumn("weeknumber_", weekofyear(col("date_")))

df = df.dropna(subset=['Discount_Percentage', 'Profit_Margin'])

# Coalesce to a single partition to ensure a single output file
df = df.coalesce(1)

# Write the merged DataFrame back to S3
df.write.mode("overwrite").parquet("s3://cftcleandata/fact_cft/")

job.commit()
