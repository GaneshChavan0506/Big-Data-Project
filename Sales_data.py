import sys 
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, date_format, weekofyear, col
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read all CSV files into a single DataFrame
df = spark.read.csv("s3://flipkar-data-uncleaned/fact/", header=True, inferSchema=True)

df=df.drop("_c0")
df=df.drop("Unnamed: 0")

df = df.withColumn("date_", col("date_").cast("date"))

#removing rows from procured quantity which contains 0
df = df.filter(df.procured_quantity != 0)

df = df.withColumn(
    "total_weighted_landing_price",
    F.when(F.col("total_weighted_landing_price").isNull(), 0)
    .otherwise(F.col("total_weighted_landing_price"))
)

# Calculate the total sales per product
df = df.withColumn(
    "Total_Sales",(F.col("procured_quantity") * F.col("unit_selling_price")).cast("double"))

df = df.withColumn(
    "Total_Revenue",
     ((F.col("procured_quantity") * F.col("unit_selling_price")) - F.col("total_discount_amount")).cast("double"))

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
df = df.withColumn("monthname_", date_format(col("date_"), "MMMM")) \
       .withColumn("weekday_", date_format(col("date_"), "EEEE"))

df = df.withColumn(
    "Discount_Percentage",
    F.when(F.col("Discount_Percentage").isNull(), 0)
    .otherwise(F.col("Discount_Percentage"))
).withColumn(
    "Profit_Margin",
    F.when(F.col("Profit_Margin").isNull(), 0)
    .otherwise(F.col("Profit_Margin"))
)

# Coalesce to a single partition to ensure a single output file
df = df.coalesce(1)

# Write the merged DataFrame back to S3
df.write.mode("overwrite").parquet("s3://cleanautomatic/fact_c_data/")

job.commit()
