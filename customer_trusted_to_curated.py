import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1686102292408 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-project",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1686102292408",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-project",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node1686102222894 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CustomerTrusted_node1686102292408,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerPrivacyFilter_node1686102222894",
)

# Script generated for node Drop Fields
DropFields_node1686102591478 = DropFields.apply(
    frame=CustomerPrivacyFilter_node1686102222894,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1686102591478",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1686107024179 = DynamicFrame.fromDF(
    DropFields_node1686102591478.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1686107024179",
)

# Script generated for node Customer Curated
CustomerCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1686107024179,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project-bucket/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node3",
)

job.commit()
