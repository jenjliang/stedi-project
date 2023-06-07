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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project-bucket/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Customer Curated
CustomerCurated_node1686104757341 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project-bucket/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1686104757341",
)

# Script generated for node Select Serial Number Field only
SelectSerialNumberFieldonly_node1686144008051 = SelectFields.apply(
    frame=CustomerCurated_node1686104757341,
    paths=["serialNumber"],
    transformation_ctx="SelectSerialNumberFieldonly_node1686144008051",
)

# Script generated for node Drop Duplicate Serial Numbers
DropDuplicateSerialNumbers_node1686144049865 = DynamicFrame.fromDF(
    SelectSerialNumberFieldonly_node1686144008051.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicateSerialNumbers_node1686144049865",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1686144131832 = ApplyMapping.apply(
    frame=DropDuplicateSerialNumbers_node1686144049865,
    mappings=[("serialNumber", "string", "`(right) serialNumber`", "string")],
    transformation_ctx="RenamedkeysforJoin_node1686144131832",
)

# Script generated for node Join
Join_node1686104880092 = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=RenamedkeysforJoin_node1686144131832,
    keys1=["serialNumber"],
    keys2=["`(right) serialNumber`"],
    transformation_ctx="Join_node1686104880092",
)

# Script generated for node Drop Fields
DropFields_node1686104961313 = DropFields.apply(
    frame=Join_node1686104880092,
    paths=["`(right) serialNumber`"],
    transformation_ctx="DropFields_node1686104961313",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1686104961313,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project-bucket/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node3",
)

job.commit()
