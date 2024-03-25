import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer_Landing
Accelerometer_Landing_node1711332907552 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://danask-lake-house/stedi/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometer_Landing_node1711332907552",
)

# Script generated for node Customer_Trusted
Customer_Trusted_node1711332976257 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://danask-lake-house/stedi/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Customer_Trusted_node1711332976257",
)

# Script generated for node Join
Join_node1711333022086 = Join.apply(
    frame1=Customer_Trusted_node1711332976257,
    frame2=Accelerometer_Landing_node1711332907552,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1711333022086",
)

# Script generated for node Drop Fields
DropFields_node1711333047108 = DropFields.apply(
    frame=Join_node1711333022086,
    paths=[
        "serialNumber",
        "birthDay",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithPublicAsOfDate",
    ],
    transformation_ctx="DropFields_node1711333047108",
)

# Script generated for node Accelerometer_Trusted
Accelerometer_Trusted_node1711333078154 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1711333047108,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://danask-lake-house/stedi/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="Accelerometer_Trusted_node1711333078154",
)

job.commit()
