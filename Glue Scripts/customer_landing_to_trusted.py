import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node CustomerLanding
CustomerLanding_node1711250553604 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://danask-lake-house/stedi/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1711250553604",
)

# Script generated for node PrivacyFilter
PrivacyFilter_node1711250855528 = Filter.apply(
    frame=CustomerLanding_node1711250553604,
    f=lambda row: (not (row["sharewithresearchasofdate"] == 0)),
    transformation_ctx="PrivacyFilter_node1711250855528",
)

# Script generated for node TrustedCusomer
TrustedCusomer_node1711251614798 = glueContext.write_dynamic_frame.from_options(
    frame=PrivacyFilter_node1711250855528,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://danask-lake-house/stedi/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="TrustedCusomer_node1711251614798",
)

job.commit()
