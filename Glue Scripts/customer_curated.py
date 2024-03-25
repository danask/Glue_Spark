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

# Script generated for node Customer_Trusted
Customer_Trusted_node1711327922406 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://danask-lake-house/stedi/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Customer_Trusted_node1711327922406",
)

# Script generated for node Accelerometer_Landing
Accelerometer_Landing_node1711327928729 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://danask-lake-house/stedi/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometer_Landing_node1711327928729",
)

# Script generated for node Join
Join_node1711328307439 = Join.apply(
    frame1=Customer_Trusted_node1711327922406,
    frame2=Accelerometer_Landing_node1711327928729,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1711328307439",
)

# Script generated for node Drop Fields
DropFields_node1711328327436 = DropFields.apply(
    frame=Join_node1711328307439,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1711328327436",
)

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct * from myDataSource
'''
SQLQuery_node1711340420597 = sparkSqlQuery(
    glueContext, 
    query = SqlQuery0, 
    mapping = {"myDataSource":DropFields_node1711328327436}, 
    transformation_ctx = "SQLQuery_node1711340420597"
)

# Script generated for node Amazon S3
AmazonS3_node1711328384535 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1711328327436,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://danask-lake-house/stedi/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1711328384535",
)

job.commit()
