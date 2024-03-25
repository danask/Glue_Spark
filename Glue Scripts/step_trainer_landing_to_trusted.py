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

# Script generated for node Step_Trainer_Landing
Step_Trainer_Landing_node1711328848924 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://danask-lake-house/stedi/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Step_Trainer_Landing_node1711328848924",
)

# Script generated for node Customer_Curated
Customer_Curated_node1711328917143 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://danask-lake-house/stedi/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="Customer_Curated_node1711328917143",
)

# Script generated for node Join
Join_node1711329213188 = Join.apply(
    frame1=Step_Trainer_Landing_node1711328848924,
    frame2=Customer_Curated_node1711328917143,
    keys1=["serialnumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1711329213188",
)

# Script generated for node Drop Fields
DropFields_node1711329330055 = DropFields.apply(
    frame=Join_node1711329213188,
    paths=[
        "`.serialNumber`",
        "registrationDate",
        "lastUpdateDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
        "customerName",
        "email",
        "phone",
        "birthDay",
    ],
    transformation_ctx="DropFields_node1711329330055",
)

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct serialnumber, sensorreadingtime, distancefromobject from myDataSource
'''
SQLQuery_node1711342200465 = sparkSqlQuery(
    glueContext, 
    query = SqlQuery0, 
    mapping = {"myDataSource":Join_node1711333961772}, 
    transformation_ctx = "SQLQuery_node1711342200465"
)

# Script generated for node Step_Trainer_Trusted
Step_Trainer_Trusted_node1711329420425 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1711329330055,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://danask-lake-house/stedi/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="Step_Trainer_Trusted_node1711329420425",
)

job.commit()


