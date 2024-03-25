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

# Script generated for node Step_Trainer_Trusted
Step_Trainer_Trusted_node1711345085644 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://danask-lake-house/stedi/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Step_Trainer_Trusted_node1711345085644",
)

# Script generated for node Accelerometer_Trusted
Accelerometer_Trusted_node1711345132305 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://danask-lake-house/stedi/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometer_Trusted_node1711345132305",
)

# Script generated for node Join
Join_node1711345208028 = Join.apply(
    frame1=Accelerometer_Trusted_node1711345132305,
    frame2=Step_Trainer_Trusted_node1711345085644,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="Join_node1711345208028",
)

# Script generated for node Machine_Learning_Curated2
Machine_Learning_Curated2_node1711345256047 = (
    glueContext.write_dynamic_frame.from_options(
        frame=Join_node1711345208028,
        connection_type="s3",
        format="json",
        connection_options={
            "path": "s3://danask-lake-house/stedi/machine_learning_curated/",
            "partitionKeys": [],
        },
        transformation_ctx="Machine_Learning_Curated2_node1711345256047",
    )
)

job.commit()

