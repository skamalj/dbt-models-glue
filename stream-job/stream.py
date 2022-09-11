import sys
from awsglue.transforms import *
from pyspark.sql.functions import current_timestamp
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

from pyspark.sql import DataFrame, Row
import datetime
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sparkSession = SparkSession.builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") .getOrCreate()
sc = sparkSession.sparkContext
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Kinesis Stream
dataframe_KinesisStream_node1 = glueContext.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options={
        "typeOfData": "kinesis",
        "streamARN": "<Stream ARN>",
        "classification": "json",
        "startingPosition": "earliest",
        "inferSchema": "true",
    },
    transformation_ctx="dataframe_KinesisStream_node1",
)


def processBatch(data_frame, batchId):
    logger = glueContext.get_logger()
    if data_frame.count() > 0:
        KinesisStream_node1 = DynamicFrame.fromDF(
            data_frame, glueContext, "from_data_frame"
        )
        # Script generated for node SelectFields
        logger.info('Kinesis schema is:' + str(KinesisStream_node1.schema()) )
        KinesisStream_node2 = KinesisStream_node1.apply_mapping(
            [
                ('id','bigint', 'id','int' ),
                ('dept','bigint', 'dept','int' ),
                ('firstname','string','firstname','string'),
                ('lastname','string','lastname','string'),
                ('phone','string','phone','string'),
                ('email','string','email','string'), 
                ('pincode','bigint','pincode','int'), 
                ('eventtime','bigint','eventtime','bigint'), 
                ('joiningDate','string','joiningDate','string') 
            ]
        )
        SelectFields_node2 = SelectFields.apply(
            frame=KinesisStream_node2, paths=['id','dept','firstname','lastname','phone','joiningDate','pincode','eventtime','email'], transformation_ctx="SelectFields_node2"
        )
        
        logger.info('Dataframe for this batch:' + str(data_frame.take(data_frame.count())))
        logger.info('Selected Field:' + str(SelectFields_node2.toDF().take(1)))
        
        combinedConf = {'className' : 'org.apache.hudi', 
        'hoodie.datasource.hive_sync.use_jdbc':'false', 
        'hoodie.datasource.write.precombine.field': 'eventtime', 
        'hoodie.consistency.check.enabled': 'true', 
        'hoodie.datasource.write.recordkey.field': 'id', 
        'hoodie.datasource.write.partitionpath.field': 'dept',
        'hoodie.datasource.hive_sync.partition_fields': 'dept',
        'hoodie.datasource.write.table.type': 'MERGE_ON_READ',
        'hoodie.table.name': 'hudi_emp_mor', 
        'hoodie.datasource.hive_sync.database': 'flights', 
        'hoodie.datasource.hive_sync.table': 'hudi_emp_mor', 
        'hoodie.datasource.hive_sync.enable': 'true',  
        'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
        'hoodie.upsert.shuffle.parallelism': 20, 'hoodie.datasource.write.operation': 'upsert', 
        'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS', 'hoodie.cleaner.commits.retained': 10}
        (
            SelectFields_node2.toDF().withColumnRenamed('joiningDate', 'joiningdate')
            .withColumn("update_hudi_ts",current_timestamp())
            .write.format('org.apache.hudi')
            .options(**combinedConf)
            .mode('Append')
            .save("s3://<bucket-location>/flights/hudi_emp_mor/")
        )
          
glueContext.forEachBatch(
    frame=dataframe_KinesisStream_node1,
    batch_function=processBatch,
    options={
        "windowSize": "10 seconds",
        "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/",
    },
)
job.commit()
