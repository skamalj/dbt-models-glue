### This repository has code for this blog - [Lakehouse on AWS with Hudi and DBT](https://www.kamalsblog.com/2022/09/data-lakehouse-on-aws-with-hudi-and-dbt.html)


### DBT profile to use.  Replace region , role and bucket values
``
sync_tool_classes is not supported in dbt glue adapter. There is customized code available from this repo.
``
```
glue:
  outputs:
    dev:
      type: glue
      query-comment: This is a glue dbt example
      role_arn: <role arn>
      region: <region>
      workers: 1
      worker_type: Standard
      idle_timeout: 10
      glue_version: "3.0"
      extra_jars: "s3://<bucket>/calcite-core-1.15.0.jar,s3://<bucket>/hudi-spark3.1-bundle_2.12-0.12.0.jar,s3://<bucket>/spark-avro_2.12-3.1.0.jar,s3://<bucket>/httpclient-4.5.9.jar,s3://<bucket>/json-serde-1.3.8-jar-with-dependencies.jar"
      schema: "flights"
      database: "flights"
      session_provisioning_timeout_in_seconds: 120
      location: "s3://<bucket>/hudi"
      conf: "spark.serializer=org.apache.spark.serializer.KryoSerializer"
      sync_tool_classes: "org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool"
```

### Create table statement for test data included with repo
test_0.json can be used as initial load and test_update_0.json can be used to test incremental updates.
```
CREATE EXTERNAL TABLE `emp_raw`(
  `id` int,
  `dept` int,
  `firstname` string,
  `lastname` string,
  `phone` string,
  `email` string,
  `pincode` int,
  `eventtime` bigint,
  `joiningdate` string)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'paths'='id,firstname,lastname,joiningdate,eventtime,pincode,email,phone') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://<bucket-name>/hudidata/'
TBLPROPERTIES (
  'classification'='json')
```

## Create EMR cluster
aws emr create-cluster --name emrdbt  --use-default-roles --release-label emr-6.7.0 \
--instance-type m4.large --instance-count 2 \
--applications Name=Hadoop Name=Hive Name=Spark Name=Presto Name=Tez \
--log-uri s3://skamalj-s3/emr_logs/ \
--ec2-attributes KeyName=home-key,SubnetId=subnet-07f82958e75238837 \
--steps Name="Start Thrift Server",Jar=command-runner.jar,Args=sudo,/usr/lib/spark/sbin/start-thriftserver.sh \
--configurations '[{"Classification":"spark-hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}}]'

## EMR config
emr:
 target: dev
 outputs:
  dev:
   type: spark
   method: thrift
   schema: dbt
   host: master.hostname
   port: 10001
   user: root