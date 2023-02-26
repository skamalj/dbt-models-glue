## Lakehouse with Glue and DBT

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

## Lakehouse with EMR and DBT

EMR comes with built in support for Hudi, we utilizue packaged libraries to work with hudi.

## Create EMR cluster
aws emr create-cluster --name emrdbt --use-default-roles --release-label emr-6.7.0 \
--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.xlarge InstanceGroupType=CORE,InstanceCount=1,InstanceType=m4.large InstanceGroupType=TASK,InstanceCount=2,InstanceType=m4.large \
--applications Name=Hadoop Name=Spark Name=Tez \
--step-concurrency-level 3 \
--log-uri s3://skamalj-s3/emr_logs/ \
--ec2-attributes KeyName=home-key,SubnetId=subnet-07f82958e75238837 \
--bootstrap-actions Path="s3://skamalj-dbt/emr/emrbootstrap.sh" \
--managed-scaling-policy file://emr/emrscaling.json \
--configurations file://emr/emrconfig.json

## DBT EMR config
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

### Thrift server config
`Thrift server config and bootstrap script is included on "emr" folder.  Bootstrap actions run before hadoop/spark components are installed. We need to wait till the cluster is fully installed.`

## EMR Glue Catalog and Scaling
Glue catalog must be enabled at create time, configuration for the same is included in emr folder along with scaling configuration.

## Important
* Hudi tables on EMR, when created via spark-sql require that partition key is specified, hence all models have partition key. This is not required on glue as it uses pyspark
* Merge strategy does not work with dbt-spark due to additional hudi columns on tables. dbt-spark adapter needs modification to make it work. This code is available [here](https://github.com/skamalj/dbt-spark.git)