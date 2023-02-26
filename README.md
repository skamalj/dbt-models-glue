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
aws emr create-cluster --name emrdbt --use-default-roles --release-label emr-6.7.0 \
--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.xlarge InstanceGroupType=CORE,InstanceCount=1,InstanceType=m4.large InstanceGroupType=TASK,InstanceCount=2,InstanceType=m4.large \
--applications Name=Hadoop Name=Spark Name=Tez \
--step-concurrency-level 3 \
--log-uri s3://skamalj-s3/emr_logs/ \
--ec2-attributes KeyName=home-key,SubnetId=subnet-07f82958e75238837 \
--bootstrap-actions Path="s3://skamalj-dbt/emr/emrbootstrap.sh" \
--managed-scaling-policy file://./emrscaling.json \
--configurations file://./emrconfig.json

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

   sudo cp /usr/lib/hudi/hudi-spark-bundle.jar /usr/lib/spark/jars

   sudo /usr/lib/spark/sbin/start-thriftserver.sh --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension

    merge into flights.hudi_emp_cow_emr as DBT_INTERNAL_DEST
    using flights.emp_raw as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.id = DBT_INTERNAL_DEST.id
    when matched then update set * 
    when not matched then insert *

  create table flights.hudi_cow_pt_tbl (
  id bigint,
  name string,
  ts bigint,
  dt string,
  hh string
) using hudi
tblproperties (
  type = 'mor',
  primaryKey = 'id,hh',
  preCombineField = 'ts',
  hoodie.datasource.hive_sync.skip_ro_suffix = true
 )
partitioned by (dt);

insert into hudi_cow_pt_tbl select 1, 'a0', 1000, '2021-12-09', '10';
select * from hudi_cow_pt_tbl;

-- record id=1 changes `name`
insert into hudi_cow_pt_tbl select 1, 'a1', 1001, '2021-12-09', '10';
select * from hudi_cow_pt_tbl;

-- time travel based on first commit time, assume `20220307091628793`
select * from hudi_cow_pt_tbl timestamp as of '20220307091628793' where id = 1;
-- time travel based on different timestamp formats
select * from hudi_cow_pt_tbl timestamp as of '2022-03-07 09:16:28.100' where id = 1;
select * from hudi_cow_pt_tbl timestamp as of '2022-03-08' where id = 1;

create table if not exists flights.h3(
  id bigint, 
  name string, 
  price double
) using hudi
options (
  primaryKey = 'id',
  type = 'mor',
  hoodie.cleaner.fileversions.retained = '20',
  hoodie.keep.max.commits = '20'
);

create table flights.hudi_ctas_cow_pt_tbl
using hudi
tblproperties (type = 'mor', primaryKey = 'id', preCombineField = 'ts')
as
select 1 as id, 'a1' as name, 10 as price, 1000 as ts, '2021-12-01' as dt;

create table flights.hudi_ctas_emp_mor
using hudi
tblproperties (type = 'mor', primaryKey = 'id', preCombineField = 'eventtime', hoodie.datasource.hive_sync.skip_ro_suffix = true)
partitioned by (dept)
as
select * from flights.emp_raw;

create table flights.hudi_ctas_cow_nonpcf_tbl
using hudi
tblproperties (primaryKey = 'id')
as
select 1 as id, 'a1' as name, 10 as price;