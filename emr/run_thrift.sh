#!/bin/bash
status=`aws emr list-clusters --query 'Clusters[?Status.State != \`TERMINATED\`] | [?Status.State != \`TERMINATED_WITH_ERRORS\`] | [?Name == \`emrdbt\`] | [0].Status.State'`
while [ "$status" != \"WAITING\" ]
do
  status=`aws emr list-clusters --query 'Clusters[?Status.State != \`TERMINATED\`] | [?Status.State != \`TERMINATED_WITH_ERRORS\`] | [?Name == \`emrdbt\`] | [0].Status.State'`
  sleep 5
  echo "$status" "WAITING"
done
sudo cp /usr/lib/hudi/hudi-spark-bundle.jar /usr/lib/spark/jars/
sudo /usr/lib/spark/sbin/start-thriftserver.sh --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension
