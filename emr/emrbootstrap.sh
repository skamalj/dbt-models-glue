aws s3 cp s3://skamalj-dbt/emr/run_thrift.sh /home/hadoop/
chmod a+x /home/hadoop/run_thrift.sh
nohup /home/hadoop/run_thrift.sh &
