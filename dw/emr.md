
Criação do EMR na mesma rede VPC do RDS

```
aws emr create-cluster \
 --name "EMR PySpark Cluster" \
 --log-uri "s3://mba-nyc-dataset/emr/logs" \
 --release-label "emr-6.10.0" \
 --service-role "EMR_DefaultRole" \
 --unhealthy-node-replacement \
 --ec2-attributes '{"InstanceProfile":"EMR_EC2_DefaultRole","EmrManagedMasterSecurityGroup":"sg-039e4551e594b0810","EmrManagedSlaveSecurityGroup":"sg-0ecce9d7903e4424f","KeyName":"emr-keypair","AvailabilityZone":"us-east-1a"}' \
 --applications Name=Spark \
 --instance-groups '[{"InstanceCount":2,"InstanceGroupType":"CORE","Name":"Core","InstanceType":"m5.xlarge","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp2","SizeInGB":32},"VolumesPerInstance":2}]}},{"InstanceCount":1,"InstanceGroupType":"MASTER","Name":"Master","InstanceType":"m5.xlarge","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp2","SizeInGB":32},"VolumesPerInstance":2}]}}]' \
 --steps '[{"Name":"Load To DW and RDS","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Args":["spark-submit","--deploy-mode","cluster","--master","yarn","--conf","spark.jars=s3://mba-nyc-dataset/emr/jars/mysql-connector-j-8.0.33.jar","s3://mba-nyc-dataset/emr/scripts/load_to_dw_and_rds.py"],"Type":"CUSTOM_JAR"},{"Name":"Load to DW and RDS","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Args":["spark-submit","--deploy-mode","cluster","--master","yarn","--jars","s3://mba-nyc-dataset/emr/jars/mysql-connector-j-8.0.33.jar","s3://mba-nyc-dataset/emr/scripts/load_to_dw_and_rds.py"],"Type":"CUSTOM_JAR"},{"Name":"Load to DW and RDS","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Args":["spark-submit","--deploy-mode","cluster","--master","yarn","--jars","s3://mba-nyc-dataset/emr/jars/mysql-connector-j-8.0.33.jar","s3://mba-nyc-dataset/emr/scripts/load_to_dw_and_rds.py"],"Type":"CUSTOM_JAR"},{"Name":"Load DW and RDS (script atualizado)","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Args":["spark-submit","--deploy-mode","cluster","--master","yarn","--jars","s3://mba-nyc-dataset/emr/jars/mysql-connector-j-8.0.33.jar","s3://mba-nyc-dataset/emr/scripts/load_to_dw_and_rds.py"],"Type":"CUSTOM_JAR"}]' \
 --scale-down-behavior "TERMINATE_AT_TASK_COMPLETION" \
 --region "us-east-1"
```

![image](https://github.com/user-attachments/assets/2e54f869-d624-45d9-a868-dffe44334a20)

Criação de etapa:

```
aws emr add-steps \
  --cluster-id j-38ZUIC0TPOFM6 \
  --steps '[
    {
      "Name": "Load DW and RDS (script atualizado)",
      "ActionOnFailure": "CONTINUE",
      "Type": "CUSTOM_JAR",
      "Jar": "command-runner.jar",
      "Args": [
        "spark-submit",
        "--deploy-mode", "cluster",
        "--master", "yarn",
        "--jars", "s3://mba-nyc-dataset/emr/jars/mysql-connector-j-8.0.33.jar",
        "s3://mba-nyc-dataset/emr/scripts/load_to_dw_and_rds.py"
      ]
    }
  ]' \
  --region us-east-1

```

![image](https://github.com/user-attachments/assets/dc87c374-8ab1-4bdf-9724-eca6591b4b73)

`--deploy-mode cluster`: roda o script diretamente no cluster EMR.

`--master yarn`: usa o YARN como gerenciador de recursos.

`--jars`: adiciona o conector JDBC necessário para escrever no RDS MySQL.

`s3://.../load_to_dw_and_rds.py`: caminho script no S3.
