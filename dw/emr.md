
Criação do EMR na mesma rede VPC do RDS

```
aws emr create-cluster \
  --name "NYC EMR in RDS VPC" \
  --release-label "emr-6.10.0" \
  --applications Name=Spark \
  --log-uri "s3://mba-nyc-dataset/emr/logs" \
  --ec2-attributes '{
    "InstanceProfile":"EMR_EC2_DefaultRole",
    "KeyName":"emr-keypair",
    "SubnetId":"subnet-08b07f8cf72e46285",
    "EmrManagedMasterSecurityGroup":"sg-0705f0473d9bcdc1b",
    "EmrManagedSlaveSecurityGroup":"sg-0705f0473d9bcdc1b"
  }' \
  --service-role "EMR_DefaultRole" \
  --instance-groups '[
    {
      "InstanceGroupType":"MASTER",
      "InstanceType":"m5.xlarge",
      "InstanceCount":1,
      "Name":"Master nodes"
    },
    {
      "InstanceGroupType":"CORE",
      "InstanceType":"m5.xlarge",
      "InstanceCount":2,
      "Name":"Core nodes"
    }
  ]' \
  --scale-down-behavior "TERMINATE_AT_TASK_COMPLETION" \
  --region us-east-1

```

![image](https://github.com/user-attachments/assets/4726a69f-6adf-402e-8e8e-5992f2fbce9e)

Criação de etapa:

```
aws emr add-steps \
  --cluster-id j-38ZUIC0TPOFM6 \
  --steps '[
    {
      "Name": "Load to DW and RDS",
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

![image](https://github.com/user-attachments/assets/a03a2d3b-bf9b-4443-bce2-60bb2f15b8a0)

`--deploy-mode cluster`: roda o script diretamente no cluster EMR.

`--master yarn`: usa o YARN como gerenciador de recursos.

`--jars`: adiciona o conector JDBC necessário para escrever no RDS MySQL.

`s3://.../load_to_dw_and_rds.py`: caminho script no S3.
