
Criação do EMR na mesma rede VPC do RDS

```
aws emr create-cluster \
  --name "NYC Taxi Load to DW and RDS" \
  --release-label emr-6.15.0 \
  --applications Name=Spark \
  --log-uri s3://mba-nyc-dataset/emr/logs/ \
  --ec2-attributes KeyName=emr-keypair,SubnetId=subnet-08b07f8cf72e46285 \
  --instance-type m5.xlarge \
  --instance-count 2 \
  --use-default-roles \
  --auto-terminate \
  --steps Type=Spark,Name="Run load_to_dw_and_rds.py",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,s3://mba-nyc-dataset/emr/scripts/load_to_dw_and_rds.py] \
  --configurations '[{"Classification":"spark-defaults","Properties":{"spark.executor.memory":"4g","spark.driver.memory":"4g"}}]'


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
