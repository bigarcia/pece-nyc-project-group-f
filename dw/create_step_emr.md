
Criação do EMR:

```
aws emr create-cluster \
 --name "EMR PySpark Cluster Grupo F" \
 --log-uri "s3://mba-nyc-dataset/emr/logs" \
 --release-label "emr-6.10.0" \
 --service-role "EMR_DefaultRole" \
 --unhealthy-node-replacement \
 --applications Name=Spark \
 --ec2-attributes '{
   "InstanceProfile":"EMR_EC2_DefaultRole",
   "EmrManagedMasterSecurityGroup":"sg-039e4551e594b0810",
   "EmrManagedSlaveSecurityGroup":"sg-0ecce9d7903e4424f",
   "KeyName":"emr-keypair",
   "AvailabilityZone":"us-east-1a"
 }' \
 --instance-groups '[
   {
     "InstanceCount": 1,
     "InstanceGroupType": "MASTER",
     "Name": "MASTER",
     "InstanceType": "m5.xlarge",
     "EbsConfiguration": {
       "EbsBlockDeviceConfigs": [
         {
           "VolumeSpecification": {
             "VolumeType": "gp2",
             "SizeInGB": 32
           },
           "VolumesPerInstance": 2
         }
       ]
     }
   },
   {
     "InstanceCount": 2,
     "InstanceGroupType": "CORE",
     "Name": "CORE",
     "InstanceType": "m5.xlarge",
     "EbsConfiguration": {
       "EbsBlockDeviceConfigs": [
         {
           "VolumeSpecification": {
             "VolumeType": "gp2",
             "SizeInGB": 32
           },
           "VolumesPerInstance": 2
         }
       ]
     }
   }
 ]' \
 --steps '[
   {
     "Name": "Load Trusted to DW and RDS",
     "ActionOnFailure": "CONTINUE",
     "Jar": "command-runner.jar",
     "Args": [
       "spark-submit",
       "--deploy-mode", "cluster",
       "--master", "yarn",
       "--conf", "spark.jars=s3://mba-nyc-dataset/emr/jars/mysql-connector-j-8.0.33.jar",
       "s3://mba-nyc-dataset/emr/scripts/load_to_dw_and_rds.py"
     ]
   }
 ]' \
 --scale-down-behavior "TERMINATE_AT_TASK_COMPLETION" \
 --region "us-east-1"

```
Criação de etapa:
```
aws emr add-steps \
  --cluster-id j-2BF29XPYP368K \
  --steps 'Type=Spark,Name="Load To DW and RDS",ActionOnFailure=CONTINUE,Args=["spark-submit","--deploy-mode","cluster","--master","yarn","--conf","spark.jars=s3://mba-nyc-dataset/emr/jars/mysql-connector-j-8.0.33.jar","s3://mba-nyc-dataset/emr/scripts/load_to_dw_and_rds.py"]'

```

![image](https://github.com/user-attachments/assets/a03a2d3b-bf9b-4443-bce2-60bb2f15b8a0)

`--deploy-mode cluster`: roda o script diretamente no cluster EMR.

`--master yarn`: usa o YARN como gerenciador de recursos.

`--jars`: adiciona o conector JDBC necessário para escrever no RDS MySQL.

`s3://.../load_to_dw_and_rds.py`: caminho script no S3.
