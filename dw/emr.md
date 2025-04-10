
Criação do EMR na mesma rede VPC do RDS

USAR O MESMO EMR USADO NA TRUSTED

Criação de etapa:

```
aws emr add-steps \
	--cluster-id j-1MSUMN363EQB3 \
	--steps Type=Spark,Name="Load to DW and RDS",ActionOnFailure=CONTINUE,\
	Args=[--deploy-mode,client,--master,yarn,\
	--jars,s3://f-mba-nyc-dataset/emr/jars/mysql-connector-j-8.0.33.jar,\s3://f-mba-nyc-dataset/emr/scripts/load_to_dw_and_rds.py]

```

![image](https://github.com/user-attachments/assets/dc87c374-8ab1-4bdf-9724-eca6591b4b73)

- `--deploy-mode cluster`: roda o script diretamente no cluster EMR.

- `--master yarn`: usa o YARN como gerenciador de recursos.

- `--jars`: adiciona o conector JDBC necessário para escrever no RDS MySQL.

- `s3://.../load_to_dw_and_rds.py`: caminho script no S3.


Escalar o grupo CORE para 3 instâncias

```
aws emr modify-instance-groups \
  --cluster-id j-38ZUIC0TPOFM6 \
  --instance-groups InstanceGroupId=ig-2J7DSTDHV29W7,InstanceCount=3 \
  --region us-east-1
```

Permissões necessárias no bucket:

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowFullAccessToSpecificAccounts",
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    "arn:aws:iam::576030079868:root",
                    "arn:aws:iam::203375014542:root"
                ]
            },
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::f-mba-nyc-dataset",
                "arn:aws:s3:::f-mba-nyc-dataset/*"
            ]
        },
        {
            "Sid": "AllowEMRReadAccessToJar",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::290302628046:role/EMR_EC2_DefaultRole"
            },
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::f-mba-nyc-dataset/emr/jars/mysql-connector-j-8.0.33.jar"
        }
    ]
}
```
