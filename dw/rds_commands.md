# Criação do RDS e tabelas
1. Criar um grupo de segurança para o RDS

```
aws ec2 create-security-group \
  --group-name nyc-mysql-sg \
  --description "SG para RDS MySQL DW NYC" \
  --vpc-id $(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query "Vpcs[0].VpcId" --output text)
```

![image](https://github.com/user-attachments/assets/3d1af60f-546c-459f-b232-20202d48e50a)

Grupo retornado:
```
{
    "GroupId": "sg-0705f0473d9bcdc1b",
    "SecurityGroupArn": "arn:aws:ec2:us-east-1:178179121271:security-group/sg-0705f0473d9bcdc1b"
}
```

2. Liberar acesso à porta MySQL (3306)

```
aws ec2 authorize-security-group-ingress \
  --group-name nyc-mysql-sg \
  --protocol tcp \
  --port 3306 \
  --cidr 0.0.0.0/0
```
![image](https://github.com/user-attachments/assets/cc4729eb-578a-48e8-95de-fcf0949eb807)

Retorno:
```
{
    "Return": true,
    "SecurityGroupRules": [
        {
            "SecurityGroupRuleId": "sgr-00e8b42cd2a6ab08b",
            "GroupId": "sg-0705f0473d9bcdc1b",
            "GroupOwnerId": "178179121271",
            "IsEgress": false,
            "IpProtocol": "tcp",
            "FromPort": 3306,
            "ToPort": 3306,
            "CidrIpv4": "0.0.0.0/0",
            "SecurityGroupRuleArn": "arn:aws:ec2:us-east-1:178179121271:security-group-rule/sgr-00e8b42cd2a6ab08b"
        }
    ]
}
```
3. Criar instância RDS MySQL

```
aws rds create-db-instance \
  --db-instance-identifier nyc-dw-mysql-v2 \
  --db-instance-class db.t3.medium \
  --engine mysql \
  --master-username admin \
  --master-user-password 'GrupoF_MBA_nyc2025' \
  --allocated-storage 50 \
  --storage-type gp2 \
  --vpc-security-group-ids sg-0705f0473d9bcdc1b \
  --publicly-accessible \
  --availability-zone us-east-1a \
  --backup-retention-period 1 \
  --no-multi-az \
  --db-name nyc_dw \
  --region us-east-1

```


![image](https://github.com/user-attachments/assets/2721c0ad-a437-4f74-8294-87cf0ccaede2)

![image](https://github.com/user-attachments/assets/1b892c0b-264c-4333-89a1-25b6c098415f)



4. Verificar status

```
aws rds describe-db-instances \
  --db-instance-identifier nyc-dw-mysql-v2 \
  --query "DBInstances[0].DBInstanceStatus"
```
![image](https://github.com/user-attachments/assets/2c6fcc00-bd4a-45a3-bb84-b19a2af230f4)

5. Pegar o endpoint

```
aws rds describe-db-instances \
  --db-instance-identifier nyc-dw-mysql-v2 \
  --query "DBInstances[0].Endpoint.Address" \
  --output text
```
nyc-dw-mysql-v2.coseekllgrql.us-east-1.rds.amazonaws.com

![image](https://github.com/user-attachments/assets/ed51b034-fa6a-4ad3-904a-d08d5c2b9fc2)

6.Pegar usuário e senha
É o mesmo definido na criação da instância
```
--master-username admin \
--master-user-password GrupoF_MBA_nyc2025 \
```
Seção "Configurações":
- Usuário mestre (Master username): admin
- Senha: não aparece no console por segurança

Caso ainda assim precise da senha, redifina.

6.1 Redefinir a senha:

- Modificar
- Configurações
- Novo grupo de configurações
- Nova senha mestre
- Continuar
- Modificar instância

A alteração da senha não reinicia a instância

7. Criar Banco de dados.
   
   7.1 Conectar na instância
   
`mysql -h nyc-dw-mysql-v2.chmgbrx9sdjy.us-east-1.rds.amazonaws.com -u admin -p`

![image](https://github.com/user-attachments/assets/aaf58740-9938-4701-b2ab-b2ec0c88c310)

   7.2 Criar banco de dados `nyc_dw`:
   
`CREATE DATABASE nyc_dw;` 

   ![image](https://github.com/user-attachments/assets/b0de7591-6b56-479e-a7d9-b9821b430983)


8. Ver todos bases criadas:
   
  `SHOW DATABASES;`
  
  ![image](https://github.com/user-attachments/assets/e0091f8f-7dcf-4694-b14a-47438c261a8d)

9. Ver tabelas criadas:
    
`USE nyc_dw;`

`SHOW TABLES;`

![image](https://github.com/user-attachments/assets/f22d1bdd-f840-44ba-b919-a93772dcca84)

`DESCRIBE fact_taxi_trip;`

![image](https://github.com/user-attachments/assets/e4cdddeb-bbbd-492d-bff4-ed8f3c4efff2)

# Instalar o JDBC do MySQL

https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar


Cloud9:
```
mkdir -p /home/ec2-user/spark_jars
wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar -P /home/ec2-user/spark_jars/
```

EMR:
```s3://mba-nyc-dataset/emr/jars/mysql-connector-j-8.0.33.jar```
![image](https://github.com/user-attachments/assets/d7183673-c428-41af-9c37-bc067c11a23e)

10. Rodar código pyspark
