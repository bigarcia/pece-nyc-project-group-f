- Liberar espaço limpando os diretórios usados por Spark:

```
sudo rm -rf /tmp/*
```

- Instalar o JDBC do MySQL

https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar

```
mkdir -p /home/ec2-user/spark_jars
wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar -P /home/ec2-user/spark_jars/
```


Crie pasta temporária exclusiva pro Spark

```
mkdir -p ~/tmp_spark
chmod 777 ~/tmp_spark
```

Limpa espaço para corrigir erro na execução por falta de espaço:

```
rm -rf /tmp/*
rm -rf ~/tmp_spark
```

Espaço disponível:

```df -h```
