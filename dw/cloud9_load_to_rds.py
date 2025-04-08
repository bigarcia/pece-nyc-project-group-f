from pyspark.sql import SparkSession

# Dados de conexão com o RDS
jdbc_url = "jdbc:mysql://<ENDPOINT>:3306/nyc_dw"
jdbc_props = {
    "user": "<USUARIO>",
    "password": "<SENHA>",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Caminho do conector JDBC local (Cloud9)
jar_path = "/home/ec2-user/spark_jars/mysql-connector-j-8.0.33.jar"

# Inicia Spark com o JAR
spark = SparkSession.builder \
    .appName("Export to RDS - Cloud9") \
    .config("spark.jars", jar_path) \
    .getOrCreate()

# Lista de tabelas a exportar
tables = [
    "fact_taxi_trip", "dim_time", "dim_service_type", "dim_location",
    "dim_payment_type", "dim_vendor", "dim_ratecode"
]

# Caminho base no S3
base_path = "s3a://mba-nyc-dataset/dw/"

for table in tables:
    print(f"📤 Exportando tabela: {table}")
    df = spark.read.parquet(f"{base_path}{table}")
    df.write.jdbc(url=jdbc_url, table=table, mode="overwrite", properties=jdbc_props)

print("✅ Exportação concluída com sucesso!")
