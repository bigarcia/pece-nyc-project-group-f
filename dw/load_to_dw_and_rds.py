from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    lit, to_date, date_format, hour, year, month, dayofweek, when, monotonically_increasing_id, col
)
from pyspark.sql.types import LongType, DoubleType, StringType
from typing import List, Tuple, Union

# Constante do bucket
BUCKET_S3 = "mba-nyc-dataset"

# Caminho da camada trusted e destino da camada dw
TRUSTED_PATH = f"s3a://{BUCKET_S3}/trusted"
DW_PATH = f"s3a://{BUCKET_S3}/dw"
RDS_JDBC_URL = "jdbc:mysql://nyc-dw-mysql.coseekllgrql.us-east-1.rds.amazonaws.com:3306/nyc_dw"
RDS_USER = "admin"
RDS_PASSWORD = "SuaSenhaForte123"
RDS_JAR_PATH = f"s3://{BUCKET_S3}/emr/jars/mysql-connector-j-8.0.33.jar"

def create_spark_session(app_name: str) -> SparkSession:
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", RDS_JAR_PATH) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()
    return spark

def read_and_normalize(spark: SparkSession, path: str, service_type: str, years: List[int], months: Union[List[int], str] = "*") -> DataFrame:
    if months == "*":
        parquet_paths = [f"{path}/year={year}/month=*/.parquet" for year in years]
    else:
        parquet_paths = [f"{path}/year={year}/month={month}/*.parquet" for year in years for month in months]

    df = spark.read.option("basePath", path).parquet(*parquet_paths)
    df = df.withColumn("service_type", lit(service_type))

    rename_map = {
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "lpep_pickup_datetime": "pickup_datetime",
        "lpep_dropoff_datetime": "dropoff_datetime",
        "PULocationID": "pickup_location_id",
        "DOlocationID": "dropoff_location_id",
        "RatecodeID": "ratecode_id",
        "VendorID": "vendor_id",
        "Affiliated_base_number": "affiliated_base_number",
        "originating_base_num": "originating_base_number"
    }
    for old_col, new_col in rename_map.items():
        if old_col in df.columns:
            df = df.withColumnRenamed(old_col, new_col)

    type_map = {
        "vendor_id": LongType(),
        "ratecode_id": DoubleType(),
        "payment_type": DoubleType(),
        "trip_type": DoubleType(),
        "pickup_location_id": DoubleType(),
        "dropoff_location_id": DoubleType()
    }
    for col_name, data_type in type_map.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, df[col_name].cast(data_type))

    required_cols = [
        "pickup_datetime", "dropoff_datetime", "vendor_id", "ratecode_id",
        "payment_type", "trip_type", "pickup_location_id", "dropoff_location_id",
        "passenger_count", "trip_distance", "fare_amount", "tip_amount",
        "tolls_amount", "total_amount", "extra", "mta_tax", "improvement_surcharge",
        "ehail_fee", "congestion_surcharge", "airport_fee", "sales_tax", "bcf",
        "driver_pay", "store_and_fwd_flag", "shared_request_flag", "shared_match_flag",
        "access_a_ride_flag", "wav_request_flag", "wav_match_flag", "affiliated_base_number",
        "originating_base_number", "has_problem", "problem_description", "year", "month"
    ]
    for col_name in required_cols:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None).cast(StringType()))

    df = df.withColumn("pickup_date", to_date("pickup_datetime")) \
           .withColumn("dropoff_date", to_date("dropoff_datetime")) \
           .withColumn("trip_duration_minutes", (col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long")) / 60) \
           .withColumn("weekday", date_format(col("pickup_datetime"), "EEEE")) \
           .withColumn("hour", hour("pickup_datetime")) \
           .withColumn("fk_time", date_format("dropoff_datetime", "ddMMyyyy").cast("int")) \
           .withColumn("sk_trip", monotonically_increasing_id())

    return df

def write_to_rds(df: DataFrame, table_name: str):
    print(f"📤 Exportando para RDS: {table_name}")
    df.repartition(10).write.jdbc(
        url=RDS_JDBC_URL,
        table=table_name,
        mode="append",
        properties={
            "user": RDS_USER,
            "password": RDS_PASSWORD,
            "driver": "com.mysql.cj.jdbc.Driver",
            "batchsize": "500"
        }
    )

def main():
    spark = create_spark_session("NYC Taxi - DW + RDS Export")

    anos = [2022,2023,2024]
    meses = [*]  # ou "*" para todos os meses disponíveis

    df = None
    try:
        df = read_and_normalize(spark, f"{TRUSTED_PATH}/yellowTaxi", "yellow", anos, meses)
    except Exception as e:
        print(f"⚠️ Erro ao carregar dados: {e}")
        return

    print("✅ Dados lidos com sucesso")

    df.write.mode("overwrite").parquet(f"{DW_PATH}/fact_taxi_trip")
    print("💾 Dados salvos na camada DW (Parquet)")

    write_to_rds(df, "fact_taxi_trip")
    print("📤 Dados exportados para o RDS com sucesso")

    spark.stop()
    print("🏁 Processo finalizado com sucesso")

if __name__ == "__main__":
    main()
