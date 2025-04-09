
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, year, month, dayofweek, hour, lit, to_date, monotonically_increasing_id
)
from pyspark.sql.types import LongType, DoubleType
import os

# Constantes
BUCKET_S3 = "mba-nyc-dataset"
TRUSTED_PATH = f"s3a://{BUCKET_S3}/trusted"
DW_PATH = f"s3a://{BUCKET_S3}/dw"
RDS_JDBC_URL = "jdbc:mysql://nyc-dw-mysql.coseekllgrql.us-east-1.rds.amazonaws.com:3306/nyc_dw"
RDS_USER = "admin"
RDS_PASSWORD = "SuaSenhaForte123"  # Substitua pela senha real
RDS_JAR_PATH = f"s3://{BUCKET_S3}/emr/jars/mysql-connector-j-8.0.33.jar"

SERVICE_TYPES = ["yellowTaxi", "greenTaxi", "forHireVehicle", "hvfhs"]

def create_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder         .appName(app_name)         .config("spark.jars", RDS_JAR_PATH)         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")         .getOrCreate()

def load_dataframes(spark: SparkSession) -> DataFrame:
    dfs = []
    for service in SERVICE_TYPES:
        base_path = f"{TRUSTED_PATH}/{service}"
        try:
            df = spark.read.option("basePath", base_path).parquet(f"{base_path}/*/*/*.parquet")
            df = df.withColumn("service_type", lit(service))
            dfs.append(df)
        except:
            print(f"Nenhum dado encontrado para {service}")
    return dfs

def normalize_columns(df: DataFrame) -> DataFrame:
    rename_map = {
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "lpep_pickup_datetime": "pickup_datetime",
        "lpep_dropoff_datetime": "dropoff_datetime",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "RatecodeID": "ratecode_id",
        "VendorID": "vendor_id",
        "originating_base_num": "originating_base_number",
        "Affiliated_base_number": "affiliated_base_number"
    }
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    cast_map = {
        "vendor_id": LongType(),
        "ratecode_id": DoubleType(),
        "payment_type": DoubleType(),
        "trip_type": DoubleType(),
        "pickup_location_id": DoubleType(),
        "dropoff_location_id": DoubleType()
    }
    for col_name, dtype in cast_map.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(dtype))
    return df

def create_dimensions_and_fact(df: DataFrame, spark: SparkSession):
    df = df.withColumn("trip_id", monotonically_increasing_id())

    dim_time = df.select("pickup_datetime")         .withColumn("time_id", monotonically_increasing_id())         .withColumn("hour", hour("pickup_datetime"))         .withColumn("day_of_week", dayofweek("pickup_datetime"))         .withColumn("day", dayofweek("pickup_datetime"))         .withColumn("month", month("pickup_datetime"))         .withColumn("year", year("pickup_datetime"))         .dropDuplicates(["pickup_datetime"])

    dim_service_type = df.select("service_type").dropDuplicates().withColumn("service_type_id", monotonically_increasing_id())
    dim_vendor = df.select("vendor_id").dropna().dropDuplicates().withColumn("vendor_key", monotonically_increasing_id())
    dim_payment_type = df.select("payment_type").dropna().dropDuplicates().withColumn("payment_type_id", monotonically_increasing_id())
    dim_ratecode = df.select("ratecode_id").dropna().dropDuplicates().withColumn("ratecode_id_key", monotonically_increasing_id())
    dim_location = df.select("pickup_location_id", "dropoff_location_id").dropna().dropDuplicates().withColumn("location_id", monotonically_increasing_id())

    fact_taxi_trip = df.select(
        "trip_id", "pickup_datetime", "dropoff_datetime", "passenger_count",
        "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
        "total_amount", "payment_type", "ratecode_id", "vendor_id", "pickup_location_id",
        "dropoff_location_id", "service_type"
    )

    tables = {
        "dim_time": dim_time,
        "dim_service_type": dim_service_type,
        "dim_vendor": dim_vendor,
        "dim_payment_type": dim_payment_type,
        "dim_ratecode": dim_ratecode,
        "dim_location": dim_location,
        "fact_taxi_trip": fact_taxi_trip
    }

    for name, df in tables.items():
        df.write.mode("overwrite").parquet(f"{DW_PATH}/{name}")
        df.write             .format("jdbc")             .option("url", RDS_JDBC_URL)             .option("dbtable", name)             .option("user", RDS_USER)             .option("password", RDS_PASSWORD)             .option("driver", "com.mysql.cj.jdbc.Driver")             .mode("overwrite")             .save()

def main():
    spark = create_spark_session("NYC Taxi Load to DW and RDS")
    dfs = load_dataframes(spark)
    if not dfs:
        print("Nenhum dado carregado.")
        return
    df_union = normalize_columns(dfs[0])
    for df in dfs[1:]:
        df_union = df_union.unionByName(normalize_columns(df), allowMissingColumns=True)
    create_dimensions_and_fact(df_union, spark)
    spark.stop()

if __name__ == "__main__":
    main()
