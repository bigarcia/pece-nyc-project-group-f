
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, year, month, dayofmonth, dayofweek, to_date, lit, monotonically_increasing_id
)
from pyspark.sql.types import LongType, DoubleType
import os

BUCKET_S3 = "mba-nyc-dataset"
TRUSTED_PATH = f"s3a://{BUCKET_S3}/trusted"
DW_PATH = f"s3a://{BUCKET_S3}/dw"
RDS_JDBC_URL = "jdbc:mysql://nyc-dw-mysql-v2.coseekllgrql.us-east-1.rds.amazonaws.com:3306/nyc_dw"
RDS_USER = "admin"
RDS_PASSWORD = "GrupoF_MBA_nyc2025"
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
        "PULocationID": "pickup_location",
        "DOLocationID": "dropoff_location",
        "RatecodeID": "code_ratecode",
        "VendorID": "code_vendor",
        "originating_base_num": "originating_base_number",
        "Affiliated_base_number": "affiliated_base_number",
        "payment_type": "code_payment_type"
    }
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    cast_map = {
        "code_vendor": LongType(),
        "code_ratecode": DoubleType(),
        "code_payment_type": DoubleType(),
        "trip_type": DoubleType(),
        "pickup_location": DoubleType(),
        "dropoff_location": DoubleType()
    }
    for col_name, dtype in cast_map.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(dtype))
    return df

def create_dimensions_and_fact(df: DataFrame, spark: SparkSession):
    df = df.withColumn("trip_id", monotonically_increasing_id())
    df = df.withColumn("pickup_date", to_date("pickup_datetime"))

    dim_date = df.select("pickup_date").distinct()         .withColumn("pk_date", monotonically_increasing_id())         .withColumn("day", dayofmonth("pickup_date"))         .withColumn("month", month("pickup_date"))         .withColumn("year", year("pickup_date"))         .withColumn("day_of_week", dayofweek("pickup_date"))

    df = df.join(dim_date.select("pickup_date", "pk_date"), on="pickup_date", how="left")            .withColumnRenamed("pk_date", "fk_date")

    dim_service_type = df.select("service_type").dropDuplicates().withColumn("pk_service_type", monotonically_increasing_id())                          .withColumnRenamed("service_type", "description")

    df = df.join(dim_service_type, on="description", how="left")            .withColumnRenamed("pk_service_type", "fk_service_type")

    dim_vendor = df.select("code_vendor").dropna().dropDuplicates().withColumn("pk_vendor", monotonically_increasing_id())                    .withColumnRenamed("code_vendor", "code")

    df = df.join(dim_vendor, on="code", how="left")            .withColumnRenamed("pk_vendor", "fk_vendor")

    dim_payment_type = df.select("code_payment_type").dropna().dropDuplicates().withColumn("pk_payment_type", monotonically_increasing_id())                          .withColumnRenamed("code_payment_type", "code")

    df = df.join(dim_payment_type, on="code", how="left")            .withColumnRenamed("pk_payment_type", "fk_payment_type")

    dim_ratecode = df.select("code_ratecode").dropna().dropDuplicates().withColumn("pk_ratecode", monotonically_increasing_id())                      .withColumnRenamed("code_ratecode", "code")

    df = df.join(dim_ratecode, on="code", how="left")            .withColumnRenamed("pk_ratecode", "fk_ratecode")

    dim_location = df.select("pickup_location", "dropoff_location").dropna().dropDuplicates()                      .withColumn("pk_location", monotonically_increasing_id())

    df = df.join(dim_location, on=["pickup_location", "dropoff_location"], how="left")            .withColumnRenamed("pk_location", "fk_location")

    fact_taxi_trip = df.select(
        "trip_id", "pickup_datetime", "dropoff_datetime", "passenger_count",
        "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
        "total_amount", "fk_payment_type", "fk_ratecode", "fk_vendor", "fk_location",
        "fk_service_type", "fk_date"
    )

    tables = {
        "dim_date": dim_date,
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
