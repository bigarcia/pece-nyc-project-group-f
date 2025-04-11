from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, year, month, dayofmonth, dayofweek, to_date, lit, monotonically_increasing_id, when, date_format
)
from pyspark.sql.types import LongType, DoubleType
import os

# Caminho da camada trusted e destino da camada dw
BUCKET_S3 = "f-mba-nyc-dataset"
TRUSTED_PATH = f"s3a://{BUCKET_S3}/trusted"
DW_PATH = f"s3a://{BUCKET_S3}/dw"
RDS_JDBC_JAR = f"s3a://{BUCKET_S3}/emr/jars/mysql-connector-j-8.0.33.jar"
RDS_JDBC_URL = "jdbc:mysql://nyc-dw-mysql-v2.chmgbrx9sdjy.us-east-1.rds.amazonaws.com:3306/nyc_dw"
RDS_USER = "admin"
RDS_PASSWORD = "GrupoF_MBA_nyc2025"
# SERVICE_TYPES = ["yellowTaxi", "greenTaxi"]
SERVICE_TYPES = ["yellowTaxi", "greenTaxi", "forHireVehicle", "highVolumeForHire"]

def create_spark_session(app_name: str) -> SparkSession:
    jars_path = "/home/ec2-user/spark_jars/hadoop-aws-3.3.1.jar,/home/ec2-user/spark_jars/aws-java-sdk-bundle-1.11.901.jar,/home/ec2-user/spark_jars/mysql-connector-j-8.0.33.jar"
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", "/home/ec2-user/spark_jars/mysql-connector-j-8.0.33.jar") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()
    return spark

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

def create_df_service_type(df: DataFrame, spark: SparkSession, table_name: str = 'dim_service_type'):
    # Tabela de dimensão com descrição simples
    df_service_type = df.select("service_type").dropDuplicates() \
        .withColumnRenamed("service_type", "code")  # renomeando para 'code' para manter 'description' limpa
    
    # Adicionando a coluna de descrição completa
    df_service_type = df_service_type.withColumn(
        "description",
        when(df_service_type.code == "yellowTaxi", "Yellow Taxi")
        .when(df_service_type.code == "greenTaxi", "Green Taxi")
        .when(df_service_type.code == "forHireVehicle", "For-Hire Vehicle (FHV)")
        .when(df_service_type.code == "highVolumeForHire", "High-Volume For-Hire Vehicle")
        .otherwise("Desconhecido")
    )
    
    # Adicionando coluna 'category'
    df_service_type = df_service_type.withColumn(
        "category",
        when(df_service_type.code.isin("yellowTaxi", "greenTaxi"), "Taxi")
        .when(df_service_type.code.isin("forHireVehicle", "highVolumeForHire"), "For-Hire")
        .otherwise("Desconhecido")
    )
    
    # Adicionando coluna 'regulation_body'
    df_service_type = df_service_type.withColumn(
        "regulation_body",
        when(df_service_type.code.isin("yellowTaxi", "greenTaxi"), "TLC")
        .when(df_service_type.code.isin("forHireVehicle", "highVolumeForHire"), "Empresas Privadas")
        .otherwise("Desconhecido")
    )
    df_service_type.show(10)
    write_to_dw(df=df_service_type, spark=spark, table_name=table_name)
    return
    
def create_df_date(df: DataFrame, spark: SparkSession, table_name: str = 'dim_date'):
    df = df.withColumn("date", to_date("pickup_datetime"))

    
    df_date = df.select("date").distinct() \
        .withColumn("pk_date", date_format(col("date"), "yyyyMMdd")) \
        .withColumn("day", dayofmonth("date")) \
        .withColumn("month", month("date")) \
        .withColumn("year", year("date")) \
        .withColumn("day_of_week", dayofweek("date"))
    write_to_dw(df=df_date, spark=spark, table_name=table_name)
    return

def create_df_vendor(df: DataFrame, spark: SparkSession, table_name: str = 'dim_vendor') -> DataFrame:
    df_vendor = df.select("code_vendor") \
        .dropna() \
        .dropDuplicates() \
        .withColumnRenamed("code_vendor", "code") \
        .withColumn(
              "vendor_description",
              when(col("code") == "1", "Creative Mobile Technologies, LLC")
              .when(col("code") == "2", "Curb Mobility, LLC")
              .when(col("code") == "6", "Myle Technologies Inc")
              .when(col("code") == "7", "Helix")
              .otherwise("Unknown")
          ) \
        .withColumn("pk_vendor", monotonically_increasing_id())
    


    write_to_dw(df=df_vendor, spark=spark, table_name=table_name)

    # Realiza o join usando os nomes corretos
    return df.join(df_vendor.withColumnRenamed("code", "code_vendor"), on="code_vendor", how="left") \
             .withColumnRenamed("pk_vendor", "fk_vendor")


def create_df_payment_type(df: DataFrame, spark: SparkSession, table_name: str = 'dim_payment_type') -> DataFrame:
    df_payment_type = (
        df.select("code_payment_type")
          .dropna()
          .dropDuplicates()
          .withColumnRenamed("code_payment_type", "code")
          .withColumn(
              "payment_type_description",
              when(col("code") == "0", "Flex Fare trip")
              .when(col("code") == "1", "Credit card")
              .when(col("code") == "2", "Cash")
              .when(col("code") == "3", "No charge")
              .when(col("code") == "4", "Dispute")
              .when(col("code") == "5", "Unknown")
              .when(col("code") == "6", "Voided trip")
              .otherwise("Other")
          )
          .withColumn("pk_payment_type", monotonically_increasing_id())
    )

    write_to_dw(df=df_payment_type, spark=spark, table_name=table_name)

    return (
        df.join(df_payment_type.withColumnRenamed("code", "code_payment_type"), on="code_payment_type", how="left")
          .withColumnRenamed("pk_payment_type", "fk_payment_type")
    )

def create_df_ratecode(df: DataFrame, spark: SparkSession, table_name: str = 'dim_ratecode') -> DataFrame:
    df_ratecode = (
        df.select("code_ratecode")
          .dropna()
          .dropDuplicates()
          .withColumnRenamed("code_ratecode", "code")
          .withColumn(
              "ratecode_description",
              when(col("code") == "1", "Standard rate")
              .when(col("code") == "2", "JFK")
              .when(col("code") == "3", "Newark")
              .when(col("code") == "4", "Nassau or Westchester")
              .when(col("code") == "5", "Negotiated fare")
              .when(col("code") == "6", "Group ride")
              .when(col("code") == "99", "Null/unknown")
              .otherwise("Other")
          )
          .withColumn("pk_ratecode", monotonically_increasing_id())
    )

    write_to_dw(df=df_ratecode, spark=spark, table_name=table_name)

    return (
        df.join(df_ratecode.withColumnRenamed("code", "code_ratecode"), on="code_ratecode", how="left")
          .withColumnRenamed("pk_ratecode", "fk_ratecode")
    )


def create_df_location(df: DataFrame, spark: SparkSession, table_name: str = 'dim_location') -> DataFrame:
    df_location = (
        spark.read.parquet("s3a://f-mba-nyc-dataset/dw/taxi_zone_lookup.parquet")
             .withColumn("pk_location", monotonically_increasing_id())
    )

    write_to_dw(df=df_location, spark=spark, table_name=table_name)

    # df = (
    #     df.join(df_location.withColumnRenamed("LocationID", "PUlocationID"), on="PUlocationID", how="left")
    #       .withColumnRenamed("pk_location", "fk_pickup_location")
    #       .withColumnRenamed("Borough", "pickup_borough")
    #       .withColumnRenamed("Zone", "pickup_zone")
    #       .withColumnRenamed("service_zone", "pickup_service_zone")
    # )

    # df = (
    #     df.join(df_location.withColumnRenamed("PUlocationID", "DOlocationID"), on="DOlocationID", how="left")
    #       .withColumnRenamed("pk_location", "fk_dropoff_location")
    #       .withColumnRenamed("Borough", "dropoff_borough")
    #       .withColumnRenamed("Zone", "dropoff_zone")
    #       .withColumnRenamed("service_zone", "dropoff_service_zone")
    # )

    return df



def create_dimensions_and_fact(df: DataFrame, spark: SparkSession):
    df = df.withColumn("trip_id", monotonically_increasing_id())
    df = df.withColumn("pickup_date", to_date("pickup_datetime"))
    df = df.withColumn("fk_date", date_format("pickup_date", "yyyyMMdd").cast("int"))

    df_vendor = create_df_vendor(df, spark)
    df_payment_type = create_df_payment_type(df, spark)
    df_ratecode = create_df_ratecode(df, spark)
    df_location = create_df_location(df, spark)

    df = df.withColumnRenamed("service_type", "description")

    fact_taxi_trip = df.select(
        "trip_id", "pickup_datetime", "dropoff_datetime", "passenger_count",
        "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
        "total_amount", "fk_payment_type", "fk_ratecode", "fk_vendor", "fk_location",
        "fk_date", "description"
    )
    write_to_dw(df=fact_taxi_trip, spark=spark, table_name="fact_taxi_trip")


    
    # tables = {
    #     "dim_date": dim_date,
    #     "dim_vendor": dim_vendor,
    #     "dim_payment_type": dim_payment_type,
    #     "dim_ratecode": dim_ratecode,
    #     "dim_location": dim_location,
    #     "fact_taxi_trip": fact_taxi_trip
    # }

    # for name, df in tables.items():
    #     df.write.mode("overwrite").parquet(f"{DW_PATH}/{name}")
    #     df.write \
    #         .format("jdbc") \
    #         .option("url", RDS_JDBC_URL) \
    #         .option("dbtable", name) \
    #         .option("user", RDS_USER) \
    #         .option("password", RDS_PASSWORD) \
    #         .option("driver", "com.mysql.cj.jdbc.Driver") \
    #         .mode("overwrite") \
    #         .save()
            
def write_to_dw(df: DataFrame, spark: SparkSession, table_name):
    df.write.mode("overwrite").parquet(f"{DW_PATH}/{table_name}")
    df.write \
        .format("jdbc") \
        .option("url", RDS_JDBC_URL) \
        .option("dbtable", table_name) \
        .option("user", RDS_USER) \
        .option("password", RDS_PASSWORD) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("overwrite") \
        .save()

def main():
    spark = create_spark_session("NYC Taxi Load to DW and RDS")
    dfs = load_dataframes(spark)
    
    if not dfs:
        print("Nenhum dado carregado.")
        return
    df_union = normalize_columns(dfs[0])
    
    for df in dfs[1:]:
        # df_union.show(10)
        df_union = df_union.unionByName(normalize_columns(df), allowMissingColumns=True)
    # df_union.select('service_type').distinct().show()

    # ==================================================
    # Dimensions already created
    # -------
    # create_df_payment_type(df_union, spark)
    # create_df_vendor(df_union, spark)
    # create_df_date(df_union, spark)
    # create_df_service_type(df_union, spark)
    # create_df_ratecode(df_union, spark)
    # create_df_location(df_union, spark) # Reading from a parquet stored in Bucket S3
    # ==================================================
    
    # create_dimensions_and_fact(df_union, spark)
    
    # df_yellow = spark.read.option("basePath", base_path).parquet(f"{base_path}/*/*/*.parquet")
    
    # for service in SERVICE_TYPES:
    #     base_path = f"{TRUSTED_PATH}/{service}"
    #     try:
    #         df = spark.read.option("basePath", base_path).parquet(f"{base_path}/*/*/*.parquet")

    #         df = df.withColumn("service_type", lit(service))
    #         # df.show(10)
    #         df2 = normalize_columns(df)
    #         print('Pós normalize_columns')
    #         df2.show(10)
    #     except:
    #         print(f"Nenhum dado encontrado para {service}")
    spark.stop()

if __name__ == "__main__":
    main()
