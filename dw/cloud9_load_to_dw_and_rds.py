from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, year, month, dayofmonth, dayofweek, to_date, lit, monotonically_increasing_id, when, date_format, expr, row_number
)
from pyspark.sql.types import LongType, DoubleType
from pyspark.sql.window import Window
from datetime import datetime, timedelta
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
        .config("spark.local.dir", "/home/ec2-user/tmp_spark") \
        .getOrCreate()
    return spark

def load_dataframes(spark: SparkSession, year = '*', month = '*') -> DataFrame:
    dfs = []
    for service in SERVICE_TYPES:
        base_path = f"{TRUSTED_PATH}/{service}"
        try:
            df = spark.read.option("basePath", base_path).parquet(f"{base_path}/*/{month}/*.parquet")
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
        "PULocationID": "id_location_pickup",
        "DOLocationID": "id_location_dropoff",
        "RatecodeID": "id_rate",
        "VendorID": "id_vendor",
        "originating_base_num": "originating_base_number",
        "Affiliated_base_number": "affiliated_base_number",
        "payment_type": "id_payment_type"
    }
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    cast_map = {
        "id_vendor": LongType(),
        "id_rate": LongType(),
        "id_payment_type": LongType(),
        "trip_type": LongType(),
        "id_location_pickup": DoubleType(),
        "id_location_dropoff": DoubleType()
    }
    for col_name, dtype in cast_map.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(dtype))
    return df

def create_dim_service_type(df: DataFrame, spark: SparkSession, table_name: str = 'dim_service_type'):
    # Tabela de dimensão com descrição simples

    df_service_type = df.select("service_type") \
        .dropDuplicates() \
        .withColumn("sk_service_type", monotonically_increasing_id()) \

    # Adicionando a coluna de descrição completa
    df_service_type = df_service_type.withColumn(
        "description",
        when(df_service_type.service_type == "yellowTaxi", "Yellow Taxi")
        .when(df_service_type.service_type == "greenTaxi", "Green Taxi")
        .when(df_service_type.service_type == "forHireVehicle", "For-Hire Vehicle (FHV)")
        .when(df_service_type.service_type == "highVolumeForHire", "High-Volume For-Hire Vehicle")
        .otherwise("Desconhecido")
    )

    # Adicionando coluna 'category'
    df_service_type = df_service_type.withColumn(
        "category",
        when(df_service_type.service_type.isin("yellowTaxi", "greenTaxi"), "Taxi")
        .when(df_service_type.service_type.isin("forHireVehicle", "highVolumeForHire"), "For-Hire")
        .otherwise("Desconhecido")
    )

    # Adicionando coluna 'regulation_body'
    df_service_type = df_service_type.withColumn(
        "regulation_body",
        when(df_service_type.service_type.isin("yellowTaxi", "greenTaxi"), "TLC")
        .when(df_service_type.service_type.isin("forHireVehicle", "highVolumeForHire"), "Empresas Privadas")
        .otherwise("Desconhecido")
    )

    df = df.join(df_service_type.select("sk_service_type", "service_type"), on="service_type", how="left") \
            .withColumnRenamed("sk_service_type", "fk_service_type")

    # print("df")
    # df.show(1, vertical = True)


    df_service_type = df_service_type.withColumnRenamed("service_type", "id_service_type")

    #Reorder
    df_service_type = df_service_type.select("sk_service_type", "id_service_type", "description", "category", "regulation_body")

    # print("dim_service_type")
    # df_service_type.show()



    # print("Loading dim_service_type to DW and RDS")
    # write_to_dw(df=df_service_type, spark=spark, table_name=table_name)

    return df



    # return df.join(df_service_type, on="description", how="left") \
        # .withColumnRenamed("pk_service_type", "fk_service_type")

def create_dim_date(spark: SparkSession, table_name: str = 'dim_date'):

    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)
    date_list = [(start_date + timedelta(days=x),) for x in range((end_date - start_date).days + 1)]

    # Cria DataFrame com coluna 'date'
    df_date = spark.createDataFrame(date_list, ["date"])

    # Adiciona campos derivados
    df_date = df_date.withColumn("sk_date", monotonically_increasing_id()) \
        .withColumn("day", dayofmonth("date")) \
        .withColumn("month", month("date")) \
        .withColumn("year", year("date")) \
        .withColumn("day_of_week", date_format("date", "EEEE")) \
        .withColumn("week_day", dayofweek("date"))

    df_date = df_date.select("sk_date", "date", "day", "month", "year", "day_of_week", "week_day")

    # df_date.show(10)


    print("Loading dim_date to DW and RDS")
    write_to_dw(df=df_date, spark=spark, table_name=table_name)


    return

    #=====RASCUNHO

    # df_date = df.select("pickup_date").distinct() \
    #     .withColumn("pk_date", date_format(col("pickup_date"), "yyyyMMdd")) \
    #     .withColumn("sk_date", monotonically_increasing_id()) \
    #     .withColumn("day", dayofmonth("pickup_date")) \
    #     .withColumn("month", month("pickup_date")) \
    #     .withColumn("year", year("pickup_date")) \
    #     .withColumn("day_of_week", dayofweek("pickup_date"))


    # df = df.join(df_date.select("sk_date", "pickup_date"), on="pickup_date", how="left")  \
    #     .withColumnRenamed("sk_date", "fk_date")

    # # print("df")
    # # df.show(1, vertical = True)


    # df_date.withColumnRenamed("pickup_date", "date")


    # print("df_date")
    # df_date.show()



    # return df


def create_dim_vendor(df: DataFrame, spark: SparkSession, table_name: str = 'dim_vendor') -> DataFrame:

    df_vendor = df.select("id_vendor") \
        .dropna() \
        .dropDuplicates() \
        .withColumn("sk_vendor", monotonically_increasing_id()) \
        .withColumn(
              "vendor_description",
              when(col("id_vendor") == "1", "Creative Mobile Technologies, LLC")
              .when(col("id_vendor") == "2", "Curb Mobility, LLC")
              .when(col("id_vendor") == "6", "Myle Technologies Inc")
              .when(col("id_vendor") == "7", "Helix")
              .otherwise("Unknown")
          )

    #reorder
    df_vendor = df_vendor.select("sk_vendor", "id_vendor", "vendor_description")

    # print("dim_vendor")
    # df_vendor.show()

    # print("Loading dim_vendor to DW and RDS")
    # write_to_dw(df=df_vendor, spark=spark, table_name=table_name)

    # Realiza o join usando os nomes corretos
    df = df.join(df_vendor.select("sk_vendor", "id_vendor"), on="id_vendor", how="left") \
             .withColumnRenamed("sk_vendor", "fk_vendor")

    # print("df")
    # df.show(1, vertical = True)

    return df


def create_dim_payment_type(df: DataFrame, spark: SparkSession, table_name: str = 'dim_payment_type') -> DataFrame:
    df_payment_type = df.select("id_payment_type") \
          .dropna() \
          .dropDuplicates() \
          .withColumn("sk_payment_type", monotonically_increasing_id()) \
          .withColumn(
              "payment_type_description",
              when(col("id_payment_type") == "0", "Flex Fare trip")
              .when(col("id_payment_type") == "1", "Credit card")
              .when(col("id_payment_type") == "2", "Cash")
              .when(col("id_payment_type") == "3", "No charge")
              .when(col("id_payment_type") == "4", "Dispute")
              .when(col("id_payment_type") == "5", "Unknown")
              .when(col("id_payment_type") == "6", "Voided trip")
              .otherwise("Other")
          )

    df_payment_type=df_payment_type.select("sk_payment_type", "id_payment_type", "payment_type_description")

    # print("dim_payment_type")
    # df_payment_type.show()


    # print("Loading dim_payment_type to DW and RDS")
    # write_to_dw(df=df_payment_type, spark=spark, table_name=table_name)

    df = df.join(df_payment_type.select("id_payment_type","sk_payment_type"), on="id_payment_type", how="left") \
          .withColumnRenamed("sk_payment_type", "fk_payment_type")

    # print("df")
    # df.show(1, vertical = True)



    return df

def create_dim_rate(df: DataFrame, spark: SparkSession, table_name: str = 'dim_rate') -> DataFrame:
    df_rate = (
        df.select("id_rate") \
          .dropna() \
          .dropDuplicates() \
          .withColumn("sk_ratecode", monotonically_increasing_id()) \
          .withColumn(
              "ratecode_description",
              when(col("id_rate") == "1", "Standard rate")
              .when(col("id_rate") == "2", "JFK")
              .when(col("id_rate") == "3", "Newark")
              .when(col("id_rate") == "4", "Nassau or Westchester")
              .when(col("id_rate") == "5", "Negotiated fare")
              .when(col("id_rate") == "6", "Group ride")
              .when(col("id_rate") == "99", "Null/unknown")
              .otherwise("Other")
          )
    )

    df_rate= df_rate.select("sk_ratecode", "id_rate", "ratecode_description")
    # print("dim_rate")
    # df_rate.show()


    # print("Loading dim_rate to DW and RDS")
    # write_to_dw(df=df_rate, spark=spark, table_name=table_name)

    df = df.join(df_rate.select("sk_ratecode","id_rate"), on="id_rate", how="left") \
          .withColumnRenamed("sk_ratecode", "fk_ratecode")
    # print("df")
    # df.show(1, vertical = True)


    return df


def create_dim_location(spark: SparkSession, table_name: str = 'dim_location') -> DataFrame:
    df_location = (
        spark.read.parquet("s3a://f-mba-nyc-dataset/dw/taxi_zone_lookup.parquet")
             .withColumn("sk_location", monotonically_increasing_id())
    )

    df_location = df_location.toDF(*map(str.lower, df_location.columns)) \
             .withColumnRenamed("locationid", "id_location")

    df_location = df_location.select("sk_location", "id_location", "borough", "zone", "service_zone")

    # print("dim_location")
    # df_location.show()


    print("Loading dim_location to DW and RDS")

    write_to_dw(df=df_location, spark=spark, table_name=table_name)

    return
    #=====RASCUNHO



    # df = (
    #     df.join(df_location.withColumnRenamed("LocationID", "PUlocationID"), on="PUlocationID", how="left")
    #       .withColumnRenamed("pk_location", "fk_id_location_pickup")
    #       .withColumnRenamed("Borough", "pickup_borough")
    #       .withColumnRenamed("Zone", "pickup_zone")
    #       .withColumnRenamed("service_zone", "pickup_service_zone")
    # )

    # df = (
    #     df.join(df_location.withColumnRenamed("PUlocationID", "DOlocationID"), on="DOlocationID", how="left")
    #       .withColumnRenamed("pk_location", "fk_id_location_dropoff")
    #       .withColumnRenamed("Borough", "dropoff_borough")
    #       .withColumnRenamed("Zone", "dropoff_zone")
    #       .withColumnRenamed("service_zone", "dropoff_service_zone")
    # )

    # print(df)
    # df.show(1, vertical = True)


    # return df

def create_fact_taxi_trip(df: DataFrame, spark: SparkSession, table_name: str = 'fact_taxi_trip') -> DataFrame:

    df_fact_taxi_trip = df.select("fk_payment_type", "fk_ratecode", "fk_vendor","pickup_datetime", "dropoff_datetime",
                            "id_location_pickup", "id_location_dropoff", "passenger_count",
                            "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
                            "total_amount"
                        ) \
                        .withColumn("sk_trip", monotonically_increasing_id()) \
                        .withColumn("pickup_date", to_date("pickup_datetime")) \
                        .withColumn("dropoff_date", to_date("dropoff_datetime")) \
                        .withColumnRenamed("service_type", "fk_service_type")


    # print("fact_taxi_trip")
    # df_fact_taxi_trip.show(5)

    df_fact_taxi_trip = df_fact_taxi_trip.select("sk_trip", "fk_payment_type", "fk_ratecode", "fk_vendor","pickup_datetime",
                            "pickup_date", "dropoff_datetime", "dropoff_date",
                            "id_location_pickup", "id_location_dropoff", "passenger_count",
                            "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
                            "total_amount")

    df_fact_taxi_trip_sample = df_fact_taxi_trip.sample(fraction=0.08).limit(10000)
    print("Loading fact_taxi_trip to DW and RDS")
    write_to_dw(df=df_fact_taxi_trip_sample, spark=spark, table_name=table_name)

    return df

def create_dimensions_and_fact(df: DataFrame, spark: SparkSession):

    # df = df.withColumn("fk_date", date_format("pickup_date", "yyyyMMdd").cast("int"))

    df = create_dim_vendor(df, spark)
    df = create_dim_payment_type(df, spark)
    df = create_dim_rate(df, spark)
    df = create_dim_service_type(df, spark)
    df = create_fact_taxi_trip(df, spark)

    # create_dim_location(spark)
    # create_dim_date(spark)


def write_to_dw(df: DataFrame, spark: SparkSession, table_name, partition_s3=4, partition_rds=1):
    df.coalesce(partition_s3).write.mode("overwrite").parquet(f"{DW_PATH}/{table_name}")
    df.coalesce(partition_rds).write \
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
    dfs = load_dataframes(spark, month = "month=10")

    if not dfs:
        print("Nenhum dado carregado.")
        return
    df_union = normalize_columns(dfs[0])

    for df in dfs[1:]:
        # df_union.show(10)
        df_union = df_union.unionByName(normalize_columns(df), allowMissingColumns=True)

    create_dimensions_and_fact(df_union, spark)

    # df_union.select('service_type').distinct().show()

    # ==================================================



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
