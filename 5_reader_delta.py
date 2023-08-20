from pyspark.sql import SparkSession
from delta import *


spark = SparkSession.builder\
    .appName("app")\
    .master("local")\
    .config("spark.executor.memory", "12g") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()


print("Arquivo full pre_delta")
arquivo_full_pre_delta = spark.read.format("parquet").load("landing/full_parquet")
arquivo_full_pre_delta.show()
print("*********************************")
print("*********************************\n")


print("Arquivo incremental")
arquivo_full_pre_delta = spark.read.format("parquet").load("landing/incremental_2023-08-18")
arquivo_full_pre_delta.show()
print("*********************************")
print("*********************************\n")


print("Arquivo delta pos incremental")
delta_full_pos_incremental = spark.read.format("delta").load("bronze/delta_table")
delta_full_pos_incremental.show()
print("*********************************\n")