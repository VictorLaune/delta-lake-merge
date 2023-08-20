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


path_landing = "./landing/full_parquet"
parquet_file = spark.read.format("parquet").load(path_landing)


parquet_file.write.format("delta").save("./bronze/delta_table")