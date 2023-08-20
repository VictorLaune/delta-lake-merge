from pyspark.sql import SparkSession
from delta import *


spark = SparkSession.builder\
    .appName("app")\
    .master("local")\
    .config("spark.executor.memory", "12g")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()


delta_table = DeltaTable.forPath(spark, './bronze/delta_table')
incremental = spark.read.format("parquet").load("./landing/incremental_2023-08-18")


(
    delta_table.alias("target")
    .merge(incremental.alias("source"), "target.id = source.id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)