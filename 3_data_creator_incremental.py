from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


spark = SparkSession.builder\
    .appName("app")\
    .master("local")\
    .config("spark.executor.memory", "12g") \
    .getOrCreate()


schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("nome", StringType(), True),
    StructField("idade", IntegerType(), True)
])


data = [
        (8, "Rodrigo", 8),
        (9, "Rafael", 9),
        (12, "Cristiano", 40),
        (13, "Robert", 55),
        (14, "Gabriela", 32),
        (15, "Mariana", 18)
        ]


df = spark.createDataFrame(data, schema)


parquet_path = "./landing/incremental_2023-08-18"
df.write.parquet(parquet_path)