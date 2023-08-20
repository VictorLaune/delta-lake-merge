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
        (1, "Alice", 25),
        (2, "Bob", 30),
        (3, "Carol", 28),
        (4, "Taina", 21),
        (5, "Manoelle", 25),
        (6, "Victor", 25),
        (7, "Pedro", 23),
        (8, "Rodrigo", 22),
        (9, "Rafael", 31),
        (10, "Joao", 26),
        (11, "Daniel", 34)        
]


df = spark.createDataFrame(data, schema)


parquet_path = "./landing/full_parquet"


df.write.parquet(parquet_path)