from pyspark.sql import SparkSession

spark = SparkSession.builder()\
    .appName("my app1")\
    .master("local[*]")\
    .getOrCreate()

df = spark.read.csv("/path")

df.withColumn("new",coalesce(col(1)+col(2)))
