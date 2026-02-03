from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PySparkTest") \
    .getOrCreate()

print("🔥 PySpark Session Created Successfully!")

# Basic RDD test
data = [1, 2, 3, 4, 5]
rdd = spark.sparkContext.parallelize(data)
result = rdd.map(lambda x: x * 10).collect()

print("RDD Test Output:", result)

spark.stop()
