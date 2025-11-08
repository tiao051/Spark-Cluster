from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("MyApp") \
    .config("spark.driver.log.level", "ERROR") \
    .config("spark.executor.log.level", "ERROR") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
print("Number of Partitions: " + str(rdd.getNumPartitions()))
print("Action: First element: " + str(rdd.first()))
print("Action: RDD converted to Array:")
for item in rdd.collect():
    print(item)
