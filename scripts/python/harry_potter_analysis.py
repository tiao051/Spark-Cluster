from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lower, split, regexp_replace, desc

spark = SparkSession.builder \
    .appName("HarryPotterWordCount") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read.text("hdfs:///harrypotter.txt")

words_df = df.select(explode(split(col("value"), "\\s+")).alias("word")) \
    .withColumn("word", lower(col("word"))) \
    .withColumn("word", regexp_replace(col("word"), "[^a-z0-9]", "")) \
    .filter(col("word") != "")

word_counts = words_df.groupBy("word").count()

print("TOP 20 TỪ XUẤT HIỆN NHIỀU NHẤT")

word_counts.orderBy(col("count").desc()).show(20, truncate=False)

target_words = ["harry", "ron", "hermione", "malfoy", "snape", "dumbledore"]
print(f"THỐNG KÊ CÁC NHÂN VẬT: {target_words}")

target_counts = word_counts.filter(col("word").isin(target_words)) \
                           .orderBy(col("count").desc())

target_counts.show()

spark.stop()