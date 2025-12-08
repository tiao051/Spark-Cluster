from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("Lab1_WordCount").getOrCreate()

# Đọc file từ HDFS
df = spark.read.text("hdfs://namenode:9000/input/harrypotter.txt")

# Tách từ, chuẩn hóa chữ thường
words_df = df.select(F.explode(F.split(F.lower(F.col("value")), "\\W+")).alias("word"))
words_df = words_df.filter(words_df.word != "")

# Yêu cầu 3: Top 20 từ xuất hiện nhiều nhất [cite: 86]
print("--- TOP 20 TU PHO BIEN ---")
words_df.groupBy("word").count().orderBy(F.desc("count")).show(20)

# Yêu cầu 4: Đếm các nhân vật cụ thể [cite: 87, 88]
targets = ["harry", "ron", "hermione", "malfoy", "snape", "dumbledore"]
print("--- TAN SUAT NHAN VAT ---")
words_df.filter(F.col("word").isin(targets)).groupBy("word").count().show()

spark.stop()