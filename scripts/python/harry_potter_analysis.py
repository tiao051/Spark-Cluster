from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lower, split, regexp_replace, desc

# 1. Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("HarryPotterWordCount") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("--- Đang đọc dữ liệu từ HDFS ---")

# --- SỬA LỖI Ở ĐÂY ---
# Thay vì "hdfs:///..." ta phải chỉ định rõ "hdfs://namenode:9000/..."
# "namenode" là tên container, "9000" là cổng giao tiếp nội bộ của HDFS
df = spark.read.text("hdfs://namenode:8020/harrypotter.txt")

# 3. Xử lý dữ liệu (Transformation)
words_df = df.select(explode(split(col("value"), "\\s+")).alias("word")) \
    .withColumn("word", lower(col("word"))) \
    .withColumn("word", regexp_replace(col("word"), "[^a-z0-9]", "")) \
    .filter(col("word") != "")

# Thực hiện đếm
word_counts = words_df.groupBy("word").count()

print("\n" + "="*40)
print("TOP 20 TỪ XUẤT HIỆN NHIỀU NHẤT")
print("="*40)
word_counts.orderBy(col("count").desc()).show(20, truncate=False)

# 4. Đếm các nhân vật cụ thể
target_words = ["harry", "ron", "hermione", "malfoy", "snape", "dumbledore"]
print("\n" + "="*40)
print(f"THỐNG KÊ CÁC NHÂN VẬT: {target_words}")

target_counts = word_counts.filter(col("word").isin(target_words)) \
                           .orderBy(col("count").desc())

target_counts.show()

spark.stop()