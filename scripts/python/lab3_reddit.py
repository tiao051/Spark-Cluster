from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, month, year, desc

spark = SparkSession.builder.appName("Lab3_Reddit").getOrCreate()

# --- XỬ LÝ COMMENT ---
print("--- ANALYZING COMMENTS ---")
# Đọc file Comments
df_cmt = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("hdfs://namenode:9000/input/the-antiwork-subreddit-dataset-comments.csv")

# Yêu cầu 1: Lọc bỏ bình luận [deleted] [cite: 112]
clean_cmt = df_cmt.filter(col("body") != "[deleted]").filter(col("body").isNotNull())

# Yêu cầu 3 (một phần): Tổng comment theo tháng [cite: 119]
time_cmt = clean_cmt.withColumn("date", from_unixtime(col("created_utc"))) \
                    .withColumn("month", month("date")) \
                    .withColumn("year", year("date"))

print("Tong so comment theo thang:")
time_cmt.groupBy("year", "month").count().orderBy("year", "month").show()


# --- XỬ LÝ POSTS ---
print("--- ANALYZING POSTS ---")
# Đọc file Posts
df_post = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("hdfs://namenode:9000/input/the-antiwork-subreddit-dataset-posts.csv")

# Yêu cầu 2: Top 5% bài đăng có tương tác cao nhất (dựa trên score) [cite: 116]
# Lấy Top 20 bài để minh họa
print("Top 20 bai dang co score cao nhat:")
df_post.select("title", "score", "subreddit").orderBy(desc("score")).show(20, truncate=50)

spark.stop()