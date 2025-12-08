from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, lower, udf
from pyspark.sql.types import ArrayType, StringType
import itertools

spark = SparkSession.builder.appName("Lab2_Interaction").getOrCreate()

# Đọc dữ liệu
raw_data = spark.read.text("hdfs://namenode:9000/input/harrypotter.txt")

# Yêu cầu 1: Cấu trúc hóa (Tách câu dựa trên dấu chấm) [cite: 92]
sentences = raw_data.select(explode(split(col("value"), "[.?!]")).alias("text"))
sentences = sentences.filter(col("text") != "").withColumn("text", lower(col("text")))

# Danh sách nhân vật [cite: 94]
chars = ["harry", "ron", "hermione", "malfoy", "snape", "dumbledore"]

# Hàm tìm cặp nhân vật cùng xuất hiện
def get_pairs(text):
    found = [c for c in chars if c in text]
    if len(found) < 2: return []
    return [str(sorted(p)) for p in itertools.combinations(found, 2)]

pair_udf = udf(get_pairs, ArrayType(StringType()))

# Yêu cầu 2: Đếm cặp nhân vật [cite: 96, 97]
df_pairs = sentences.withColumn("pairs", pair_udf(col("text")))
result = df_pairs.select(explode(col("pairs")).alias("pair")) \
                 .groupBy("pair").count() \
                 .orderBy(col("count").desc())

print("--- CAP NHAN VAT TUONG TAC ---")
result.show(20, truncate=False)

spark.stop()