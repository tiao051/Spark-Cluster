"""
Character Interaction Analysis

Analyzes character co-occurrence patterns in the Harry Potter text.
Identifies pairs of characters that appear together in sentences
and ranks them by frequency of interaction.

Input:
  - hdfs://namenode:9000/input/harrypotter.txt

Output:
  - Character interaction pairs ranked by frequency
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, lower, udf, desc
from pyspark.sql.types import ArrayType, StringType
import itertools


CHARACTER_LIST = ["harry", "ron", "hermione", "malfoy", "snape", "dumbledore"]


def extract_character_pairs(text: str) -> list:
    """
    Extract character pairs that co-occur in the same sentence.
    
    Args:
        text: Text string to search for characters
        
    Returns:
        List of character pair combinations found in text
    """
    found_chars = [c for c in CHARACTER_LIST if c in text]
    if len(found_chars) < 2:
        return []
    return [str(sorted(pair)) for pair in itertools.combinations(found_chars, 2)]


def main():
    """
    Main execution function for character interaction analysis.
    """
    spark = SparkSession.builder \
        .appName("Lab2_Interaction") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        print("[INFO] Reading data from HDFS...")
        raw_data = spark.read.text("hdfs://namenode:9000/input/harrypotter.txt")
        
        print("[INFO] Tokenizing sentences...")
        sentences = raw_data.select(
            explode(split(col("value"), "[.?!]")).alias("text")
        ).filter(col("text") != "").withColumn("text", lower(col("text")))
        
        pair_udf = udf(extract_character_pairs, ArrayType(StringType()))
        
        print("[INFO] Extracting and counting character pairs...")
        df_pairs = sentences.withColumn("pairs", pair_udf(col("text")))
        result = df_pairs.select(explode(col("pairs")).alias("pair")) \
                         .groupBy("pair").count() \
                         .orderBy(desc("count"))
        
        print("\n" + "="*60)
        print("CHARACTER INTERACTION PAIRS")
        print("="*60)
        result.show(20, truncate=False)
        
    except Exception as e:
        print(f"[ERROR] {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()