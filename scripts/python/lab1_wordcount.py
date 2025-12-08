"""
Lab 1: Word Count Analysis

Analyzes word frequency in the Harry Potter text dataset.
Performs text processing and identifies top 20 most frequent words
and counts character name occurrences.

Input:
  - hdfs://namenode:9000/input/harrypotter.txt

Output:
  - Top 20 most frequent words
  - Character frequency statistics
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def main():
    """
    Main execution function for word count analysis.
    """
    spark = SparkSession.builder \
        .appName("Lab1_WordCount") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Load text data
        print("[INFO] Reading data from HDFS...")
        df = spark.read.text("hdfs://namenode:9000/input/harrypotter.txt")
        
        # Text processing: tokenize and normalize
        words_df = df.select(
            F.explode(F.split(F.lower(F.col("value")), "\\W+")).alias("word")
        ).filter(F.col("word") != "")
        
        # Top 20 most frequent words
        print("\n" + "="*50)
        print("TOP 20 MOST FREQUENT WORDS")
        print("="*50)
        word_counts = words_df.groupBy("word").count()
        word_counts.orderBy(F.desc("count")).show(20, truncate=False)
        
        # Character frequency analysis
        targets = ["harry", "ron", "hermione", "malfoy", "snape", "dumbledore"]
        print("\n" + "="*50)
        print("CHARACTER FREQUENCY STATISTICS")
        print("="*50)
        char_counts = words_df.filter(F.col("word").isin(targets)).groupBy("word").count()
        char_counts.orderBy(F.desc("count")).show()
        
    except Exception as e:
        print(f"[ERROR] {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()