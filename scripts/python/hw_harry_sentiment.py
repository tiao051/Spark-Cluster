"""
Harry Potter Sentiment Analysis

Analyzes sentiment polarity in the Harry Potter text dataset.
Computes average sentiment scores for each character and tracks
sentiment evolution throughout the narrative.

Requirements:
  - textblob
  - pyspark

Input:
  - hdfs://namenode:9000/input/harrypotter.txt

Output:
  - Character sentiment rankings
  - Sentiment timeline for Harry Potter character
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, split, explode, lower, udf, avg,
    monotonically_increasing_id
)
from pyspark.sql.types import FloatType, StringType, ArrayType
from textblob import TextBlob


TARGET_CHARACTERS = ["harry", "ron", "hermione", "malfoy", "snape", "dumbledore"]
CHUNK_SIZE = 500


def get_sentiment_score(text: str) -> float:
    """
    Calculate sentiment polarity score for text.
    
    Args:
        text: Text string to analyze
        
    Returns:
        Float between -1.0 (negative) and 1.0 (positive)
    """
    try:
        return float(TextBlob(text).sentiment.polarity)
    except Exception:
        return 0.0


def identify_characters(text: str) -> list:
    """
    Identify characters present in text.
    
    Args:
        text: Text string to search
        
    Returns:
        List of character names found in text
    """
    return [c for c in TARGET_CHARACTERS if c in text]


def analyze_character_sentiment(spark, sent_df):
    """
    Rank characters by average sentiment score.
    
    Args:
        spark: SparkSession instance
        sent_df: DataFrame with sentiment scores
    """
    print("\n" + "="*60)
    print("CHARACTER SENTIMENT RANKING")
    print("(Most Positive → Most Negative)")
    print("="*60)
    
    chars_udf = udf(identify_characters, ArrayType(StringType()))
    
    char_sentiments = sent_df.withColumn(
        "character", explode(chars_udf(col("text")))
    )
    
    ranking = char_sentiments.groupBy("character") \
        .agg(avg("sentiment").alias("avg_sentiment")) \
        .orderBy(col("avg_sentiment").desc())
    
    ranking.show(truncate=False)


def analyze_character_timeline(sent_df):
    """
    Track sentiment evolution for Harry Potter character.
    
    Args:
        sent_df: DataFrame with sentiment scores and temporal info
    """
    print("\n" + "="*60)
    print("HARRY POTTER SENTIMENT TIMELINE")
    print(f"(Grouped by {CHUNK_SIZE} sentences)")
    print("="*60)
    
    harry_timeline = sent_df.filter(col("text").contains("harry")) \
        .withColumn("chunk", (col("id") / CHUNK_SIZE).cast("int")) \
        .groupBy("chunk") \
        .agg(avg("sentiment").alias("avg_sentiment")) \
        .orderBy("chunk")
    
    print("\nChunk | Average Sentiment")
    print("-" * 30)
    harry_timeline.show(20, truncate=False)


def main():
    """
    Main execution function for sentiment analysis.
    """
    spark = SparkSession.builder \
        .appName("HW_Harry_Sentiment") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        print("[INFO] Reading text data from HDFS...")
        raw_data = spark.read.text("hdfs://namenode:9000/input/harrypotter.txt")
        
        # Sentence tokenization
        print("[INFO] Tokenizing sentences...")
        sentences = raw_data.select(
            explode(split(col("value"), "[.?!]")).alias("text")
        ).withColumn("id", monotonically_increasing_id())
        
        sentences = sentences.filter(col("text") != "") \
            .withColumn("text", lower(col("text")))
        
        # Sentiment analysis
        print("[INFO] Computing sentiment scores...")
        sentiment_udf = udf(get_sentiment_score, FloatType())
        sent_df = sentences.withColumn("sentiment", sentiment_udf(col("text")))
        
        # Analysis
        analyze_character_sentiment(spark, sent_df)
        analyze_character_timeline(sent_df)
        
    except Exception as e:
        print(f"[ERROR] {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()