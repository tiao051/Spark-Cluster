"""
Reddit Antiwork Sentiment Analysis

Analyzes sentiment trends in r/antiwork subreddit comments over time.
Computes average sentiment polarity scores grouped by month and year.

Requirements:
  - textblob
  - pyspark

Input:
  - hdfs://namenode:9000/input/the-antiwork-subreddit-dataset-comments.csv

Output:
  - Monthly sentiment statistics

"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, month, year, udf, avg, count
from pyspark.sql.types import FloatType
from textblob import TextBlob


HDFS_DATA_PATH = "hdfs://namenode:9000/input"
SAMPLE_FRACTION = 1.0


def get_sentiment_score(text: str) -> float:
    """
    Calculate sentiment polarity score for text.
    
    Args:
        text: Text string to analyze
        
    Returns:
        Float between -1.0 (negative) and 1.0 (positive)
    """
    try:
        return float(TextBlob(str(text)).sentiment.polarity)
    except Exception:
        return 0.0


def main():
    """
    Main execution function for Reddit sentiment analysis.
    """
    spark = SparkSession.builder \
        .appName("HW_Reddit_Sentiment") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        print("[INFO] Reading comments data from HDFS...")
        df_comments = spark.read.option("header", "true") \
            .option("inferSchema", "true") \
            .csv(f"{HDFS_DATA_PATH}/the-antiwork-subreddit-dataset-comments.csv")
        
        print("[INFO] Cleaning and preprocessing data...")
        clean_df = df_comments.filter(
            (col("body") != "[deleted]") & (col("body").isNotNull())
        )
        
        # Optional: sample data for faster processing
        if SAMPLE_FRACTION < 1.0:
            clean_df = clean_df.sample(
                withReplacement=False,
                fraction=SAMPLE_FRACTION
            )
        
        print("[INFO] Extracting temporal features...")
        time_df = clean_df.withColumn(
            "date", from_unixtime(col("created_utc"))
        ).withColumn(
            "year", year("date")
        ).withColumn(
            "month", month("date")
        )
        
        print("[INFO] Computing sentiment scores (this may take time)...")
        sentiment_udf = udf(get_sentiment_score, FloatType())
        df_sentiment = time_df.withColumn("sentiment", sentiment_udf(col("body")))
        
        print("[INFO] Aggregating monthly sentiment statistics...")
        monthly_sentiment = df_sentiment.groupBy("year", "month") \
            .agg(
                avg("sentiment").alias("avg_sentiment"),
                count("sentiment").alias("comment_count")
            ) \
            .orderBy("year", "month")
        
        print("\n" + "="*70)
        print("MONTHLY SENTIMENT STATISTICS")
        print("="*70)
        print("\nYear | Month | Avg Sentiment | Comment Count")
        print("-" * 70)
        monthly_sentiment.show(50, truncate=False)
        
    except Exception as e:
        print(f"[ERROR] {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()