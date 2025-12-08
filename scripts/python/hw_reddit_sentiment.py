"""
Reddit Antiwork Sentiment Analysis

Analyzes sentiment trends in r/antiwork subreddit comments over time.
Computes average sentiment polarity scores grouped by month and year.
Creates visualizations of sentiment evolution and community growth.

Requirements:
  - textblob
  - pyspark
  - matplotlib
  - pandas

Input:
  - hdfs://namenode:9000/input/the-antiwork-subreddit-dataset-comments.csv

Output:
  - Monthly sentiment statistics
  - Sentiment timeline visualization
  - Community growth chart

"""

import subprocess
import sys

# Install required packages
subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "textblob", "matplotlib", "pandas"])

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, month, year, udf, avg, count
from pyspark.sql.types import FloatType
from textblob import TextBlob
import matplotlib.pyplot as plt
import pandas as pd


HDFS_DATA_PATH = "hdfs://namenode:9000/input"
SAMPLE_FRACTION = 0.05


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
        
        # Visualization
        print("\n[INFO] Creating visualizations...")
        visualize_sentiment_trends(monthly_sentiment)
        
    except Exception as e:
        print(f"[ERROR] {str(e)}")
        raise
    
    finally:
        spark.stop()


def visualize_sentiment_trends(monthly_sentiment):
    """
    Create sentiment and growth timeline visualizations.
    
    Args:
        monthly_sentiment: DataFrame with monthly aggregated data
    """
    # Convert to pandas for visualization
    df_pd = monthly_sentiment.toPandas()
    
    # Create year-month column for x-axis
    df_pd['year_month'] = df_pd['year'].astype(str) + '-' + df_pd['month'].astype(str).str.zfill(2)
    
    # Create figure with 2 subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10))
    
    # Plot 1: Sentiment Timeline
    ax1.plot(range(len(df_pd)), df_pd['avg_sentiment'], 
             marker='o', linewidth=2, markersize=4, color='#2E86AB')
    ax1.axhline(y=0, color='red', linestyle='--', alpha=0.5, label='Neutral (0)')
    ax1.fill_between(range(len(df_pd)), df_pd['avg_sentiment'], 0, 
                      where=(df_pd['avg_sentiment'] >= 0), alpha=0.3, color='green', label='Positive')
    ax1.fill_between(range(len(df_pd)), df_pd['avg_sentiment'], 0, 
                      where=(df_pd['avg_sentiment'] < 0), alpha=0.3, color='red', label='Negative')
    
    ax1.set_title('Reddit r/antiwork: Sentiment Timeline', fontsize=14, fontweight='bold')
    ax1.set_ylabel('Average Sentiment Score', fontsize=12)
    ax1.set_xlabel('Time Period', fontsize=12)
    ax1.grid(True, alpha=0.3)
    ax1.legend(loc='best')
    
    # Set x-axis labels (show every Nth label for readability)
    step = max(1, len(df_pd) // 12)
    ax1.set_xticks(range(0, len(df_pd), step))
    ax1.set_xticklabels(df_pd['year_month'].iloc[::step], rotation=45, ha='right')
    
    # Plot 2: Community Growth (Comment Count)
    ax2.bar(range(len(df_pd)), df_pd['comment_count'], 
            color='#A23B72', alpha=0.7, edgecolor='black', linewidth=0.5)
    ax2.set_title('Reddit r/antiwork: Community Activity Growth', fontsize=14, fontweight='bold')
    ax2.set_ylabel('Number of Comments', fontsize=12)
    ax2.set_xlabel('Time Period', fontsize=12)
    ax2.grid(True, alpha=0.3, axis='y')
    
    # Set x-axis labels
    ax2.set_xticks(range(0, len(df_pd), step))
    ax2.set_xticklabels(df_pd['year_month'].iloc[::step], rotation=45, ha='right')
    
    plt.tight_layout()
    
    # Save figure
    output_path = '/tmp/reddit_sentiment_analysis.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"[INFO] Visualization saved to {output_path}")
    
    plt.show()


if __name__ == "__main__":
    main()