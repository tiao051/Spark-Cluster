"""
Lab 3: Reddit Antiwork Dataset Analysis

Analyzes comments and posts from the r/antiwork subreddit.
Performs temporal analysis on comments and identifies high-engagement posts.

Input:
  - hdfs://namenode:9000/input/the-antiwork-subreddit-dataset-comments.csv
  - hdfs://namenode:9000/input/the-antiwork-subreddit-dataset-posts.csv

Output:
  - Monthly comment statistics
  - Top 20 high-engagement posts by score
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, month, year, desc


HDFS_BASE_PATH = "hdfs://namenode:9000/input"


def analyze_comments(spark):
    """
    Analyze comments from the antiwork subreddit.
    
    Args:
        spark: SparkSession instance
    """
    print("\n" + "="*60)
    print("COMMENT ANALYSIS")
    print("="*60)
    
    # Load comments
    print("[INFO] Loading comments data...")
    df_comments = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv(f"{HDFS_BASE_PATH}/the-antiwork-subreddit-dataset-comments.csv")
    
    # Data cleaning: remove deleted comments and null values
    clean_comments = df_comments.filter(
        (col("body") != "[deleted]") & (col("body").isNotNull())
    )
    
    # Temporal analysis: group by month and year
    temporal_comments = clean_comments.withColumn(
        "date", from_unixtime(col("created_utc"))
    ).withColumn(
        "month", month("date")
    ).withColumn(
        "year", year("date")
    )
    
    print("\nMonthly Comment Distribution:")
    temporal_comments.groupBy("year", "month") \
        .count() \
        .orderBy("year", "month") \
        .show(50, truncate=False)


def analyze_posts(spark):
    """
    Analyze posts from the antiwork subreddit.
    
    Args:
        spark: SparkSession instance
    """
    print("\n" + "="*60)
    print("POST ANALYSIS")
    print("="*60)
    
    # Load posts
    print("[INFO] Loading posts data...")
    df_posts = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv(f"{HDFS_BASE_PATH}/the-antiwork-subreddit-dataset-posts.csv")
    
    print("\nTop 20 High-Engagement Posts (by score):")
    df_posts.select("title", "score", "subreddit") \
        .orderBy(desc("score")) \
        .show(20, truncate=50)


def main():
    """
    Main execution function for Reddit data analysis.
    """
    spark = SparkSession.builder \
        .appName("Lab3_Reddit") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        analyze_comments(spark)
        analyze_posts(spark)
        
    except Exception as e:
        print(f"[ERROR] {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()