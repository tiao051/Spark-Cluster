"""
Lab 4: Spam Message Classifier

Builds a machine learning pipeline to classify messages as spam or ham (not spam).
Uses Logistic Regression with TF-IDF features.

Requirements:
  - pyspark

Input:
  - /dataset/week4/spam_messages_train.csv
  - /dataset/week4/spam_messages_test.csv

Output:
  - Model accuracy score
  - Sample predictions
"""

import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Configuration
TRAIN_PATH = "/dataset/week4/spam_messages_train.csv"
TEST_PATH = "/dataset/week4/spam_messages_test.csv"
TEXT_COL = "message"
LABEL_COL = "label"


def load_data(spark: SparkSession, path: str) -> DataFrame:
    """
    Load CSV data from the specified path.
    
    Args:
        spark: SparkSession instance
        path: Path to the CSV file
        
    Returns:
        DataFrame containing the loaded data
    """
    try:
        print(f"[INFO] Loading data from: {path}")
        df = spark.read.csv(path, header=True, inferSchema=True)
        return df
    except Exception as e:
        print(f"[ERROR] Failed to load data from {path}. Check volume mapping.")
        print(f"[ERROR] Details: {str(e)}")
        sys.exit(1)


def validate_data(df: DataFrame, required_cols: list) -> DataFrame:
    """
    Validate that required columns exist and remove null values.
    
    Args:
        df: Input DataFrame
        required_cols: List of column names that must exist
        
    Returns:
        Cleaned DataFrame
    """
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"[ERROR] Missing required columns: {missing_cols}")
        print(f"[INFO] Available columns: {df.columns}")
        sys.exit(1)
        
    # Remove rows with null values in critical columns
    return df.na.drop(subset=required_cols)


def build_pipeline(text_col: str, label_col: str) -> Pipeline:
    """
    Construct the ML pipeline for text classification.
    
    Args:
        text_col: Name of the column containing text
        label_col: Name of the column containing labels
        
    Returns:
        Configured Pipeline object
    """
    # 1. Convert string labels to indices
    indexer = StringIndexer(inputCol=label_col, outputCol="label_index")
    
    # 2. Tokenize text into words
    tokenizer = RegexTokenizer(inputCol=text_col, outputCol="words", pattern="\\W")
    
    # 3. Remove stop words
    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    
    # 4. Convert words to feature vectors (HashingTF)
    hashingTF = HashingTF(inputCol="filtered", outputCol="features", numFeatures=10000)
    
    # 5. Logistic Regression Classifier
    lr = LogisticRegression(maxIter=20, regParam=0.1, labelCol="label_index", featuresCol="features")
    
    return Pipeline(stages=[indexer, tokenizer, remover, hashingTF, lr])


def train_and_evaluate(pipeline: Pipeline, train_data: DataFrame, test_data: DataFrame):
    """
    Train the model and evaluate its performance.
    
    Args:
        pipeline: ML Pipeline
        train_data: Training DataFrame
        test_data: Testing DataFrame
    """
    print("[INFO] Training model...")
    model = pipeline.fit(train_data)
    
    print("[INFO] Evaluating on test set...")
    predictions = model.transform(test_data)
    
    evaluator = MulticlassClassificationEvaluator(
        labelCol="label_index", 
        predictionCol="prediction", 
        metricName="accuracy"
    )
    accuracy = evaluator.evaluate(predictions)
    
    print("\n" + "="*40)
    print(f"MODEL ACCURACY: {accuracy * 100:.2f}%")
    print("="*40)
    
    print("\n[INFO] Sample Predictions:")
    predictions.select(TEXT_COL, LABEL_COL, "prediction").show(5, truncate=False)


def main():
    """
    Main execution function.
    """
    spark = SparkSession.builder \
        .appName("Week4_SpamClassifier") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Load Data
        train_df = load_data(spark, TRAIN_PATH)
        test_df = load_data(spark, TEST_PATH)
        
        # Validate Data
        required_cols = [TEXT_COL, LABEL_COL]
        train_df = validate_data(train_df, required_cols)
        test_df = validate_data(test_df, required_cols)
        
        # Build Pipeline
        pipeline = build_pipeline(TEXT_COL, LABEL_COL)
        
        # Train and Evaluate
        train_and_evaluate(pipeline, train_df, test_df)
        
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()