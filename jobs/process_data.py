#!/usr/bin/env python3

import argparse
import os
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, when

def process_movie_reviews(spark, input_path, gcp_project_id, gcp_dataset_name, run_id):
    print(f"Processing movie reviews from: {input_path}")
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    print(f"Movie reviews count: {df.count()}")
    df.show(5)
    
    tokenizer = Tokenizer(inputCol="review_str", outputCol="words")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    hashingTF = HashingTF(inputCol="filtered_words", outputCol="rawFeatures", numFeatures=20)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf])
    model = pipeline.fit(df)
    predictions = model.transform(df)
    
    # Dummy classification logic - Create a sentiment column first
    predictions = predictions.withColumn("sentiment", 
                                       when(col("features").isNotNull(), "positive")
                                       .otherwise("neutral"))
    
    predictions = predictions.withColumn("positive_review", 
                                       when(col("sentiment") == "positive", True)
                                       .otherwise(False))
    
    print("Saving movie reviews to BigQuery...")
    predictions.select("cid", "positive_review").write \
        .format("bigquery") \
        .option("table", f"{gcp_project_id}.{gcp_dataset_name}.movie_review") \
        .option("parentProject", gcp_project_id) \
        .option("project", gcp_project_id) \
        .option("writeMethod", "direct") \
        .option("temporaryGcsBucket", f"{gcp_project_id}-temp") \
        .mode("overwrite") \
        .save()
    print("Movie reviews saved successfully!")

def process_user_purchases(spark, input_path, gcp_project_id, gcp_dataset_name, run_id):
    print(f"Processing user purchases from: {input_path}")
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    print(f"User purchases count: {df.count()}")
    df.show(5)
    
    print("Saving user purchases to BigQuery...")
    df.write \
        .format("bigquery") \
        .option("table", f"{gcp_project_id}.{gcp_dataset_name}.user_purchase") \
        .option("parentProject", gcp_project_id) \
        .option("project", gcp_project_id) \
        .option("writeMethod", "direct") \
        .option("temporaryGcsBucket", f"{gcp_project_id}-temp") \
        .mode("overwrite") \
        .save()
    print("User purchases saved successfully!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process user analytics data')
    parser.add_argument("--movie_review_input", required=True, help='Movie review input path')
    parser.add_argument("--user_purchase_input", required=True, help='User purchase input path')
    parser.add_argument("--gcp_project_id", required=True, help='GCP Project ID')
    parser.add_argument("--gcp_dataset_name", required=True, help='GCP Dataset Name')
    parser.add_argument("--run-id", required=True, help='Run ID')
    
    args = parser.parse_args()
    
    print(f"Starting data processing job...")
    print(f"Movie review input: {args.movie_review_input}")
    print(f"User purchase input: {args.user_purchase_input}")
    print(f"GCP Project ID: {args.gcp_project_id}")
    print(f"GCP Dataset Name: {args.gcp_dataset_name}")
    print(f"Run ID: {args.run_id}")
    
    # Set up Google Cloud credentials
    credentials_path = "/opt/spark/sa-key.json"
    if os.path.exists(credentials_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
        print(f"Google Cloud credentials set to: {credentials_path}")
    else:
        print(f"Warning: Credentials file not found at {credentials_path}")
    
    spark = SparkSession.builder \
        .appName("ProcessData") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()
    
    try:
        process_movie_reviews(spark, args.movie_review_input, args.gcp_project_id, args.gcp_dataset_name, args.run_id)
        process_user_purchases(spark, args.user_purchase_input, args.gcp_project_id, args.gcp_dataset_name, args.run_id)
        print("Data processing completed successfully!")
        
    except Exception as e:
        print(f"Error in data processing: {str(e)}")
        raise
    finally:
        spark.stop()