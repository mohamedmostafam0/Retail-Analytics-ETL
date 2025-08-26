# pyspark
import argparse

from pyspark.ml.feature import StopWordsRemover, Tokenizer
from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, lit


def random_text_classifier(input_loc, output_loc, run_id, spark):
    """
    This is a dummy function to show how to use spark, It is supposed to mock
    the following steps
        1. clean input data
        2. use a pre-trained model to make prediction
        3. write predictions to a HDFS output

    Since this is meant as an example, we are going to skip building a model,
    instead we are naively going to mark reviews having the text "good" as
    positive and the rest as negative



        Mock Spark job:
      1. Read raw reviews from input (CSV in HDFS/S3).
      2. Clean/tokenize text.
      3. (Pretend to) run a classifier → here we just check if text contains "good".
      4. Save results (predictions) into output path (Parquet).
    
    """

    # read input
    df_raw = spark.read.option("header", True).csv(input_loc)
    # perform text cleaning

    # Tokenize text
    tokenizer = Tokenizer(inputCol="review_str", outputCol="review_token")
    df_tokens = tokenizer.transform(df_raw).select("cid", "review_token")

    # Remove stop words
    remover = StopWordsRemover(
        inputCol="review_token", outputCol="review_clean"
    )
    df_clean = remover.transform(df_tokens).select("cid", "review_clean")

    # function to check presence of good
    df_out = df_clean.select(
        "cid",
        array_contains(df_clean.review_clean, "good").alias("positive_review"),
    )

    df_fin = df_out.withColumn("insert_date", lit(run_id))

    # save in parquet format to be used in data warehouse
    df_fin.write.mode("overwrite").parquet(output_loc)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        type=str,
        help="HDFS input",
        default="s3a://user-analytics/raw/movie_review.csv",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="HDFS output",
        default="s3a://user-analytics/clean/movie_review",
    )
    parser.add_argument(
        "--run-id", type=str, help="run id", default="2024-05-05"
    )
    args = parser.parse_args()
    spark = (
        SparkSession.builder.appName("efficient-data-processing-spark").getOrCreate()
    )

    random_text_classifier(args.input, args.output, args.run_id, spark)
