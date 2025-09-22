import argparse
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, when


def process_movie_reviews(spark, input_path, gcp_project_id, gcp_dataset_name, run_id):
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    tokenizer = Tokenizer(inputCol="review_str", outputCol="words")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    hashingTF = HashingTF(inputCol="filtered_words", outputCol="rawFeatures", numFeatures=20)
    idf = IDF(inputCol="rawFeatures", outputCol="features")

    pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf])
    model = pipeline.fit(df)
    predictions = model.transform(df)

    # Dummy classification logic
    predictions = predictions.withColumn("positive_review", when(col("sentiment") == "positive", True).otherwise(False))

    predictions.select("cid", "positive_review").write.format("bigquery").option("table", f"{gcp_project_id}.{gcp_dataset_name}.movie_review").mode("overwrite").save()


def process_user_purchases(spark, input_path, gcp_project_id, gcp_dataset_name, run_id):
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df.write.format("bigquery").option("table", f"{gcp_project_id}.{gcp_dataset_name}.user_purchase").mode("overwrite").save()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--movie_review_input", required=True)
    parser.add_argument("--user_purchase_input", required=True)
    parser.add_argument("--gcp_project_id", required=True)
    parser.add_argument("--gcp_dataset_name", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("ProcessData") \
        .getOrCreate()

    process_movie_reviews(spark, args.movie_review_input, args.gcp_project_id, args.gcp_dataset_name, args.run_id)
    process_user_purchases(spark, args.user_purchase_input, args.gcp_project_id, args.gcp_dataset_name, args.run_id)

    spark.stop()
