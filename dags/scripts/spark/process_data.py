import argparse
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, when


def process_movie_reviews(spark, input_path, output_path, run_id):
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

    predictions.select("cid", "positive_review").write.mode("overwrite").parquet(f"{output_path}/movie_review/{run_id}")

def process_user_purchases(spark, input_path, output_path, run_id):
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df.write.mode("overwrite").parquet(f"{output_path}/user_purchase/{run_id}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--movie_review_input", required=True)
    parser.add_argument("--user_purchase_input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("ProcessData").getOrCreate()

    process_movie_reviews(spark, args.movie_review_input, args.output, args.run_id)
    process_user_purchases(spark, args.user_purchase_input, args.output, args.run_id)

    spark.stop()
