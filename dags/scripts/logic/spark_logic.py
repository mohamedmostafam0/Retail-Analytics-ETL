from variables.minio_vars import USER_ANALYTICS_BUCKET

def generate_spark_output_paths(**kwargs):
    ti = kwargs['ti']
    ds = kwargs['ds'] # execution_date as YYYY-MM-DD

    spark_clean_base_path = f"s3a://{USER_ANALYTICS_BUCKET}/clean"
    ti.xcom_push(key='spark_clean_base_path', value=spark_clean_base_path)

    user_purchase_spark_output_path = f"{spark_clean_base_path}/user_purchase/{ds}"
    movie_review_spark_output_path = f"{spark_clean_base_path}/movie_review/{ds}"

    ti.xcom_push(key='user_purchase_spark_output_path', value=user_purchase_spark_output_path)
    ti.xcom_push(key='movie_review_spark_output_path', value=movie_review_spark_output_path)
