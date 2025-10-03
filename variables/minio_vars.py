import os

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_API_PORT = int(os.getenv("MINIO_API_PORT", "9000"))
MINIO_CONSOLE_PORT = int(os.getenv("MINIO_CONSOLE_PORT", "9001"))
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

USER_ANALYTICS_BUCKET = "user-analytics"
MOVIE_REVIEW_KEY = "movie_review.csv"
USER_PURCHASE_KEY = "raw/user_purchase.csv"
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_REGION = os.getenv("MINIO_REGION")

AWS_CONN_ID = os.getenv("AWS_CONN_ID")
MINIO_CONN_TYPE = os.getenv("MINIO_CONN_TYPE")
AWS_REGION = os.getenv("AWS_REGION")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")