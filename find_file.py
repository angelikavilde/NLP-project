"""Find relevant files from AWS S3 bucket"""

from os import environ
from datetime import datetime, timezone, timedelta

from botocore.client import BaseClient
from dotenv import load_dotenv
from boto3 import client


TIME_NOW = datetime.now(timezone.utc)


def get_bucket_connection() -> BaseClient:
    """Returns connection to the AWS buckets"""
    load_dotenv()
    return client("s3", aws_access_key_id = environ.get("ACCESS_KEY"),
                aws_secret_access_key = environ.get("SECRET_KEY"))


def search_for_new_files(s_three: BaseClient, bucket_name: str) -> list[tuple[str]]:
    """Function that finds the list of all items in the bucket"""
    return [(obj["Key"],obj["LastModified"]) for obj
            in s_three.list_objects(Bucket=bucket_name)["Contents"]]


def download_new_file(s_three: BaseClient, bucket_name: str, files: list[tuple[str]]) -> None:
    """Downloading relevant data from the past 10 minutes"""
    for file in files:
        time = TIME_NOW - timedelta(minutes=10)
        if file[0].startswith("angela/data") and file[0].endswith(".xml") and file[1] > time:
            time = "-".join([str(TIME_NOW.day),str(TIME_NOW.hour),str(TIME_NOW.minute)])
            s_three.download_file(bucket_name, file[0], "/tmp/data.xml")


if __name__ == "__main__":
    connection = get_bucket_connection()
    bucket_name = "sigma-pharmazer-input"
    all_files = search_for_new_files(connection, bucket_name)
    download_new_file(connection, bucket_name, all_files)
