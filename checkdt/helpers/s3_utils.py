import boto3
from checkdt.config.config import AWS__S3_CONN_TYPE, AWS__ACCESS_KEY_ID, AWS__SECRET_ACCESS_KEY, AWS__S3_BUCKET


class S3Conn:
    def __init__(self, s3_bucket=None):
        self.s3_bucket = s3_bucket or AWS__S3_BUCKET
        self.conn = boto3.ses
