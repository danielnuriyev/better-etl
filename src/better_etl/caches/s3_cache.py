import boto3
import botocore
import json

from better_etl.utils.decorators import retry

class S3Cache:

    def __init__(self, bucket, path):

        self.s3 = boto3.resource('s3')

        self.bucket = bucket

        if path[0] == "/": path = path[1:]
        if path[-1] == "/": path = path[:-1]
        self.path = path

    @retry
    def get(self, k):
        print("S3Cache.get")
        full_path = f"{self.path}/{k}"
        object = self.s3.Object(self.bucket, full_path)
        try:
            _json = object.get()['Body'].read().decode('utf-8')
            return json.loads(_json)
        except BaseException as e:
            if e.__class__.__name__ == "NoSuchKey":
                return None
            else:
                raise e

    @retry
    def put(self, k, v):
        full_path = f"{self.path}/{k}"
        object = self.s3.Object(self.bucket, full_path)
        _json = json.dumps(v)
        _bytes = bytes(_json, "utf-8")
        object.put(Body=_bytes)
