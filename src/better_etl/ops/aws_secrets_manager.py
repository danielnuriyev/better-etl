import boto3
import dagster
import json
import typing

from better_etl.ops.op_wrappers import condition

class AWSSecretsManager:

    @classmethod
    @dagster.op
    @condition
    def get_secret(context: dagster.OpExecutionContext) -> typing.Dict:

        context.log.info("get_aws_secret")
        secret_name = context.solid_config["secret_name"]
        session = boto3.Session()
        client = session.client("secretsmanager")
        response = client.get_secret_value(SecretId=secret_name)
        secret = response["SecretString"]
        return json.loads(secret)
