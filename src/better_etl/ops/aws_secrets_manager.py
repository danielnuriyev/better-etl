import boto3
import dagster
import json
import typing

from better_etl.ops.op_wrappers import condition

class AWSSecretsManager:

    def get_op_metadata(self):
        return {
            "get_secret": {
                "return": {
                    "dynamic": False
                }
            }
        }

    @dagster.op(retry_policy=dagster.RetryPolicy(max_retries=2, delay=1, backoff=dagster.Backoff(dagster.Backoff.EXPONENTIAL)))
    @condition
    def get_secret(context: dagster.OpExecutionContext, args) -> typing.Dict:

        context.log.info("get_aws_secret")
        secret_name = context.op_config["secret_name"]
        session = boto3.Session()
        client = session.client("secretsmanager")
        response = client.get_secret_value(SecretId=secret_name)
        secret = response["SecretString"]
        return json.loads(secret)
