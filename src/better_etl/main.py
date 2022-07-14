
from boto3 import Session

from dagster import job, op

@op
def get_aws_secret(context):
    secret_name = context.op_config["secret_name"]

    session = Session()
    client = session.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    secret = response["SecretString"]
    context.log.info(secret)


@job
def main():
    get_aws_secret()
