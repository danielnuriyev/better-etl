import boto3
import dagster
import time

from better_etl.ops.op_wrappers import condition


class AWSAthena:


    def __init__(self):
        session = boto3.Session()
        self.client = session.client("athena")


    def query(self, query, timeout=900):

        response = self.client.start_query_execution(
            QueryString=query
        )

        id = response["QueryExecutionId"]

        get_status = True
        sleep = 0
        total_sleep = 0
        max_sleep = timeout

        while get_status:

            response = self.client.get_query_execution(QueryExecutionId=id)

            state = response["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                get_status = False
            elif state == "FAILED":
                raise f"Failed to execute {query}"
            else:
                sleep += 1

            time.sleep(sleep)
            total_sleep += sleep
            if total_sleep > max_sleep:
                raise f"Exceeded {max_sleep} seconds to execute {query}"

    # @classmethod
    @dagster.op(retry_policy=dagster.RetryPolicy(max_retries=2, delay=1, backoff=dagster.Backoff(dagster.Backoff.EXPONENTIAL)))
    @condition
    def drop_table(context: dagster.OpExecutionContext, args):

        database = context.op_config["database"]
        table = context.op_config["table"]

        q = f"DROP TABLE IF EXISTS {database}.{table}"

        AWSAthena().query(q)
