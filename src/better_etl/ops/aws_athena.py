import boto3
import time

class AWSAthena:

    @classmethod
    @dagster.op
    @condition
    def drop_table(context: dagster.OpExecutionContext) -> None:

        database = context.solid_config["database"]
        table = context.solid_config["table"]

        query = f"DROP TABLE IF EXISTS {database}.{table}"

        client = session.client("athena")

        response = client.start_query_execution(
            QueryString=query
        )

        id = response["QueryExecutionId"]

        get_status = True
        sleep = 0
        total_sleep = 0
        max_sleep = 900

        while get_status:

            response = client.get_query_execution(QueryExecutionId=id)

            state = response["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                get_status = False
            elif state == "FAILED":
                raise f"Failed to drop {database}.{table}"
            else:
                sleep += 1

            time.sleep(sleep)
            total_sleep += sleep
            if total_sleep > max_sleep:
                raise f"Exceeded {max_sleep} seconds to drop {database}.{table}"