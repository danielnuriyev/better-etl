import boto3
import dagster
import time

from better_etl.ops.op_wrappers import condition


class AWSAthena:

    def __init__(self):
        session = boto3.Session()
        self._athena = session.client("athena")
        self._glue = session.client("glue")

    def query(self, query, timeout=900):

        response = self._athena.start_query_execution(
            QueryString=query
        )

        id = response["QueryExecutionId"]

        get_status = True
        sleep = 0
        total_sleep = 0
        max_sleep = timeout

        while get_status:

            response = self._athena.get_query_execution(QueryExecutionId=id)

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

    def table_exists(self, database, table):
        try:
            self._glue.get_table(
                DatabaseName=database,
                Name=table
            )
            return True
        except self._glue.exceptions.EntityNotFoundException:
            return False

    def drop_table(self, database, table):
        self._glue.delete_table(
            DatabaseName=database,
            Name=table
        )


    @dagster.op(
        retry_policy=dagster.RetryPolicy(max_retries=2, delay=1, backoff=dagster.Backoff(dagster.Backoff.EXPONENTIAL)))
    @condition
    def create_table(context: dagster.OpExecutionContext, batches):

        database = context.op_config["database"]
        table = context.op_config["table"]
        bucket = context.op_config["bucket"]
        path = context.op_config["path"]
        partition = context.op_config.get("partition", None)
        recreate = context.op_config.get("recreate", False)

        if recreate:
            athena = AWSAthena()
            if athena.table_exists(database, table):
                athena.drop_table(database, table)

            column2type = {}
            column_names = []
            for batch in batches:
                for p in batch:
                    for column in p["metadata"]["columns"]:
                        name = column["Field"]
                        type = column["Type"]

                        if name not in column2type:
                            column2type[name] = type
                            column_names.append(name)

            columns = ",\n".join([f"{name} {column2type[name]}" for name in column_names])

            if partition:
                partition = f"PARTITIONED BY ({partition})"
            else:
                partition = ""

            location = f"s3://{bucket}/{path}"

            sql = f"""
                CREATE TABLE {database}.{table} (
                    {columns},
                  ) 
                {partition}
                LOCATION '{location}' 
                TBLPROPERTIES (
                  'table_type'='ICEBERG'
                )
            """
            context.log.info(sql)
            athena.query(sql)


    # @classmethod
    @dagster.op(retry_policy=dagster.RetryPolicy(max_retries=2, delay=1, backoff=dagster.Backoff(dagster.Backoff.EXPONENTIAL)))
    @condition
    def drop_table(context: dagster.OpExecutionContext, args):

        database = context.op_config["database"]
        table = context.op_config["table"]

        AWSAthena().drop_table(database, table)
