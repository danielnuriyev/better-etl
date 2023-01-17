import datetime

import dagster

class Utils:

    @dagster.op(retry_policy=dagster.RetryPolicy(max_retries=2, delay=1, backoff=dagster.Backoff(dagster.Backoff.EXPONENTIAL)))
    def find_last_keys(context: dagster.OpExecutionContext, collection):

        max_item = {}
        cache_key = None
        for item in collection:
            metadata = item["metadata"]
            last_keys = metadata["last_keys"]
            for k in last_keys.keys():
                pre_v = max_item.get(k, None)
                cur_v = last_keys[k]
                if pre_v is None:
                    max_item[k] = cur_v
                    cache_key = metadata["cache_key"]
                else:
                    if cur_v > pre_v:
                        max_item[k] = cur_v
                        cache_key = metadata["cache_key"]

        return {
            "last_keys": max_item,
            "cache_key": cache_key
        }

    @dagster.op(
        required_resource_keys={"notifier"},
        # retry_policy=dagster.RetryPolicy(max_retries=2, delay=1, backoff=dagster.Backoff(dagster.Backoff.EXPONENTIAL))
    )
    def check_previous_run(context: dagster.OpExecutionContext):

        job_name = context.solid_config["job_name"]

        previous_runs = []
        for run_record in context.instance.get_run_records(
                filters=dagster.RunsFilter(
                    job_name=job_name,
                    # statuses=[DagsterRunStatus.SUCCESS],
                    # updated_after=midnight,
                ),
                ascending=False,
                limit=2
        ):
            previous_runs.append(run_record)

        if len(previous_runs) == 2:

            latest_event = previous_runs[-1]

            finished_states = [
                dagster.DagsterRunStatus.QUEUED,
                dagster.DagsterRunStatus.NOT_STARTED,
                dagster.DagsterRunStatus.MANAGED,
                dagster.DagsterRunStatus.STARTING,
                dagster.DagsterRunStatus.STARTED,
                dagster.DagsterRunStatus.CANCELING,
            ]

            context.log.info(latest_event.pipeline_run.status.name)

            if latest_event.pipeline_run.status.name in finished_states:

                context.resources.notifier.notify(f"Previous run not finished for {job_name} job")

                raise Exception("Previous run not finished")


    @dagster.op
    def empty(context: dagster.OpExecutionContext): pass