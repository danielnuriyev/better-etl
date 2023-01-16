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

        all_events: Iterable[dagster.EventLogRecord] = []

        for event_type in dagster._core.events.PIPELINE_EVENTS:

            events: Iterable[dagster.EventLogRecord] = context.instance.get_event_records(
                # Only get failure events < 1hr old
                dagster.EventRecordsFilter(
                    event_type=event_type,
                    before_timestamp=datetime.datetime.now().timestamp()
                ),
                ascending=False,
                limit=10, # this should equal the number of jobs
            )
            if len(events) > 0:
                all_events.extend(events)

        if len(all_events) > 0:
            _events = []
            job_name = context.solid_config["job_name"]
            for event in all_events:
                if event.event_log_entry.pipeline_name == job_name:
                    _events.append(event)
            all_events = _events

        if len(all_events) > 0:

            def sort_key(event):
                return event.event_log_entry.timestamp

            all_events.sort(key=sort_key)

            latest_event = all_events[-1]

            context.log.info(latest_event.event_log_entry.run_id)

            for event in reversed(all_events):

                context.log.info(event.event_log_entry.run_id)
                context.log.info(event.event_log_entry.dagster_event.event_type_value)

                if event.event_log_entry.run_id != latest_event.event_log_entry.run_id:

                    context.log.info("IN")

                    finished_states = [
                        dagster.DagsterEventType.RUN_SUCCESS,
                        dagster.DagsterEventType.RUN_FAILURE,
                        dagster.DagsterEventType.RUN_CANCELED
                    ]

                    if event.event_log_entry.dagster_event.event_type_value not in finished_states:

                        context.resources.notifier.notify(f"Previous run not finished for {job_name} job")
                        raise "Previous run not finished"

                    break

    @dagster.op
    def empty(context: dagster.OpExecutionContext): pass