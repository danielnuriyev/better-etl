import dagster

class Test:

    @dagster.op
    def test(context: dagster.OpExecutionContext, test):

        context.log.info(test)
        context.log.info(type(test))
