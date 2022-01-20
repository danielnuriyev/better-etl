from apscheduler.executors.pool import ProcessPoolExecutor
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler, BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from multiprocessing import cpu_count

from pytz import utc

from step import Step

class Scheduler:
    def __init__(self):

        self._scheduler = BackgroundScheduler() # TODO: cluster, cloud

        # multiprocessing + local slite is just for testing
        cores = cpu_count()
        cores = cores - 1 if cores > 1 else 1
        jobstores = {
            'default': SQLAlchemyJobStore(url='sqlite:///jobs.sqlite')
        }
        executors = {
            'default': ProcessPoolExecutor(max_workers=cores)
        }
        self._scheduler.configure(jobstores=jobstores, executors=executors, timezone=utc)


    def start(self): # on a cluster should join an existing cluster
        self._scheduler.start()

    def pause(self):
        self._scheduler.pause()

    def schedule(self, cron, f): # TODO: cluster, cloud
        self._scheduler.add_job(f, CronTrigger.from_crontab(cron))

