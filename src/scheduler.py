from apscheduler.executors.pool import ProcessPoolExecutor, ThreadPoolExecutor
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler, BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from multiprocessing import cpu_count

from pytz import utc

from step import Step

class Scheduler:
    def __init__(self):



        # multiprocessing + local slite is just for testing
        cores = cpu_count()
        cores = cores - 1 if cores > 1 else 1
        jobstores = {
            'default': SQLAlchemyJobStore(url='sqlite:///jobs.sqlite')
        }
        executors = {
            'default': ProcessPoolExecutor(max_workers=cores)
        }
        job_defaults = {
            'coalesce': False,
            'max_instances': 1
        }
        conf = {
            "jobstores": jobstores,
            "executors": executors,
            "job_defaults": job_defaults,
            "timezone": utc
        }
        scheduler = BackgroundScheduler(gconfig=conf)  # TODO: cluster, cloud
        # scheduler.configure(jobstores=jobstores, executors=executors, timezone=utc, job_defaults=job_defaults)
        scheduler.start()
        self._scheduler = scheduler

    def schedule(self, cron, f): # TODO: cluster, cloud
        self._scheduler.add_job(f, CronTrigger.from_crontab(cron))

