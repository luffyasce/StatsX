import signal
import sys
import traceback
from typing import Any, Callable
from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_SCHEDULER_SHUTDOWN
from utils.tool.decorator import sched_job
from utils.tool.logger import log

logger = log(__file__, "utils", warning_only=False)


class APSchedulerBase:
    def __init__(self, scheduler: Any = None, add_listener: bool = False):
        if scheduler is None:
            self.sched = BlockingScheduler(timezone="Asia/Shanghai")
            if add_listener:
                self.sched.add_listener(self.ap_listener, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)
        else:
            self.sched = scheduler

        signal.signal(signal.SIGINT | signal.SIGTERM, lambda signum, frame: self.sig_handle(signum, frame, self.sched))

    @staticmethod
    def sig_handle(sig_num, frame, scheduler):
        scheduler.shutdown(wait=False)

    @staticmethod
    def ap_listener(event):
        if event.exception:
            logger.error(str(traceback.format_exc()))

    def on_kill(self):
        logger.warning("Scheduler Exit.")
        self.sched.shutdown(wait=False)

    def register_task(
            self, func: Callable, trigger: str,
            **kwargs
    ):
        logger.info(f"Register cron job for {func.__name__} with params: {kwargs}")
        misfire_grace_time = kwargs.pop("misfire_grace_time", 60)
        self.sched.add_job(func, trigger=trigger, misfire_grace_time=misfire_grace_time, **kwargs)

    def start(self):
        try:
            logger.warning("Scheduler Start.")
            self.sched.start()
        except KeyboardInterrupt or SystemExit:
            self.on_kill()