import os
import random
import time
from datetime import datetime, timedelta
from x_data_proc_entry.base_update_hist_data import DataUpdateBaseCls
from utils.tool.datetime_wrangle import yield_dates
from utils.tool.decorator import try_catch
from utils.tool.base_class import APSchedulerBase
from utils.tool.configer import Config

file_name = os.path.basename(__file__)


def task_wrapper(initiation: bool = False):
    updator = DataUpdateBaseCls()
    updator.start_pretreat(initiation)
    updator.start_process()
    print(f"{file_name} --- TASK COMPLETE @{datetime.now()}.")


if __name__ == "__main__":
    dt1 = datetime.now().replace(hour=20, minute=10, second=0, microsecond=0)
    dt2 = (datetime.now() + timedelta(days=1)).replace(hour=20, minute=10, second=0, microsecond=0)
    dt = dt1 if datetime.now() < dt1 else dt2
    select_ = input(
        f"\n||>> StatsX Data Center {file_name} <<||\n\n"
        f"0.Schedule Data Pretreat & Process @{dt}\n1. Pretreat & Process Data\n2. Pretreat & Process Data for System Initiation\n"
    )
    if select_ == '0':
        scheduler = APSchedulerBase()
        scheduler.register_task(task_wrapper, trigger='date', run_date=dt)
        scheduler.start()
    elif select_ == "1":
        task_wrapper()
    elif select_ == '2':
        task_wrapper(True)