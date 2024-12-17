from model.live.sub.oi_vol_assess import OiVolAssess
from utils.tool.decorator import try_catch
from utils.tool.base_class import APSchedulerBase


@try_catch(suppress_traceback=False, catch_args=True, enable_alert=True)
def task_wrapper():
    t = OiVolAssess()
    t.run()


if __name__ == "__main__":
    k = input(
        f"{__file__}\n1. schedule task.\n2. run task\n"
    )
    if k == '1':
        scheduler = APSchedulerBase()
        scheduler.register_task(task_wrapper, trigger='cron', hour=21, minute=0, day_of_week='mon-fri', max_instances=1)
        scheduler.register_task(task_wrapper, trigger='cron', hour=9, minute=0, day_of_week='mon-fri', max_instances=1)
        scheduler.register_task(task_wrapper, trigger='cron', hour=13, minute=0, day_of_week='mon-fri', max_instances=1)
        scheduler.start()
    elif k == "2":
        task_wrapper()