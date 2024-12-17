from model.live.sub.order_tracking import OrderTracking
from utils.tool.decorator import try_catch
from utils.tool.base_class import APSchedulerBase


@try_catch(suppress_traceback=False, catch_args=True, enable_alert=True)
def task_wrapper():
    o = OrderTracking()
    o.filter_orders()


if __name__ == "__main__":
    k = input(
        f"{__file__}\n1. schedule task.\n2. run task\n"
    )
    if k == '1':
        scheduler = APSchedulerBase()
        scheduler.register_task(task_wrapper, trigger='cron', hour=20, minute=59, day_of_week='mon-fri', max_instances=1)
        scheduler.register_task(task_wrapper, trigger='cron', hour=8, minute=59, day_of_week='mon-fri', max_instances=1)
        scheduler.register_task(task_wrapper, trigger='cron', hour=12, minute=59, day_of_week='mon-fri', max_instances=1)
        scheduler.start()
    elif k == "2":
        task_wrapper()