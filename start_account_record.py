from infra.trade.service.ctp.account_stats import run_account_recorder
from utils.tool.decorator import try_catch
from utils.tool.base_class import APSchedulerBase
from utils.tool.configer import Config


configer = Config()
trade_conf = configer.get_trade_conf
record_broker = trade_conf.get("LIVESETTINGS", "live_trade_record_broker")


@try_catch(suppress_traceback=False, catch_args=True, enable_alert=True)
def task_wrapper():
    run_account_recorder(record_broker)


if __name__ == "__main__":
    k = input(
        f"{__file__}\n1. schedule task.\n2. run task\n"
    )
    if k == '1':
        scheduler = APSchedulerBase()
        scheduler.register_task(task_wrapper, trigger='cron', hour=20, minute=56, day_of_week='mon-fri', max_instances=1)
        scheduler.register_task(task_wrapper, trigger='cron', hour=8, minute=56, day_of_week='mon-fri', max_instances=1)
        scheduler.register_task(task_wrapper, trigger='cron', hour=12, minute=56, day_of_week='mon-fri', max_instances=1)
        scheduler.start()
    elif k == "2":
        task_wrapper()
