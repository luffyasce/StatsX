import traceback
import pandas as pd
import subprocess
import time
from datetime import datetime
from data.realtime.ctp import RealTimeCTPDownload
from utils.database.unified_db_control import UnifiedControl
from utils.tool.decorator import try_catch
from utils.tool.base_class import APSchedulerBase
from utils.tool.configer import Config
from infra.tool.rules import TradeRules


configer = Config()
trade_conf = configer.get_trade_conf
md_broker = trade_conf.get("LIVESETTINGS", "live_md_broker")
md_channel = trade_conf.get("LIVESETTINGS", "live_md_channel_code")

rules = TradeRules()


def check_process_running(process_name):
    try:
        # 使用 tasklist 命令检查进程
        result = subprocess.run(
            ['wmic', 'process', 'get', 'commandline'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        if result.returncode == 0:
            output = result.stdout
        else:
            print("WMIC command failed with error:", result.stderr)
            raise Exception('WMIC command failed with error.')

        check_result = process_name in output
        print(f'\nMD script running status: {check_result}\n')
        return check_result
    except subprocess.CalledProcessError:
        return False


script_str = "ctp_md_download_task"


def restart_program():
    print("Restarting the program...")
    subprocess.Popen(["python", f"{script_str}.py"])


def task_wrapper():
    process_name = script_str
    while not rules.api_exit_signal(datetime.now()):
        if not check_process_running(process_name):
            restart_program()
        time.sleep(30)


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

