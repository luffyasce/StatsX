import traceback
import pandas as pd
from data.realtime.ctp import RealTimeCTPDownload
from utils.database.unified_db_control import UnifiedControl
from utils.tool.decorator import try_catch
from utils.tool.base_class import APSchedulerBase
from utils.tool.configer import Config


configer = Config()
trade_conf = configer.get_trade_conf
md_broker = trade_conf.get("LIVESETTINGS", "live_md_broker")
md_channel = trade_conf.get("LIVESETTINGS", "live_md_channel_code")


@try_catch(suppress_traceback=False, catch_args=True, enable_alert=True)
def run_ctp_md_task():
    starter = RealTimeCTPDownload(md_broker, md_channel)
    starter.realtime_data_download(
        derivatives_include=True,
        broadcast=False,
        limit_md_history=2
    )


if __name__ == "__main__":
    run_ctp_md_task()