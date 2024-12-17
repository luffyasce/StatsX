from infra.trade.api.ctp_md import *
from utils.tool.configer import Config


class BaseCTPMd:
    def __init__(self, broker_name: str, channel: str):
        config = Config()
        trade_conf = config.get_trade_conf
        self.__m__ = CtpMd(
            trade_conf.get(broker_name, "md_front_addr"),
            trade_conf.get(broker_name, "broker_id"),
            trade_conf.get(broker_name, "investor_id"),
            trade_conf.get(broker_name, "pwd"),
            trade_conf.get(broker_name, "product_info"),
            msg_db=trade_conf.getint("MD", "md_msg_db"),
            channel_name=channel,
        )

    @property
    def md_handle(self):
        return self.__m__
