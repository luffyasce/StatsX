import json
from operator import itemgetter
from typing import Union, Tuple
from datetime import datetime, time
import pandas as pd
import numpy as np
from infra.trade.api.ctp_trade import *
from data.realtime.ctp import RealTimeCTPRecv
from utils.buffer.redis_handle import Redis
from utils.tool.logger import log
from utils.tool.configer import Config
from utils.tool.decorator import singleton

logger = log(__file__, "infra", warning_only=False)

config = Config()
trade_conf = config.get_trade_conf


class _RDS_:
    def __init__(self):
        self.db = trade_conf.getint("RDS", "db")

    def _del_key(self, rds_handle: Redis, key: Union[str, list]):
        if isinstance(key, str):
            key = [key, ]
        rds_handle.del_key(self.db, key)


@singleton
class CliRDS(_RDS_):
    def __init__(self):
        super().__init__()

    def register_status(self, rds_handle: Redis, strategy_name: str, ctp_contract: str, status_value: Union[int, float]):
        status_value = float(status_value)
        rds_handle.set_hash(self.db, f"RDS_SIG_{strategy_name}", ctp_contract, status_value)

    def flush_strategy_record(self, rds_handle: Redis, strategy: str):
        self._del_key(rds_handle, f"RDS_SIG_{strategy}")


class TRDS(_RDS_):
    def __init__(self, trader: CtpTrade, broker: str, md_channel: str):
        super().__init__()
        self.trader = trader
        self.broker = broker
        msg_db = trade_conf.getint("MD", 'md_msg_db')
        self.realtime_ctp_md = RealTimeCTPRecv(msg_db, md_channel)
        self.__md_chan = md_channel

    def reload_ctp_md(self):
        msg_db = trade_conf.getint("MD", 'md_msg_db')
        self.realtime_ctp_md = RealTimeCTPRecv(msg_db, self.__md_chan)
        logger.warning("TRDS RESET MD LISTENER...")

    def get_status(self, rds_handle: Redis, strategy_name: str):
        return pd.Series(rds_handle.get_hash(self.db, f"RDS_SIG_{strategy_name}", decode=True), dtype=float)

    def get_done_status(self, rds_handle: Redis, strategy_name: str):
        return pd.Series(rds_handle.get_hash(self.db, f"RDS_{self.broker}_DN_{strategy_name}", decode=True), dtype=float)

    def save_done_status(
            self,
            rds_handle: Redis,
            strategy_name: str,
            ctp_contract: str = None,
            status_value: Union[int, float] = None,
            status_map: dict = None
    ):
        """
        supports both k-v and mapping type.
        :param rds_handle:
        :param strategy_name:
        :param ctp_contract:
        :param status_value:
        :param status_map:
        :return:
        """
        if status_value is not None:
            status_value = float(status_value)
        if status_map is not None:
            status_map = {k: float(v) for k, v in status_map.items()}
        rds_handle.set_hash(self.db, f"RDS_{self.broker}_DN_{strategy_name}", ctp_contract, status_value, status_map)

    def flush_broker_records(self, rds_handle: Redis, strategies: list):
        key_list = [f"RDS_{self.broker}_DN_{s}" for s in strategies]
        self._del_key(rds_handle, key_list)

    @staticmethod
    def close_intraday_position(close_in: Tuple[str, str]):
        cl_t = [time.fromisoformat(x) for x in close_in]
        current_ = datetime.now().time()
        if (current_ >= min(cl_t)) and (current_ <= max(cl_t)):
            return True
        else:
            return False

    def cmp_status(self, rds_handle: Redis, strategy_name: str, instant_kill: bool):
        """
        :param rds_handle:
        :param instant_kill:
        :param strategy_name:
        :return: 1. current status series; 2. current status change series
        """
        _now = self.get_status(rds_handle, strategy_name)
        _last = self.get_done_status(rds_handle, strategy_name)
        _res = pd.concat([_last.rename('last'), _now.rename('now')], axis=1).fillna(0)
        if instant_kill:
            _res['now'] = 0
            _now = _res['now']
        return _res['now'], _res['last'], _res['now'] - _res['last']

    def get_latest_md(self, ctp_contract: str, pdelta: float = None):
        md_ = self.realtime_ctp_md.get_msg(ctp_contract)
        if pdelta is None:
            return md_
        bid1, ask1 = itemgetter("bid1", "ask1")(md_)
        max_cnt = 10
        cnt = 0
        while ask1 - bid1 > pdelta:
            if cnt > max_cnt:
                logger.warning(
                    f"\nask bid spread constantly greater than given price delta:"
                    f" ask {ask1} bid {bid1} delta {pdelta}\n"
                )
            md_ = self.realtime_ctp_md.get_msg(ctp_contract)
            bid1, ask1 = itemgetter("bid1", "ask1")(md_)
            cnt += 1
        return md_

    def query_account(self):
        req_id = self.trader.query_account()
        while req_id != self.trader.trade_spi.account_detail.get("nRequestID", 0):
            sleep(0.01)
        return self.trader.trade_spi.account_detail

    def query_position(self, ctp_contract: str = None):
        req_id = self.trader.query_position(ctp_contract)
        while not self.trader.trade_spi.req_position_record.get(req_id, False):
            sleep(0.01)
        return self.trader.trade_spi.req_position_detail

    def trade(self, command: str, *args, **kwargs) -> str:
        """
        :param command:
        :param args:
        :param kwargs:
        :return: order ref
        """
        if command == "buy_open":
            ret_ = self.trader.buy_open(*args, **kwargs)
        elif command == "buy_close":
            ret_ = self.trader.buy_close(*args, **kwargs)
        elif command == "sell_open":
            ret_ = self.trader.sell_open(*args, **kwargs)
        elif command == "sell_close":
            ret_ = self.trader.sell_close(*args, **kwargs)
        else:
            raise Exception(f"Check your command: {command}")

        return ret_

    def get_order_detail(self, order_ref: str = None):
        if isinstance(order_ref, str):
            return self.trader.trade_spi.order_rtn_detail.get(order_ref, {})
        else:
            return self.trader.trade_spi.order_rtn_detail

    def get_trade_vol(self, order_ref: str):
        return self.trader.trade_spi.trade_vol_cnt.get(order_ref, 0)

    def get_order_result(self, order_ref: str):
        while self.get_order_detail(order_ref) == {} or self.get_order_detail(order_ref).get("OrderStatus") == 'a':
            # 若没有报单回报或报单回报状态为未知，则等待有效回报
            sleep(0.01)
        if self.get_order_detail(order_ref).get("OrderSubmitStatus") == '4':
            # 如果报单被拒绝，最可能的情况是交易所已停止交易，此时应重置行情接收器，以便在下一次获取行情时堵塞（如果停盘，就没行情推送）
            self.reload_ctp_md()
        sleep(0.2)  # 避免等待更新的空挡拿到空数据
        v_origin = self.get_order_detail(order_ref).get("VolumeTotalOriginal", 0)
        v_traded_from_order_det = self.get_order_detail(order_ref).get("VolumeTraded", 0)
        v_traded_from_trade_vol_cnt = self.get_trade_vol(order_ref)
        # 拿order detail里的已成交数据和成交回报累计成交数量，比较最大值为已成交，然后和总量取最小值，防止误加
        return min(max(v_traded_from_order_det, v_traded_from_trade_vol_cnt), v_origin)

    def revoke_order(self, ctp_contract: str, exchange: str, order_ref: str):
        return self.trader.withdraw_order(ctp_contract, exchange, order_ref)

    @staticmethod
    def sort_prev_position(ctp_contract: str, last_position: pd.DataFrame):
        if last_position.empty:
            long = 0
            short = 0
            tod_long = 0
            tod_short = 0
        else:
            last_position = last_position[
                last_position['InstrumentID'] == ctp_contract
                ]
            long = last_position[last_position['PosiDirection'].astype(int) == 2]['Position'].sum()
            short = last_position[last_position['PosiDirection'].astype(int) == 3]['Position'].sum()
            tod_long = last_position[last_position['PosiDirection'].astype(int) == 2]['TodayPosition'].sum()
            tod_short = last_position[last_position['PosiDirection'].astype(int) == 3]['TodayPosition'].sum()
        return long, short, tod_long, tod_short

    @staticmethod
    def _trade_logic_close_first(pos_chg: int, long: int, short: int):
        if pos_chg > 0:  # 做多
            calc_ = 1
            if short > 0:
                # close short first if holding short.
                trade_1, remain_ = min(pos_chg, short), max(0, pos_chg - short)
                return {"buy_close": trade_1, "buy_open": remain_} if remain_ > 0 else {"buy_close": trade_1}, calc_
            else:
                return {"buy_open": pos_chg}, calc_
        else:  # 做空
            calc_ = -1
            pos_chg *= calc_
            if long > 0:
                # close long first if holding long
                trade_1, remain_ = min(pos_chg, long), max(0, pos_chg - long)
                return {"sell_close": trade_1, "sell_open": remain_} if remain_ > 0 else {"sell_close": trade_1}, calc_
            else:
                return {"sell_open": pos_chg}, calc_

    @staticmethod
    def _trade_logic_lock_first(pos_chg: int, long: int, short: int, tod_long: int, tod_short: int):
        yest_long = long - tod_long
        yest_short = short - tod_short

        if pos_chg > 0:  # 做多
            calc_ = 1
            if yest_short > 0:
                # close short first if holding short.
                trade_1, remain_ = min(pos_chg, yest_short), max(0, pos_chg - yest_short)
                return {"buy_close": trade_1, "buy_open": remain_} if remain_ > 0 else {"buy_close": trade_1}, calc_
            else:
                return {"buy_open": pos_chg}, calc_
        else:  # 做空
            calc_ = -1
            pos_chg *= calc_
            if yest_long > 0:
                # close long first if holding long
                trade_1, remain_ = min(pos_chg, yest_long), max(0, pos_chg - yest_long)
                return {"sell_close": trade_1, "sell_open": remain_} if remain_ > 0 else {"sell_close": trade_1}, calc_
            else:
                return {"sell_open": pos_chg}, calc_

    def trade_logic(self, logic_type: int, pos_chg: int, long: int, short: int, tod_long: int, tod_short: int):
        """
        :param logic_type: 0: close before open 1: lock before open
        :param pos_chg:
        :param long:
        :param short:
        :param tod_long:
        :param tod_short:
        :return:
        """
        if logic_type == 0:
            return self._trade_logic_close_first(pos_chg, long, short)
        elif logic_type == 1:
            return self._trade_logic_lock_first(pos_chg, long, short, tod_long, tod_short)
        else:
            raise ValueError(f"Unexpected trade logic type: {logic_type}")

    def runner(
            self,
            rds_handle: Redis,
            close_time_range: Tuple[str, str],
            strategy_name: str,
            trade_params: dict,
    ):
        ioc_order = True     # only ioc(FAK) in TRDS
        if not ioc_order:
            logger.warning(f"TRDS not in optimum mode: IOC FALSE.")

        _kill = self.close_intraday_position(close_time_range)
        status, rec, task = self.cmp_status(rds_handle, strategy_name, instant_kill=_kill)

        for instrument, factor in task.items():
            if factor == 0:
                continue

            init_md = self.get_latest_md(instrument)
            symbol = init_md.get("symbol")
            exchange = init_md.get("exchange")

            conf_d = trade_params.get(symbol, None)

            if conf_d is None:
                continue

            logger.info(f"\n---MD SET: {symbol} {exchange}---")

            base_position, over_price, min_price_tick = itemgetter("base_position", "over_price", "min_price_tick")(conf_d)

            price_delta = over_price * min_price_tick
            pos_chg = int(factor * base_position)

            if pos_chg == 0:
                continue

            logger.info(f"\n!! TRDS ready: {symbol} {exchange} !!")

            pos_detail = self.query_position()
            last_position = pd.DataFrame(pos_detail)

            long, short, tod_long, tod_short = self.sort_prev_position(instrument, last_position)
            trade_orders, calc_direction = self.trade_logic(0, pos_chg, long, short, tod_long, tod_short)

            completed_volume = 0
            ref_ls = []
            for k, v in trade_orders.items():
                md_ = self.get_latest_md(instrument, price_delta)
                p_ = md_.get("last") + (price_delta * calc_direction)

                if "open" in k:
                    # 开仓时 不需要td_opt参数
                    ref = self.trade(k, instrument, exchange, p_, int(v), ioc=ioc_order)
                    logger.info(f"!! TRDS trade: ref{ref} !!")
                    ref_ls.append(ref)
                else:
                    # 平仓时，上期和INE需要明确是平今还是平昨，否则默认td_opt为0时是平昨操作。
                    if exchange in ["SHFE", "INE"]:
                        if k == "buy_close":
                            yest_short = short - tod_short
                            c_yest = min(yest_short, int(v))
                            c_tod = int(v) - c_yest
                        elif k == "sell_close":
                            yest_long = long - tod_long
                            c_yest = min(yest_long, int(v))
                            c_tod = int(v) - c_yest
                        else:
                            raise AttributeError(f"{k} end up in the wrong place.")
                        for act_ in [(c_yest, 2), (c_tod, 1)]:
                            if act_[0] == 0:
                                pass
                            else:
                                ref = self.trade(k, instrument, exchange, p_, act_[0], ioc=ioc_order, td_opt=act_[1])
                                logger.info(f"!! TRDS trade: ref{ref} !!")
                                ref_ls.append(ref)
                    else:
                        ref = self.trade(k, instrument, exchange, p_, int(v), ioc=ioc_order, td_opt=0)
                        logger.info(f"!! TRDS trade: ref{ref} !!")
                        ref_ls.append(ref)

            sleep(1)
            for r in ref_ls:
                completed_volume += self.get_order_result(order_ref=r)

            rec_v = rec.loc[instrument]

            logger.info(f"TRDS trade: total vol {completed_volume} | rec: {rec_v} \n{'=*'*20}")

            done_v = (completed_volume / base_position) * np.sign(factor)

            if abs(done_v) == 1:
                # 如果全部成交，则done status设为当前状态
                done_status = status.loc[instrument]
            else:
                # 如果并非全部成交

                # 此时不能kill，需要再次尝试平仓
                _kill = False

                done_status = done_v
                if done_v == 0 and completed_volume == 0:
                    # 当次交易没有成交，那么就应该维持之前的成交态不变。
                    done_status = rec_v
                elif abs(rec_v) == 1:         # rec_v = 1
                    # 如果当次交易有成交，且前一状态是全部成交，那就需要把成交态改为当前态
                    pass
                else:
                    # 前一状态不是全部成交： 空仓，或部分成交，则两态进行累加
                    # 前一状态是0.3，当前态为0， 成交-0.3，累加后归零
                    # 前一状态是0.3, 当前态为1, 成交0.7， 累加为1
                    done_status += rec_v

            # after each completion, update status.
            self.save_done_status(rds_handle, strategy_name, instrument, done_status)
        return _kill


def run_trds_trader(
        broker: str,
        md_channel: str,
        close_time_range: Tuple[str, str],
        strategy_configs: dict,
        rds_hdl: Redis
):
    end_ls = []
    trader = CtpTrade(
        front_addr=trade_conf.get(broker, "trade_front_addr"),
        broker_id=trade_conf.get(broker, "broker_id"),
        investor_id=trade_conf.get(broker, "investor_id"),
        pwd=trade_conf.get(broker, "pwd"),
        app_id=trade_conf.get(broker, "app_id"),
        auth_code=trade_conf.get(broker, "auth_code"),
        user_product_info=trade_conf.get(broker, "product_info"),
        mode=trade_conf.getint(broker, "login_mode"),
    )
    with trader as tr:
        trds_instance = TRDS(tr, broker, md_channel)
        logger.warning(f"TRDS START! BROKER {broker} MD {md_channel} @ [{' - '.join(close_time_range)}]")
        trds_instance.speaker.speak_start()
        sleep(3)
        while tr.trade_rule.is_trading(e_type='commodities'):
            print(f"\rRunning...{datetime.now()}", end="", flush=True)
            if len(strategy_configs.keys()) != len(end_ls):
                for strategy, conf_dict in strategy_configs.items():
                    if len(conf_dict.keys()) == 0:
                        continue
                    r_kill = trds_instance.runner(
                        rds_hdl,
                        close_time_range=close_time_range,
                        strategy_name=strategy,
                        trade_params=conf_dict
                    )
                    if r_kill:
                        strategy_configs[strategy] = {}
                        end_ls.append(strategy)
                sleep(1)
            else:
                break

        if len(end_ls) != 0:
            trds_instance.flush_broker_records(rds_hdl, end_ls)      # flush DN records after all trading end.


# if __name__ == "__main__":
#     run_trds_trader(
#         "SIM", "haqh", ("14:55", "15:00"),
#         {
#             "intraday_wave": {
#                 "I": {
#                     "base_position": 1,
#                     "over_price": 1,
#                     "min_price_tick": 1,
#                 }
#             }
#         }
#     )

