from datetime import datetime
import pandas as pd
import numpy as np
from utils.database.unified_db_control import UnifiedControl
from utils.tool.configer import Config

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class IBPosComp:
    def __init__(self, date: datetime):
        self.date = date.replace(hour=0, minute=0, second=0, microsecond=0)
        self.udc = UnifiedControl(db_type='base')
        config = Config()
        self.exchange_ls = config.exchange_list

    @property
    def target_brokers(self):
        df = self.udc.read_dataframe(
            "processed_future_cn_model_data", "broker_position_information_score_SUM",
            filter_datetime={'trading_date': {'eq': self.date.strftime("%Y-%m-%d")}}
        )
        if df.empty:
            return [], []
        else:
            df = df.sort_values(by='information_score', ascending=False)
            wl = df[df['information_score'] > 0].head(5)['broker'].tolist()
            sl = df[df['information_score'] < 0].tail(5)['broker'].tolist()
            return wl, sl

    def position_sum_up(self, broker_ls: list):
        for e in self.exchange_ls:
            bdf = pd.DataFrame()
            for broker in broker_ls:
                df = self.udc.read_dataframe(
                    "processed_future_cn_trade_data", f"net_position_by_symbol_{e}",
                    filter_datetime={'trading_date': {'eq': self.date.strftime("%Y-%m-%d")}},
                    filter_keyword={"broker": {'eq': broker}},
                )
                bdf = pd.concat([bdf, df], axis=0)
            if bdf.empty:
                continue
            pos_df = bdf.groupby('symbol')[['net_pos', 'net_chg']].sum()
            pos_df = pos_df.reset_index(names=['symbol']).assign(trading_date=self.date)
            yield pos_df

    def save_position_data(self, df: pd.DataFrame, data_type: str):
        self.udc.insert_dataframe(
            df, "processed_future_cn_model_data", f"{data_type}_broker_total_position_SUM",
            set_index=['trading_date', 'symbol'], partition=['trading_date']
        )

    def total_up_brokers_position(self):
        informed, uninformed = self.target_brokers
        for res_in in self.position_sum_up(informed):
            self.save_position_data(res_in, 'informed')
        for res_un in self.position_sum_up(uninformed):
            self.save_position_data(res_un, "uninformed")


if __name__ == "__main__":
    f = IBPosComp(datetime.now())
    # print(f.brokers)
    f.position_sum_up(f.target_brokers[0])