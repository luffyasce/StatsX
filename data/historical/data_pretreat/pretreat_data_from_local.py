import re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import utils.database.unified_db_control as udc
from data.data_utils.check_alias import check_symbol_alias

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class PretreatLocal:
    def __init__(self):
        self.base = udc.UnifiedControl(db_type='base')
        self.origin = udc.UnifiedControl(db_type='origin')

    @staticmethod
    def proc_daily_iv_range(df: pd.DataFrame):
        df = df.groupby('contract').apply(
            lambda x: x.groupby('trading_date')[['iv']].mean()
        ).reset_index(drop=False)
        call_avg = df[df['contract'].str.contains('-C-')].groupby('trading_date')['iv'].mean()
        call_std = df[df['contract'].str.contains('-C-')].groupby('trading_date')['iv'].std()
        put_avg = df[df['contract'].str.contains('-P-')].groupby('trading_date')['iv'].mean()
        put_std = df[df['contract'].str.contains('-P-')].groupby('trading_date')['iv'].std()
        res = pd.concat(
            [
                call_avg.rename('call_avg'),
                (call_avg + call_std).rename('call_upper'),
                (call_avg - call_std).rename('call_lower'),
                put_avg.rename('put_avg'),
                (put_avg + put_std).rename('put_upper'),
                (put_avg - put_std).rename('put_lower'),
            ], axis=1
        ).reset_index(drop=False, names=['trading_date'])
        return res

    def pretreat_daily_iv_range_data(self):
        last_df = self.base.read_dataframe(
            "pretreated_option_cn_md_data", "all_1d_iv_range_DIY",
            ascending=[('trading_date', False)],
            filter_row_limit=1
        )
        start_date = last_df.iloc[0]['trading_date'].strftime("%Y-%m-%d") if not last_df.empty else None
        filt_date = {'trading_date': {'gte': start_date}} if start_date is not None else None
        raw_df = self.origin.read_dataframe(
            db_name="origin_future_cn_md_data",
            tb_name="iv_records_by_option_contract_DIY",
            filter_datetime=filt_date
        )
        for underlying, df in raw_df.groupby('underlying'):
            if df.empty:
                continue
            res = self.proc_daily_iv_range(df)
            res = res.assign(underlying_contract=underlying)
            yield res

    def save_daily_iv_range_data(self, df: pd.DataFrame):
        self.base.insert_dataframe(
            df, "pretreated_option_cn_md_data", "all_1d_iv_range_DIY",
            set_index=['trading_date', 'underlying_contract']
        )

    def del_past_iv_record_data(self, record_date_max: int = 5):
        rec_dts = self.origin.read_dataframe(
            sql_str="select DISTINCT(`trading_date`) from origin_future_cn_md_data.iv_records_by_option_contract_DIY "
                    "order by `trading_date` DESC"
        )
        if len(rec_dts) < record_date_max:
            return
        else:
            earliest_dt = rec_dts['trading_date'].iloc[record_date_max - 1]
            self.origin.del_row(
                db_name="origin_future_cn_md_data",
                tb_name="iv_records_by_option_contract_DIY",
                filter_datetime={'trading_date': {'lt': earliest_dt.strftime('%Y-%m-%d')}}
            )

    @staticmethod
    def _pret_single_day_filter_order_(df: pd.DataFrame):
        if len(df['trading_date'].drop_duplicates()) > 1:
            raise AttributeError("Got data with more than one trading date, Please check again.")
        return df.groupby('last')[['real_money']].sum()

    def pretreat_filtered_orders_data(self):
        last_df = self.base.read_dataframe(
            "pretreated_future_cn_model_data", "filtered_orders_DIY",
            ascending=[('trading_date', False)],
            filter_row_limit=1
        )
        start_date = last_df.iloc[0]['trading_date'].strftime("%Y-%m-%d") if not last_df.empty else None
        filt_date = {'trading_date': {'gte': start_date}} if start_date is not None else None
        raw_df = self.origin.read_dataframe(
            db_name="origin_future_cn_model_data",
            tb_name="filtered_orders_DIY",
            filter_datetime=filt_date
        )
        if raw_df.empty:
            return
        raw_df['real_money'] = raw_df['money_delta'] * raw_df['information_ratio'].abs()
        for c, v in raw_df.groupby('contract'):
            res = v.groupby('trading_date').apply(lambda x: self._pret_single_day_filter_order_(x))
            res_df = res.reset_index(drop=False).assign(contract=c).rename(columns={'real_money': "rm_stake"})
            yield res_df

    def save_filtered_orders_data(self, df: pd.DataFrame):
        self.base.insert_dataframe(
            df, "pretreated_future_cn_model_data", "filtered_orders_DIY",
            set_index=['trading_date', 'contract', 'last']
        )
