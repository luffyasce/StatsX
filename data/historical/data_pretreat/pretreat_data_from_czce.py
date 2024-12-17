import re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import utils.database.unified_db_control as udc
from data.data_utils.check_alias import check_symbol_alias

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class PretreatCZCE:
    def __init__(self):
        self.base = udc.UnifiedControl(db_type='base')

    def pretreat_option_contract_info_data(
            self,
            df: pd.DataFrame = None,
    ):
        start_date = datetime.now().strftime("%Y-%m-%d")
        fil_ = {"到期时间": {"gte": start_date}}
        if df is None:
            df = self.base.read_dataframe(
                "raw_option_cn_meta_data",
                "contract_info_CZCE",
                filter_datetime=fil_
            )
        df = df.applymap(lambda x: x if type(x) != str else x.strip())
        contract_splits = df['合约代码'].str.extract("(?P<par1>[A-Z]+)(?P<par2>[0-9]+)(?P<par3>[C, P])(?P<par4>[0-9]+)")
        df[['年份代码', '月份代码']] = df[['年份代码', '月份代码']].astype(float).astype(int).astype(str)
        df['月份代码'] = df['月份代码'].astype(int).apply(lambda x: "{:02d}".format(x))
        df['contract'] = contract_splits['par1'] + df['年份代码'].apply(lambda x: x[-2:]) + df['月份代码'] + '-' + contract_splits['par3'] + '-' + contract_splits['par4']
        df['direction'] = contract_splits['par3']
        df['strike_price'] = contract_splits['par4']
        df = df.rename(
            columns={
                '产品代码': 'symbol',
                '到期时间': 'delist_datetime',
                '合约代码': 'raw_contract',
                '第一交易日': 'listed_date',
                '最小变动价位': 'tick_price',
                '最小变动价值': 'trade_unit',
                '最后交易日': 'last_trading_date',
                '结算日': 'settlement_date',
                '到期日': 'delist_date',
            }
        ).drop(
            columns=[
                '产品名称', '交易单位', '计量单位', '最大下单量', '日持仓限额', '大宗交易最小规模',
                '上市周期', '交割通知日', '月份代码', '年份代码', '行权价', '期权卖保证金额', '交易手续费', '行权/履约手续费',
                '平今仓手续费'
            ]
        )
        df['tick_price'] = df['tick_price'].apply(lambda x: float(re.findall("[0-9]+\.[0-9]{2}", x)[0]))
        df['trade_unit'] = df['trade_unit'].apply(lambda x: float(re.findall("[0-9]+\.[0-9]{2}", x)[0])) / df['tick_price']

        df['contract'] = df['contract'].apply(
            lambda x: check_symbol_alias(re.findall("[A-Z]+", x)[0]) + x[len(re.findall("[A-Z]+", x)[0]):]
        )
        df['underlying_contract'] = df['contract'].apply(lambda x: x.split('-')[0])
        df['symbol'] = df['underlying_contract'].apply(lambda x: x[:-4])
        df['exchange'] = 'CZCE'
        for tc in [tc for tc in df.columns if 'date' in tc]:
            df[tc] = pd.to_datetime(df[tc])
        return df

    def save_option_contract_info_data(
            self, df: pd.DataFrame
    ):
        self.base.insert_dataframe(
            df, "pretreated_option_cn_meta_data", "contract_info_CZCE",
            set_index=["contract", "listed_date"],
            partition=['listed_date']
        )

    def pretreat_future_contract_info_data(
            self,
            df: pd.DataFrame = None,
    ):
        start_date = datetime.now().strftime("%Y-%m-%d")
        fil_ = {"到期时间": {"gte": start_date}}
        if df is None:
            df = self.base.read_dataframe(
                "raw_future_cn_meta_data",
                "contract_info_CZCE",
                filter_datetime=fil_
            )
        df = df.applymap(lambda x: x if type(x) != str else x.strip())
        contract_splits = df['合约代码'].str.extract("(?P<par1>[A-Z]+)(?P<par2>[0-9]+)")

        df[['年份代码', '月份代码']] = df[['年份代码', '月份代码']].astype(float).astype(int).astype(str)
        df['月份代码'] = df['月份代码'].astype(int).apply(lambda x: "{:02d}".format(x))
        df['contract'] = contract_splits['par1'] + df['年份代码'].apply(lambda x: x[-2:]) + df['月份代码']
        df = df.rename(
            columns={
                '产品代码': 'symbol',
                '到期时间': 'delist_datetime',
                '合约代码': 'raw_contract',
                '第一交易日': 'listed_date',
                '最小变动价位': 'tick_price',
                '最小变动价值': 'trade_unit',
                '最后交易日': 'last_trading_date',
                '结算日': 'settlement_date',
                '到期日': 'delist_date',
                '交割结算日': 'delivery_start_date',
                '最后交割日': 'delivery_end_date',
                '合约交割月份': 'delivery_month',
                '交易保证金率': 'margin_rate',
                '涨跌停板': 'price_limit',
                '交割手续费': 'delivery_rate'
            }
        ).drop(
            columns=[
                '产品名称', '交易单位', '计量单位', '最大下单量', '日持仓限额', '大宗交易最小规模',
                '上市周期', '交割通知日', '月份代码', '年份代码', '交易手续费', '平今仓手续费'
            ]
        )
        df['delist_date'] = pd.to_datetime(df['delist_datetime'].apply(lambda x: x.date()))
        df['contract'] = [f"{check_symbol_alias(contract_[:-4])}{contract_[-4:]}" for contract_ in df['contract']]
        df['tick_price'] = df['tick_price'].apply(lambda x: float(re.findall("[0-9]+\.[0-9]{2}", x)[0]))
        df['trade_unit'] = df['trade_unit'].apply(lambda x: float(re.findall("[0-9]+\.[0-9]{2}", x)[0])) / df['tick_price']
        df['symbol'] = df['contract'].apply(lambda x: x[:-4])
        df['exchange'] = 'CZCE'
        for tc in [tc for tc in df.columns if 'date' in tc]:
            df[tc] = pd.to_datetime(df[tc])
        return df

    def save_future_contract_info_data(
            self, df: pd.DataFrame
    ):
        self.base.insert_dataframe(
            df, "pretreated_future_cn_meta_data", "contract_info_CZCE",
            set_index=["contract", "listed_date"],
            partition=['listed_date']
        )

    def pretreat_future_md_data(
            self,
            df: pd.DataFrame = None,
            start_with_last: bool = True,
    ):
        dt_delta = -5
        start_date = (datetime.now() + timedelta(dt_delta)).strftime("%Y-%m-%d")
        if start_with_last:
            last_df = self.base.read_dataframe(
                "pretreated_future_cn_md_data", "all_1d_CZCE",
                ascending=[('datetime', False)],
                filter_row_limit=1
            )
            start_date = last_df.iloc[0]['datetime'].strftime("%Y-%m-%d") if not last_df.empty else None
            fil_ = {"datetime": {"gte": start_date}} if start_date is not None else None
        else:
            fil_ = {"datetime": {"gte": start_date}}
        if df is None:
            df = self.base.read_dataframe(
                "raw_future_cn_md_data",
                "all_1d_CZCE",
                filter_datetime=fil_
            )
        else:
            pass
        df = df.applymap(lambda x: x if type(x) != str else x.strip())

        contract_splits = df['合约代码'].str.extract("(?P<par1>[A-Z]+)(?P<par2>[0-9]+)")
        y_ = df['trading_date'].apply(lambda x: str(x.year)[-2:]).astype(int)
        c_i = contract_splits['par2'].apply(lambda x: x[0])
        y_c = pd.Series(np.where(c_i.astype(int) != y_ % 10, y_ + 1, y_), index=df.index)
        df['contract'] = contract_splits['par1'] + y_c.apply(lambda x: str(x)[-2]) + contract_splits['par2']
        df['contract'] = [f"{check_symbol_alias(contract_[:-4])}{contract_[-4:]}" for contract_ in df['contract']]
        df['symbol'] = df['contract'].apply(lambda x: x[:-4])
        df = df.rename(
            columns={
                '合约代码': 'raw_contract',
                '昨结算': 'pre_settlement',
                '今开盘': 'open',
                '最高价': 'high',
                '最低价': 'low',
                '今收盘': 'close',
                '今结算': 'settlement',
                '成交量(手)': 'volume',
                '持仓量': 'open_interest',
                '增减量': 'open_interest_chg',
                '成交额(万元)': 'turnover',
            }
        ).drop(
            columns=['涨跌1', '涨跌2', '交割结算价']
        ).fillna(0)
        df = df.applymap(lambda x: str(x).replace(',', ''))
        df[[
            'volume', 'open_interest', 'open_interest_chg', 'pre_settlement', 'open', 'high', 'low',
            'close', 'settlement', 'turnover'
        ]] = df[[
            'volume', 'open_interest', 'open_interest_chg', 'pre_settlement', 'open', 'high', 'low',
            'close', 'settlement', 'turnover'
        ]].astype(float)
        df['turnover'] = df['turnover'] * 1e4
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['trading_date'] = pd.to_datetime(df['trading_date'])
        return df

    def save_future_md_data(self, df: pd.DataFrame):
        self.base.insert_dataframe(
            df, "pretreated_future_cn_md_data", "all_1d_CZCE",
            set_index=['contract', 'datetime'],
            partition=['trading_date']
        )

    def pretreat_option_md_data(
            self,
            df: pd.DataFrame = None,
            start_with_last: bool = True
    ):
        dt_delta = -5
        start_date = (datetime.now() + timedelta(dt_delta)).strftime("%Y-%m-%d")
        if start_with_last:
            last_df1 = self.base.read_dataframe(
                "pretreated_option_cn_md_data", "all_1d_CZCE",
                ascending=[('datetime', False)],
                filter_row_limit=1
            )
            last_df2 = self.base.read_dataframe(
                "pretreated_option_cn_md_data", "all_1d_opt_summary_CZCE",
                ascending=[('datetime', False)],
                filter_row_limit=1
            )
            start_date1 = last_df1.iloc[0]['datetime'].strftime("%Y-%m-%d") if not last_df1.empty else None
            start_date2 = last_df2.iloc[0]['datetime'].strftime("%Y-%m-%d") if not last_df2.empty else None
            start_date = None if start_date1 is None or start_date2 is None else min(start_date1, start_date2)
            fil_ = {"datetime": {"gte": start_date}} if start_date is not None else None
        else:
            fil_ = {"datetime": {"gte": start_date}}
        if df is None:
            df = self.base.read_dataframe(
                "raw_option_cn_md_data",
                "all_1d_CZCE",
                filter_datetime=fil_
            )
        else:
            pass
        df = df.applymap(lambda x: x if type(x) != str else x.strip())

        contract_splits = df['合约代码'].str.extract("(?P<par1>[A-Z]+)(?P<par2>[0-9]+)(?P<par3>[C, P])(?P<par4>[0-9]+)")
        y_ = df['trading_date'].apply(lambda x: str(x.year)[-2:]).astype(int)
        c_i = contract_splits['par2'].apply(lambda x: x[0])
        y_c = pd.Series(np.where(c_i.astype(int) != y_ % 10, y_ + 1, y_), index=df.index)
        df['contract'] = contract_splits['par1'] + y_c.apply(lambda x: str(x)[-2]) + contract_splits['par2'] + '-' + contract_splits['par3'] + '-' + contract_splits['par4']
        df['contract'] = df['contract'].apply(
            lambda x: check_symbol_alias(re.findall("[A-Z]+", x)[0]) + x[len(re.findall("[A-Z]+", x)[0]):]
        )
        df = df.join(
            df['contract'].str.extract("(?P<underlying_contract>[A-Z]+[0-9]{4})-(?P<direction>[A-Z])-(?P<strike_price>[0-9]+)")
        )
        df['symbol'] = df['underlying_contract'].apply(lambda x: x[:-4])
        df = df.rename(
            columns={
                '合约代码': 'raw_contract',
                '昨结算': 'pre_settlement',
                '今开盘': 'open',
                '最高价': 'high',
                '最低价': 'low',
                '今收盘': 'close',
                '今结算': 'settlement',
                '成交量(手)': 'volume',
                '持仓量': 'open_interest',
                '增减量': 'open_interest_chg',
                '行权量': 'exec_volume',
                '成交额(万元)': 'turnover',
                '隐含波动率': 'iv',
                'DELTA': 'delta'
            }
        ).drop(
            columns=['涨跌1', '涨跌2']
        ).fillna(0)
        df = df.applymap(lambda x: str(x).replace(',', ''))
        df[[
            'volume', 'open_interest', 'open_interest_chg', 'exec_volume', 'pre_settlement', 'open', 'high', 'low',
            'close', 'settlement', 'turnover', 'iv', 'delta'
        ]] = df[[
            'volume', 'open_interest', 'open_interest_chg', 'exec_volume', 'pre_settlement', 'open', 'high', 'low',
            'close', 'settlement', 'turnover', 'iv', 'delta'
        ]].astype(float)
        df['turnover'] = df['turnover'] * 1e4
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['trading_date'] = pd.to_datetime(df['trading_date'])
        opt_df = df[[
            'raw_contract', 'contract', 'underlying_contract', 'direction', 'strike_price', 'symbol', 'datetime',
            'trading_date', 'delta', 'iv', 'exec_volume'
        ]].copy()

        df.drop(columns=['delta', 'iv', 'exec_volume'], inplace=True)
        return df, opt_df

    def save_option_md_data(self, df: pd.DataFrame):
        self.base.insert_dataframe(
            df, "pretreated_option_cn_md_data", "all_1d_CZCE",
            set_index=['underlying_contract', 'direction', 'strike_price', 'datetime'],
            partition=['trading_date']
        )

    def save_option_summary_data(self, df: pd.DataFrame):
        self.base.insert_dataframe(
            df, "pretreated_option_cn_md_data", "all_1d_opt_summary_CZCE",
            set_index=['underlying_contract', 'direction', 'strike_price', 'datetime'],
            partition=['trading_date']
        )

    def pretreat_future_position_rank_data(
            self,
            df: pd.DataFrame = None,
            start_with_last: bool = True
    ):
        dt_delta = -5
        start_date = (datetime.now() + timedelta(dt_delta)).strftime("%Y-%m-%d")
        if start_with_last:
            last_df1 = self.base.read_dataframe(
                "pretreated_future_cn_trade_data", "position_rank_by_contract_CZCE",
                ascending=[('trading_date', False)],
                filter_row_limit=1
            )
            last_df2 = self.base.read_dataframe(
                "pretreated_future_cn_trade_data", "position_rank_by_symbol_CZCE",
                ascending=[('trading_date', False)],
                filter_row_limit=1
            )
            start_date1 = last_df1.iloc[0]['trading_date'].strftime("%Y-%m-%d") if not last_df1.empty else None
            start_date2 = last_df2.iloc[0]['trading_date'].strftime("%Y-%m-%d") if not last_df2.empty else None
            start_date = None if start_date1 is None or start_date2 is None else min(start_date1, start_date2)
            fil_ = {"trading_date": {"gte": start_date}} if start_date is not None else None
        else:
            fil_ = {"trading_date": {"gte": start_date}}
        if df is None:
            df = self.base.read_dataframe(
                "raw_future_cn_trade_data",
                "position_rank_CZCE",
                filter_datetime=fil_
            )
        else:
            pass
        df = df.applymap(lambda x: x if type(x) != str else x.strip())

        df = df.applymap(lambda x: str(x).replace(',', '')).rename(
            columns={
                '名次': 'rank',
                '会员简称成交量（手）': 'broker_vol',
                '成交量（手）': 'vol',
                '成交量（手）增减量': 'vol_chg',
                '会员简称持买仓量': 'broker_long',
                '持买仓量': 'long',
                '持买仓量增减量': 'long_chg',
                '会员简称持卖仓量': 'broker_short',
                '持卖仓量': 'short',
                '持卖仓量增减量': 'short_chg',
            }
        ).replace(to_replace='-', value=np.nan)
        df[['broker_vol', 'broker_long', 'broker_short']] = df[['broker_vol', 'broker_long', 'broker_short']].applymap(
            lambda x: x.strip('（代客）') if isinstance(x, str) else x)
        df[[
            'vol', 'vol_chg', 'long', 'long_chg', 'short', 'short_chg'
        ]] = df[[
            'vol', 'vol_chg', 'long', 'long_chg', 'short', 'short_chg'
        ]].astype(float)
        df['trading_date'] = pd.to_datetime(df['trading_date'])
        df['rank'] = df['rank'].astype(int)

        df_s = df[df['data_type'] == '品种'].copy().rename(
            columns={'data_symbol': 'raw_symbol'}
        ).drop(columns=['data_type'])
        df_s['symbol'] = df_s['raw_symbol'].str.extract("([A-Z]+)")
        df_s['symbol'] = df_s['symbol'].apply(lambda x: x if len(x) <= 2 else x[-2:])
        df_s['symbol'] = [check_symbol_alias(s) for s in df_s['symbol']]

        df_c = df[df['data_type'] == '合约'].copy().rename(
            columns={'data_symbol': 'raw_contract'}
        ).drop(columns=['data_type'])
        contract_splits = df_c['raw_contract'].str.extract("(?P<par1>[A-Z]+)(?P<par2>[0-9]+)")
        y_ = df_c['trading_date'].apply(lambda x: str(x.year)[-2:]).astype(int)
        c_i = contract_splits['par2'].apply(lambda x: x[0])
        y_c = pd.Series(np.where(c_i.astype(int) != y_ % 10, y_ + 1, y_), index=df_c.index)
        df_c['contract'] = contract_splits['par1'] + y_c.apply(lambda x: str(x)[-2]) + contract_splits['par2']
        df_c['contract'] = [f"{check_symbol_alias(contract_[:-4])}{contract_[-4:]}" for contract_ in df_c['contract']]
        df_c['symbol'] = df_c['contract'].apply(lambda x: x[:-4])

        return df_s, df_c

    def save_future_position_rank_data(self, dfs: pd.DataFrame, dfc: pd.DataFrame):
        self.base.insert_dataframe(
            dfs, "pretreated_future_cn_trade_data", "position_rank_by_symbol_CZCE",
            set_index=['symbol', 'trading_date', 'rank'], partition=['trading_date']
        )
        self.base.insert_dataframe(
            dfc, "pretreated_future_cn_trade_data", "position_rank_by_contract_CZCE",
            set_index=['contract', 'trading_date', 'rank'], partition=['trading_date']
        )

    def pretreat_option_position_rank_data(
            self,
            df: pd.DataFrame = None,
            start_with_last: bool = True
    ):
        dt_delta = -5
        start_date = (datetime.now() + timedelta(dt_delta)).strftime("%Y-%m-%d")
        if start_with_last:
            last_df1 = self.base.read_dataframe(
                "pretreated_option_cn_trade_data", "position_rank_by_contract_CZCE",
                ascending=[('trading_date', False)],
                filter_row_limit=1
            )
            last_df2 = self.base.read_dataframe(
                "pretreated_option_cn_trade_data", "position_rank_by_symbol_CZCE",
                ascending=[('trading_date', False)],
                filter_row_limit=1
            )
            start_date1 = last_df1.iloc[0]['trading_date'].strftime("%Y-%m-%d") if not last_df1.empty else None
            start_date2 = last_df2.iloc[0]['trading_date'].strftime("%Y-%m-%d") if not last_df2.empty else None
            start_date = None if start_date1 is None or start_date2 is None else min(start_date1, start_date2)
            fil_ = {"trading_date": {"gte": start_date}} if start_date is not None else None
        else:
            fil_ = {"trading_date": {"gte": start_date}}
        if df is None:
            df = self.base.read_dataframe(
                "raw_option_cn_trade_data",
                "position_rank_CZCE",
                filter_datetime=fil_
            )
        else:
            pass
        df = df.applymap(lambda x: x if type(x) != str else x.strip())

        df = df.applymap(lambda x: str(x).replace(',', '')).rename(
            columns={
                '名次': 'rank',
                '会员简称成交量（手）': 'broker_vol',
                '成交量（手）': 'vol',
                '成交量（手）增减量': 'vol_chg',
                '会员简称持买仓量': 'broker_long',
                '持买仓量': 'long',
                '持买仓量增减量': 'long_chg',
                '会员简称持卖仓量': 'broker_short',
                '持卖仓量': 'short',
                '持卖仓量增减量': 'short_chg',
            }
        ).replace(to_replace='-', value=np.nan)
        df[['broker_vol', 'broker_long', 'broker_short']] = df[['broker_vol', 'broker_long', 'broker_short']].applymap(
            lambda x: x.strip('（代客）') if isinstance(x, str) else x)
        df[[
            'vol', 'vol_chg', 'long', 'long_chg', 'short', 'short_chg'
        ]] = df[[
            'vol', 'vol_chg', 'long', 'long_chg', 'short', 'short_chg'
        ]].astype(float)
        df['trading_date'] = pd.to_datetime(df['trading_date'])
        df['rank'] = df['rank'].astype(int)

        df_s = df[df['data_type'] == '品种'].copy().rename(
            columns={'data_symbol': 'raw_symbol'}
        ).drop(columns=['data_type'])
        df_s['symbol'] = df_s['raw_symbol'].str.extract("(权[A-Z]+)")
        df_s['direction'] = df_s['symbol'].apply(lambda x: x[-1])
        df_s['symbol'] = df_s['symbol'].apply(lambda x: x[1:-1])
        df_s['symbol'] = [check_symbol_alias(s) for s in df_s['symbol']]

        df_c = df[df['data_type'] == '合约'].copy().rename(
            columns={'data_symbol': 'raw_contract'}
        ).drop(columns=['data_type'])
        df_c['direction'] = df_c['raw_contract'].apply(lambda x: x[-1])
        df_c['contract'] = df_c['raw_contract'].apply(lambda x: x[:-1])
        contract_splits = df_c['contract'].str.extract("(?P<par1>[A-Z]+)(?P<par2>[0-9]+)")
        y_ = df_c['trading_date'].apply(lambda x: str(x.year)[-2:]).astype(int)
        c_i = contract_splits['par2'].apply(lambda x: x[0])
        y_c = pd.Series(np.where(c_i.astype(int) != y_ % 10, y_ + 1, y_), index=df_c.index)
        df_c['contract'] = contract_splits['par1'] + y_c.apply(lambda x: str(x)[-2]) + contract_splits['par2']
        df_c['contract'] = [f"{check_symbol_alias(contract_[:-4])}{contract_[-4:]}" for contract_ in df_c['contract']]
        df_c['symbol'] = df_c['contract'].apply(lambda x: x[:-4])

        return df_s, df_c

    def save_option_position_rank_data(self, dfs: pd.DataFrame, dfc: pd.DataFrame):
        self.base.insert_dataframe(
            dfs, "pretreated_option_cn_trade_data", "position_rank_by_symbol_CZCE",
            set_index=['symbol', 'direction', 'trading_date', 'rank'], partition=['trading_date']
        )
        self.base.insert_dataframe(
            dfc, "pretreated_option_cn_trade_data", "position_rank_by_contract_CZCE",
            set_index=['contract', 'direction', 'trading_date', 'rank'], partition=['trading_date']
        )


if __name__ == "__main__":
    ptr = PretreatCZCE()
    # df = ptr.pretreat_option_contract_info_data()
    # ptr.save_option_contract_info_data(df)
    # df = ptr.pretreat_future_contract_info_data()
    # ptr.save_future_contract_info_data(df)
    # mdf, odf = ptr.pretreat_option_md_data()
    # ptr.save_option_md_data(mdf)
    # ptr.save_option_summary_data(odf)
    # df = ptr.pretreat_future_md_data()
    # ptr.save_future_md_data(df)

    # for d in [
    #     "raw_future_cn_model_data",
    #     "pretreated_future_cn_model_data",
    #     "processed_future_cn_model_data",
    # ]:
    #     for t in ptr.base.get_table_names(d):
    #
    #         ptr.base.del_row(
    #             db_name=d,
    #             tb_name=t,
    #             filter_datetime={'trading_date': {'eq': '2024-05-27'}}
    #         )

    # res = ptr.pretreat_future_contract_info_data()
    # ptr.save_future_contract_info_data(res)
    reso = ptr.pretreat_option_contract_info_data()
    ptr.save_option_contract_info_data(reso)