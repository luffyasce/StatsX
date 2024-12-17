import re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import utils.database.unified_db_control as udc
from data.data_utils.check_alias import check_symbol_alias

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class PretreatDCE:
    def __init__(self):
        self.base = udc.UnifiedControl(db_type='base')

    def pretreat_option_contract_info_data(
            self,
            df: pd.DataFrame = None,
    ):
        start_date = datetime.now().strftime("%Y-%m-%d")
        fil_ = {"最后交易日": {"gte": start_date}}
        if df is None:
            df = self.base.read_dataframe(
                "raw_option_cn_meta_data",
                "contract_info_DCE",
                filter_datetime=fil_
            )
        df = df.applymap(lambda x: x if type(x) != str else x.strip())

        contract_splits = df['合约代码'].str.extract("(?P<par1>[A-Z]+)(?P<par2>[0-9]+)(?P<par3>[C, P])(?P<par4>[0-9]+)")
        df['contract'] = contract_splits['par1'] + contract_splits['par2'] + '-' + contract_splits['par3'] + '-' + contract_splits['par4']
        df['direction'] = contract_splits['par3']
        df['strike_price'] = contract_splits['par4']
        df = df.rename(
            columns={
                '合约代码': 'raw_contract',
                '开始交易日': 'listed_date',
                '最小变动价位': 'tick_price',
                '交易单位': 'trade_unit',
                '最后交易日': 'last_trading_date',
                '最后交割日': 'delivery_end_date',
            }
        ).drop(columns=['品种'])
        df['contract'] = df['contract'].apply(
            lambda x: check_symbol_alias(re.findall("[A-Z]+", x)[0]) + x[len(re.findall("[A-Z]+", x)[0]):]
        )
        df['underlying_contract'] = df['contract'].apply(lambda x: x.split('-')[0])
        df['symbol'] = df['underlying_contract'].apply(lambda x: x[:-4])
        df['exchange'] = 'DCE'
        for tc in [tc for tc in df.columns if 'date' in tc]:
            df[tc] = pd.to_datetime(df[tc], errors='ignore')
        return df

    def save_option_contract_info_data(
            self, df: pd.DataFrame
    ):
        self.base.insert_dataframe(
            df, "pretreated_option_cn_meta_data", "contract_info_DCE",
            set_index=["contract", "listed_date"],
            partition=['listed_date']
        )

    def pretreat_future_contract_info_data(
            self,
            df: pd.DataFrame = None,
    ):
        start_date = datetime.now().strftime("%Y-%m-%d")
        fil_ = {"最后交易日": {"gte": start_date}}
        if df is None:
            df = self.base.read_dataframe(
                "raw_future_cn_meta_data",
                "contract_info_DCE",
                filter_datetime=fil_
            )
        df = df.applymap(lambda x: x if type(x) != str else x.strip())

        contract_splits = df['合约代码'].str.extract("(?P<par1>[A-Z]+)(?P<par2>[0-9]+)")
        df['contract'] = df['合约代码'].str.upper()
        df = df.rename(
            columns={
                '合约代码': 'raw_contract',
                '开始交易日': 'listed_date',
                '最小变动价位': 'tick_price',
                '交易单位': 'trade_unit',
                '最后交易日': 'last_trading_date',
                '最后交割日': 'delivery_end_date'
            }
        ).drop(columns=['品种'])
        df['contract'] = [f"{check_symbol_alias(contract_[:-4])}{contract_[-4:]}" for contract_ in df['contract']]
        df['symbol'] = df['contract'].apply(lambda x: x[:-4])
        df['exchange'] = 'DCE'
        for tc in [tc for tc in df.columns if 'date' in tc]:
            df[tc] = pd.to_datetime(df[tc])
        return df

    def save_future_contract_info_data(
            self, df: pd.DataFrame
    ):
        self.base.insert_dataframe(
            df, "pretreated_future_cn_meta_data", "contract_info_DCE",
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
                "pretreated_future_cn_md_data", "all_1d_DCE",
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
                "all_1d_DCE",
                filter_datetime=fil_
            )
        else:
            pass
        df = df.applymap(lambda x: x if type(x) != str else x.strip())

        df['contract'] = df['合约名称'].str.upper()
        df['contract'] = [f"{check_symbol_alias(contract_[:-4])}{contract_[-4:]}" for contract_ in df['contract']]
        df['symbol'] = df['contract'].apply(lambda x: x[:-4])
        df = df.rename(
            columns={
                '合约名称': 'raw_contract',
                '前结算价': 'pre_settlement',
                '开盘价': 'open',
                '最高价': 'high',
                '最低价': 'low',
                '收盘价': 'close',
                '结算价': 'settlement',
                '成交量': 'volume',
                '持仓量': 'open_interest',
                '持仓量变化': 'open_interest_chg',
                '成交额': 'turnover',
            }
        ).drop(
            columns=['涨跌1', '涨跌', '商品名称']
        ).fillna(0)
        df = df.applymap(lambda x: str(x).replace(',', '')).replace(to_replace='-', value=np.nan)
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
            df, "pretreated_future_cn_md_data", "all_1d_DCE",
            set_index=['contract', 'datetime'],
            partition=['trading_date']
        )

    def pretreat_option_md_data(
            self,
            md_df: pd.DataFrame = None,
            iv_df: pd.DataFrame = None,
            start_with_last: bool = True
    ):
        dt_delta = -5
        start_date = (datetime.now() + timedelta(dt_delta)).strftime("%Y-%m-%d")
        if start_with_last:
            last_df1 = self.base.read_dataframe(
                "pretreated_option_cn_md_data", "all_1d_DCE",
                ascending=[('datetime', False)],
                filter_row_limit=1
            )
            last_df2 = self.base.read_dataframe(
                "pretreated_option_cn_md_data", "all_1d_opt_summary_DCE",
                ascending=[('datetime', False)],
                filter_row_limit=1
            )
            start_date1 = last_df1.iloc[0]['datetime'].strftime("%Y-%m-%d") if not last_df1.empty else None
            start_date2 = last_df2.iloc[0]['datetime'].strftime("%Y-%m-%d") if not last_df2.empty else None
            start_date = None if start_date1 is None or start_date2 is None else min(start_date1, start_date2)
            fil_ = {"datetime": {"gte": start_date}} if start_date is not None else None
        else:
            fil_ = {"datetime": {"gte": start_date}}
        if md_df is None:
            md_df = self.base.read_dataframe(
                "raw_option_cn_md_data",
                "all_1d_DCE",
                filter_datetime=fil_
            )
            iv_df = self.base.read_dataframe(
                "raw_option_cn_md_data",
                "all_1d_iv_DCE",
                filter_datetime=fil_
            )
        else:
            pass
        md_df = md_df.applymap(lambda x: x if type(x) != str else x.strip())
        iv_df = iv_df.applymap(lambda x: x if type(x) != str else x.strip())

        iv_df['合约系列'] = iv_df['合约系列'].str.upper()
        iv_df = iv_df.set_index(['datetime', '合约系列'])['隐含波动率(%)']

        md_df['contract'] = md_df['合约名称'].str.upper()
        contract_splits = md_df['contract'].str.extract("(?P<par1>[A-Z]+)(?P<par2>[0-9]+)-(?P<par3>[C, P])-(?P<par4>[0-9]+)")
        md_df['contract'] = md_df['contract'].apply(
            lambda x: check_symbol_alias(re.findall("[A-Z]+", x)[0]) + x[len(re.findall("[A-Z]+", x)[0]):]
        )

        md_df['direction'] = contract_splits['par3']
        md_df['strike_price'] = contract_splits['par4']
        md_df['underlying_contract'] = md_df['contract'].apply(lambda x: x.split('-')[0])
        md_df['symbol'] = md_df['underlying_contract'].apply(lambda x: x[:-4])
        md_df.set_index(['datetime', 'underlying_contract'], drop=False, inplace=True)
        md_df['iv'] = iv_df.loc[iv_df.index.intersection(md_df.index)].reindex(md_df.index)
        md_df.reset_index(drop=True, inplace=True)
        md_df = md_df.rename(
            columns={
                '合约名称': 'raw_contract',
                '前结算价': 'pre_settlement',
                '开盘价': 'open',
                '最高价': 'high',
                '最低价': 'low',
                '收盘价': 'close',
                '结算价': 'settlement',
                '成交量': 'volume',
                '持仓量': 'open_interest',
                '持仓量变化': 'open_interest_chg',
                '成交额': 'turnover',
                '行权量': 'exec_volume',
                'Delta': 'delta'
            }
        ).drop(
            columns=['涨跌1', '涨跌', '商品名称']
        ).fillna(0)
        md_df = md_df.applymap(lambda x: str(x).replace(',', '')).replace(to_replace='-', value=np.nan)
        md_df[[
            'volume', 'open_interest', 'open_interest_chg', 'exec_volume', 'pre_settlement', 'open', 'high', 'low',
            'close', 'settlement', 'turnover', 'iv', 'delta'
        ]] = md_df[[
            'volume', 'open_interest', 'open_interest_chg', 'exec_volume', 'pre_settlement', 'open', 'high', 'low',
            'close', 'settlement', 'turnover', 'iv', 'delta'
        ]].astype(float)
        md_df['turnover'] = md_df['turnover'] * 1e4
        md_df['datetime'] = pd.to_datetime(md_df['datetime'])
        md_df['trading_date'] = pd.to_datetime(md_df['trading_date'])
        opt_df = md_df[[
            'raw_contract', 'contract', 'underlying_contract', 'direction', 'strike_price', 'symbol', 'datetime',
            'trading_date', 'delta', 'iv', 'exec_volume'
        ]].copy()

        md_df.drop(columns=['delta', 'iv', 'exec_volume'], inplace=True)
        return md_df, opt_df

    def save_option_md_data(self, df: pd.DataFrame):
        self.base.insert_dataframe(
            df, "pretreated_option_cn_md_data", "all_1d_DCE",
            set_index=['underlying_contract', 'direction', 'strike_price', 'datetime'],
            partition=['trading_date']
        )

    def save_option_summary_data(self, df: pd.DataFrame):
        self.base.insert_dataframe(
            df, "pretreated_option_cn_md_data", "all_1d_opt_summary_DCE",
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
            last_df = self.base.read_dataframe(
                "pretreated_future_cn_trade_data", "position_rank_by_contract_DCE",
                ascending=[('trading_date', False)],
                filter_row_limit=1
            )
            start_date = last_df.iloc[0]['trading_date'].strftime("%Y-%m-%d") if not last_df.empty else None
            fil_ = {"trading_date": {"gte": start_date}} if start_date is not None else None
        else:
            fil_ = {"trading_date": {"gte": start_date}}
        if df is None:
            df = self.base.read_dataframe(
                "raw_future_cn_trade_data",
                "position_rank_DCE",
                filter_datetime=fil_
            )
        else:
            pass
        df = df.applymap(lambda x: x if type(x) != str else x.strip())

        df = df.applymap(lambda x: str(x).replace(',', '')).rename(
            columns={
                '名次': 'rank',
                '会员简称成交量': 'broker_vol',
                '成交量': 'vol',
                '成交量增减': 'vol_chg',
                '会员简称持买单量': 'broker_long',
                '持买单量': 'long',
                '持买单量增减': 'long_chg',
                '会员简称持卖单量': 'broker_short',
                '持卖单量': 'short',
                '持卖单量增减': 'short_chg',
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
        df = df.dropna(subset=['rank'])
        df['rank'] = df['rank'].astype(int)
        df.drop(columns=['datetime'], inplace=True)

        df['raw_contract'] = df['contract'].str.lower()
        df['contract'] = [f"{check_symbol_alias(contract_[:-4])}{contract_[-4:]}" for contract_ in df['contract']]

        return df

    def save_future_position_rank_data(self, dfc: pd.DataFrame):
        self.base.insert_dataframe(
            dfc, "pretreated_future_cn_trade_data", "position_rank_by_contract_DCE",
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
            last_df = self.base.read_dataframe(
                "pretreated_option_cn_trade_data", "position_rank_by_contract_DCE",
                ascending=[('trading_date', False)],
                filter_row_limit=1
            )
            start_date = last_df.iloc[0]['trading_date'].strftime("%Y-%m-%d") if not last_df.empty else None
            fil_ = {"trading_date": {"gte": start_date}} if start_date is not None else None
        else:
            fil_ = {"trading_date": {"gte": start_date}}
        if df is None:
            df = self.base.read_dataframe(
                "raw_option_cn_trade_data",
                "position_rank_DCE",
                filter_datetime=fil_
            )
        else:
            pass
        df = df.applymap(lambda x: x if type(x) != str else x.strip())
        df = df.dropna(subset='名次')
        df = df.applymap(lambda x: str(x).replace(',', '')).rename(
            columns={
                '名次': 'rank',
                '会员简称成交量': 'broker_vol',
                '成交量': 'vol',
                '成交量增减': 'vol_chg',
                '会员简称持买单量': 'broker_long',
                '持买单量': 'long',
                '持买单量增减': 'long_chg',
                '会员简称持卖单量': 'broker_short',
                '持卖单量': 'short',
                '持卖单量增减': 'short_chg',
            }
        ).replace(to_replace='-', value=np.nan)
        df[['broker_vol', 'broker_long', 'broker_short']] = df[['broker_vol', 'broker_long', 'broker_short']].applymap(
            lambda x: x.strip('（代客）') if isinstance(x, str) else x)
        df = df.dropna(subset=['broker_vol', 'broker_long', 'broker_short'])
        df[[
            'vol', 'vol_chg', 'long', 'long_chg', 'short', 'short_chg'
        ]] = df[[
            'vol', 'vol_chg', 'long', 'long_chg', 'short', 'short_chg'
        ]].astype(float)
        df['trading_date'] = pd.to_datetime(df['trading_date'])
        df['rank'] = df['rank'].astype(int)

        df['direction'] = np.where(df['opt_type'] == '看涨期权', 'C', 'P')
        df['raw_contract'] = df['contract'].str.lower()
        df['contract'] = [f"{check_symbol_alias(contract_[:-4])}{contract_[-4:]}" for contract_ in df['contract']]

        df.drop(columns=['datetime', 'opt_type'], inplace=True)
        return df

    def save_option_position_rank_data(self, dfc: pd.DataFrame):
        self.base.insert_dataframe(
            dfc, "pretreated_option_cn_trade_data", "position_rank_by_contract_DCE",
            set_index=['contract', 'direction', 'trading_date', 'rank'], partition=['trading_date']
        )


if __name__ == "__main__":
    ptr = PretreatDCE()
    # df = ptr.pretreat_option_contract_info_data()
    # ptr.save_option_contract_info_data(df)
    # df = ptr.pretreat_future_contract_info_data()
    # ptr.save_future_contract_info_data(df)
    # mdf, odf = ptr.pretreat_option_md_data()
    # ptr.save_option_md_data(mdf)
    # ptr.save_option_summary_data(odf)
    # df = ptr.pretreat_future_md_data()
    # ptr.save_future_md_data(df)

    # res = ptr.pretreat_future_position_rank_data(start_with_last=True)
    # ptr.save_future_position_rank_data(res)
    res = ptr.pretreat_future_position_rank_data(start_with_last=True)
