import re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import utils.database.unified_db_control as udc
from data.data_utils.check_alias import check_symbol_alias

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class PretreatSHFE:
    def __init__(self):
        self.base = udc.UnifiedControl(db_type='base')

    def pretreat_option_contract_info_data(
            self,
            df: pd.DataFrame = None,
    ):
        start_date = datetime.now().strftime("%Y-%m-%d")
        fil_ = {"EXPIREDATE": {"gte": start_date}}
        if df is None:
            df = self.base.read_dataframe(
                "raw_option_cn_meta_data",
                "contract_info_SHFE",
                filter_datetime=fil_
            )
        df = df.applymap(lambda x: x if type(x) != str else x.strip())

        contract_splits = df['INSTRUMENTID'].str.extract("(?P<par1>[A-Z]+)(?P<par2>[0-9]+)(?P<par3>[C, P])(?P<par4>[0-9]+)")
        df['contract'] = contract_splits['par1'] + contract_splits['par2'] + '-' + contract_splits['par3'] + '-' + contract_splits['par4']
        df['direction'] = contract_splits['par3']
        df['strike_price'] = contract_splits['par4']
        df = df.rename(
            columns={
                'COMMODITYID': 'raw_symbol',
                'EXCHANGEID': 'raw_exchange',
                'INSTRUMENTID': 'raw_contract',
                'OPENDATE': 'listed_date',
                'PRICETICK': 'tick_price',
                'TRADEUNIT': 'trade_unit',
                'EXPIREDATE': 'last_trading_date',
            }
        ).drop(columns=['SETTLEMENTGROUPID', 'TRADINGDAY', 'UPDATE_DATE', 'commodityName', 'id'])
        df['contract'] = df['contract'].apply(
            lambda x: check_symbol_alias(re.findall("[A-Z]+", x)[0]) + x[len(re.findall("[A-Z]+", x)[0]):]
        )
        df['underlying_contract'] = df['contract'].apply(lambda x: x.split('-')[0])
        df['symbol'] = df['underlying_contract'].apply(lambda x: x[:-4])
        df['exchange'] = 'SHFE'
        for tc in [tc for tc in df.columns if 'date' in tc]:
            df[tc] = pd.to_datetime(df[tc], errors='ignore')
        return df

    def save_option_contract_info_data(
            self, df: pd.DataFrame
    ):
        self.base.insert_dataframe(
            df, "pretreated_option_cn_meta_data", "contract_info_SHFE",
            set_index=["contract", "listed_date"],
            partition=['listed_date']
        )

    def pretreat_future_contract_info_data(
            self,
            df: pd.DataFrame = None,
    ):
        start_date = datetime.now().strftime("%Y-%m-%d")
        fil_ = {"EXPIREDATE": {"gte": start_date}}
        if df is None:
            df = self.base.read_dataframe(
                "raw_future_cn_meta_data",
                "contract_info_SHFE",
                filter_datetime=fil_
            )
        df = df.applymap(lambda x: x if type(x) != str else x.strip())

        df['contract'] = df['INSTRUMENTID'].str.upper()
        df = df.rename(
            columns={
                'INSTRUMENTID': 'raw_contract',
                'OPENDATE': 'listed_date',
                'EXPIREDATE': 'last_trading_date',
                'STARTDELIVDATE': 'delivery_start_date',
                'ENDDELIVDATE': 'delivery_end_date'
            }
        ).drop(columns=['BASISPRICE', 'TRADINGDAY', 'UPDATE_DATE', 'id'])
        df['contract'] = [f"{check_symbol_alias(contract_[:-4])}{contract_[-4:]}" for contract_ in df['contract']]
        df['symbol'] = df['contract'].apply(lambda x: x[:-4])
        df['exchange'] = 'SHFE'
        for tc in [tc for tc in df.columns if 'date' in tc]:
            df[tc] = pd.to_datetime(df[tc])

        return df

    def save_future_contract_info_data(
            self, df: pd.DataFrame
    ):
        self.base.insert_dataframe(
            df, "pretreated_future_cn_meta_data", "contract_info_SHFE",
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
                "pretreated_future_cn_md_data", "all_1d_SHFE",
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
                "all_1d_SHFE",
                filter_datetime=fil_
            )
        else:
            pass
        df = df.applymap(lambda x: x if type(x) != str else x.strip())

        df = df[~df['PRODUCTID'].str.contains('|'.join(['_tas', 'efp']))].copy()
        df['raw_contract'] = df['PRODUCTGROUPID'].str.strip() + df['DELIVERYMONTH'].str.strip()
        df['contract'] = df['PRODUCTGROUPID'].str.strip().str.upper() + df['DELIVERYMONTH'].str.strip()
        df['contract'] = [f"{check_symbol_alias(contract_[:-4])}{contract_[-4:]}" for contract_ in df['contract']]
        df['symbol'] = df['contract'].apply(lambda x: x[:-4])
        df = df.rename(
            columns={
                'PRESETTLEMENTPRICE': 'pre_settlement',
                'OPENPRICE': 'open',
                'HIGHESTPRICE': 'high',
                'LOWESTPRICE': 'low',
                'CLOSEPRICE': 'close',
                'SETTLEMENTPRICE': 'settlement',
                'VOLUME': 'volume',
                'OPENINTEREST': 'open_interest',
                'OPENINTERESTCHG': 'open_interest_chg',
                'TURNOVER': 'turnover',
            }
        ).drop(
            columns=[
                'PRODUCTID', 'PRODUCTGROUPID', 'PRODUCTSORTNO', 'PRODUCTNAME', 'DELIVERYMONTH', 'ZD1_CHG', 'ZD2_CHG',
                'TASVOLUME', 'ORDERNO', 'ORDERNO2', 'PRODUCTCLASS'
            ]
        ).fillna(0)
        df = df.applymap(lambda x: str(x).replace(',', '')).replace(to_replace='-', value=np.nan).replace(
            to_replace='', value=np.nan
        )
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
            df, "pretreated_future_cn_md_data", "all_1d_SHFE",
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
                "pretreated_option_cn_md_data", "all_1d_SHFE",
                ascending=[('datetime', False)],
                filter_row_limit=1
            )
            last_df2 = self.base.read_dataframe(
                "pretreated_option_cn_md_data", "all_1d_opt_summary_SHFE",
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
                "all_1d_SHFE",
                filter_datetime=fil_
            )
            iv_df = self.base.read_dataframe(
                "raw_option_cn_md_data",
                "all_1d_iv_SHFE",
                filter_datetime=fil_
            )
        else:
            pass
        md_df = md_df.applymap(lambda x: x if type(x) != str else x.strip())
        iv_df = iv_df.applymap(lambda x: x if type(x) != str else x.strip())

        iv_df['INSTRUMENTID'] = iv_df['INSTRUMENTID'].str.strip().str.upper()
        iv_df = iv_df.set_index(['datetime', 'INSTRUMENTID'])['SIGMA'] * 100

        md_df['INSTRUMENTID'] = md_df['INSTRUMENTID'].str.strip()
        md_df['contract'] = md_df['INSTRUMENTID'].str.upper()
        contract_splits = md_df['contract'].str.extract("(?P<par1>[A-Z]+)(?P<par2>[0-9]+)(?P<par3>[C, P])(?P<par4>[0-9]+)")
        md_df['contract'] = contract_splits['par1'] + contract_splits['par2'] + '-' + contract_splits['par3'] + '-' + contract_splits['par4']
        md_df['contract'] = [f"{check_symbol_alias(contract_[:-4])}{contract_[-4:]}" for contract_ in md_df['contract']]

        md_df['direction'] = contract_splits['par3']
        md_df['strike_price'] = contract_splits['par4']
        md_df['underlying_contract'] = md_df['contract'].apply(lambda x: x.split('-')[0])
        md_df['symbol'] = md_df['underlying_contract'].apply(lambda x: x[:-4])
        md_df.set_index(['datetime', 'underlying_contract'], drop=False, inplace=True)
        md_df['iv'] = iv_df.loc[iv_df.index.intersection(md_df.index)].reindex(md_df.index)
        md_df.reset_index(drop=True, inplace=True)
        md_df = md_df.rename(
            columns={
                'INSTRUMENTID': 'raw_contract',
                'PRESETTLEMENTPRICE': 'pre_settlement',
                'OPENPRICE': 'open',
                'HIGHESTPRICE': 'high',
                'LOWESTPRICE': 'low',
                'CLOSEPRICE': 'close',
                'SETTLEMENTPRICE': 'settlement',
                'VOLUME': 'volume',
                'OPENINTEREST': 'open_interest',
                'OPENINTERESTCHG': 'open_interest_chg',
                'TURNOVER': 'turnover',
                'EXECVOLUME': 'exec_volume',
                'DELTA': 'delta'
            }
        ).drop(
            columns=[
                'PRODUCTID', 'PRODUCTSORTNO', 'PRODUCTNAME', 'ZD1_CHG', 'ZD2_CHG', 'ORDERNO', 'UNDERLYINGINSTRID',
                'STRIKEPRICE', 'OPTIONSTYPE', 'PRODUCTGROUPID'
            ]
        ).fillna(0)
        md_df = md_df.applymap(lambda x: str(x).replace(',', '')).replace(to_replace='-', value=np.nan).replace(
            to_replace='', value=np.nan
        )
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
            df, "pretreated_option_cn_md_data", "all_1d_SHFE",
            set_index=['underlying_contract', 'direction', 'strike_price', 'datetime'],
            partition=['trading_date']
        )

    def save_option_summary_data(self, df: pd.DataFrame):
        self.base.insert_dataframe(
            df, "pretreated_option_cn_md_data", "all_1d_opt_summary_SHFE",
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
                "pretreated_future_cn_trade_data", "position_rank_by_contract_SHFE",
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
                "position_rank_SHFE",
                filter_datetime=fil_
            )
        else:
            pass
        df = df.applymap(lambda x: x if type(x) != str else x.strip())

        df['raw_contract'] = df['INSTRUMENTID'].str.strip()
        df['contract'] = df['raw_contract'].str.upper()
        df = df.applymap(lambda x: str(x).replace(',', '')).rename(
            columns={
                'RANK': 'rank',
                'PARTICIPANTABBR1': 'broker_vol',
                'CJ1': 'vol',
                'CJ1_CHG': 'vol_chg',
                'PARTICIPANTABBR2': 'broker_long',
                'CJ2': 'long',
                'CJ2_CHG': 'long_chg',
                'PARTICIPANTABBR3': 'broker_short',
                'CJ3': 'short',
                'CJ3_CHG': 'short_chg',
            }
        ).drop(
            columns=['INSTRUMENTID', 'PRODUCTSORTNO', 'PARTICIPANTID1', 'PARTICIPANTID2', 'PARTICIPANTID3', 'PRODUCTNAME']
        ).replace(to_replace='-', value=np.nan).replace(to_replace='', value=np.nan)
        df[['broker_vol', 'broker_long', 'broker_short']] = df[['broker_vol', 'broker_long', 'broker_short']].applymap(
            lambda x: x.strip('（代客）') if isinstance(x, str) else x)
        df[[
            'vol', 'vol_chg', 'long', 'long_chg', 'short', 'short_chg'
        ]] = df[[
            'vol', 'vol_chg', 'long', 'long_chg', 'short', 'short_chg'
        ]].astype(float)
        df['trading_date'] = pd.to_datetime(df['trading_date'])
        df['rank'] = df['rank'].astype(int)
        df.drop(columns=['datetime'], inplace=True)

        df['contract'] = [f"{check_symbol_alias(contract_[:-4])}{contract_[-4:]}" for contract_ in df['contract']]

        return df

    def save_future_position_rank_data(self, dfc: pd.DataFrame):
        self.base.insert_dataframe(
            dfc, "pretreated_future_cn_trade_data", "position_rank_by_contract_SHFE",
            set_index=['contract', 'trading_date', 'rank'], partition=['trading_date']
        )


if __name__ == "__main__":
    ptr = PretreatSHFE()

    df = ptr.pretreat_option_contract_info_data()
    ptr.save_option_contract_info_data(df)
    df = ptr.pretreat_future_contract_info_data()
    ptr.save_future_contract_info_data(df)
    mdf, odf = ptr.pretreat_option_md_data()
    ptr.save_option_md_data(mdf)
    ptr.save_option_summary_data(odf)
    df = ptr.pretreat_future_md_data()
    ptr.save_future_md_data(df)

    res = ptr.pretreat_future_position_rank_data()
    ptr.save_future_position_rank_data(res)