import re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import utils.database.unified_db_control as udc
from data.data_utils.check_alias import check_symbol_alias

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class Pretreat100Ppi:
    def __init__(self):
        self.base = udc.UnifiedControl(db_type='base')

    def __initiate_namings_table__(self):
        raw_df = self.base.read_dataframe(
            db_name="raw_spot_cn_md_data",
            tb_name="standard_price_100ppi",
        )
        namings = raw_df[['spec']].drop_duplicates(subset=['spec']).assign(symbol=None)
        self.base.insert_dataframe(
            namings, "processed_spot_cn_meta_data", "standard_symbol_naming_reference_100ppi"
        )

    def pretreat_standard_spot_price_data(self):
        last_df = self.base.read_dataframe(
            "pretreated_spot_cn_md_data", "standard_price_100ppi",
            ascending=[('record_date', False)],
            filter_row_limit=1
        )
        start_date = last_df.iloc[0]['record_date'].strftime("%Y-%m-%d") if not last_df.empty else None
        filt_date = {'record_date': {'gte': start_date}} if start_date is not None else None
        raw_df = self.base.read_dataframe(
            db_name="raw_spot_cn_md_data",
            tb_name="standard_price_100ppi",
            filter_datetime=filt_date
        )
        namings = self.base.read_dataframe(
            "processed_spot_cn_meta_data", "standard_symbol_naming_reference_100ppi"
        )
        if namings.empty:
            self.__initiate_namings_table__()
            raise FileNotFoundError("The naming reference table is not found in database, please initiate first.")
        namings = namings.set_index('spec')['symbol']
        raw_df.set_index('spec', inplace=True)
        raw_df['symbol'] = namings.loc[namings.index.intersection(raw_df.index)].reindex(raw_df.index)
        raw_df['price'] = raw_df['price'].astype(float)
        raw_df.set_index('symbol', inplace=True, drop=False)
        raw_df = self.__proc_JD__(raw_df)
        raw_df = self.__proc_SH__(raw_df)
        raw_df = self.__proc_I__(raw_df)
        raw_df = self.__proc_FG__(raw_df)
        raw_df = self.__proc_LH__(raw_df)
        return raw_df.reset_index(drop=True)

    @staticmethod
    def __proc_JD__(data: pd.DataFrame):
        """
        鸡蛋现货为元/公斤，鸡蛋期货为元/500千克
        """
        data.loc['JD', 'price'] = data.loc['JD', 'price'] * 500
        return data

    @staticmethod
    def __proc_SH__(data: pd.DataFrame):
        """
        烧碱现货价格是：32%液碱，烧碱期货价格是：100%纯碱（折百价）
        """
        data.loc['SH', 'price'] = data.loc['SH', 'price'] / 0.32
        return data

    @staticmethod
    def __proc_I__(data: pd.DataFrame):
        """
        铁矿石现货价格是：湿吨，铁矿石期货价格是：干吨
        这里我取了含水量4%作为统一入库处理标准，不代表实际真实的数据
        """
        data.loc['I', 'price'] = data.loc['I', 'price'] / 0.96
        return data

    @staticmethod
    def __proc_FG__(data: pd.DataFrame):
        """
        类型:浮法玻璃;厚度:5mm;允许偏差:±0.2
        原始数据单位为元/平方米，按照一吨玻璃体积0.4立方米计算对应玻璃面积为5mm厚度80平米
        """
        data.loc['FG', 'price'] = data.loc['FG', 'price'] * 80
        return data

    @staticmethod
    def __proc_LH__(data: pd.DataFrame):
        """
        品种:外三元;体重:90-100kg;
        近似取1000kg
        """
        data.loc['LH', 'price'] = data.loc['LH', 'price'] * 1000
        return data

    def save_standard_spot_price_data(self, df: pd.DataFrame):
        self.base.insert_dataframe(
            df, "pretreated_spot_cn_md_data", "standard_price_100ppi",
            set_index=['symbol', 'record_date']
        )


if __name__ == "__main__":
    p = Pretreat100Ppi()
    p.__initiate_namings_table__()