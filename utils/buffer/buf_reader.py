"""
To read data from redis
"""
from typing import Union
import pandas as pd
import pickle
import zlib
import json
from utils.buffer.redis_handle import Redis
from utils.database.unified_db_control import UnifiedControl
from utils.tool.logger import log
from utils.tool.decorator import private, auto_retry
from utils.tool.decorator import singleton

logger = log(__file__, 'utils')


@singleton
class Reader:
    def __init__(self):
        self.redis = Redis()
        self.app = UnifiedControl(db_type='app')
        self.base = UnifiedControl(db_type='base')

    @auto_retry()
    def flush_buffer(self, db: int):
        """
        If encountered AttributeError when calling read method from redis, and the error args show sth like :
            Can’t get attribute ‘new_block’ xxx
        Then try run this method to clean buffer.
        Such Error could be caused by different version of pandas in between different devices
        where the data was generated
        """
        self.redis.flush(db)

    @auto_retry()
    def read_buffer(
            self,
            db_name: str,
            tb_name: str,
            filter_datetime: dict = None,
            filter_keyword: dict = None,
            filter_columns: list = None,
            ascending: list = None,
            filter_row_limit: int = None,
            db: int = 3,
            otro_vez: bool = True,
            db_type: str = 'app',
            **kwargs
    ):
        res_ = self.redis.get_hash(
            db=db,
            name=tb_name,
            decode=False,
            k=None
        )
        mat_datetime = json.dumps(filter_datetime) if filter_datetime is not None else ""
        mat_kw = json.dumps(filter_keyword) if filter_keyword is not None else ""
        mat_col = json.dumps(filter_columns) if filter_columns is not None else ""
        mat_asc = json.dumps(ascending) if ascending is not None else ""
        mat_rl = json.dumps(filter_row_limit) if filter_row_limit is not None else ""
        if res_ == {} or (
                bool(
                    (
                            res_[b"filter_datetime"].decode() == mat_datetime
                    ) * (
                            res_[b"filter_keyword"].decode() == mat_kw
                    ) * (
                            res_[b"filter_columns"].decode() == mat_col
                    ) * (
                            res_[b"ascending"].decode() == mat_asc
                    ) * (
                            res_[b"filter_row_limit"].decode() == mat_rl
                    )
                ) is False
        ) or otro_vez:
            udc = self.app if db_type == 'app' else self.base
            if 'private' in kwargs.keys() and kwargs['private'] is True:
                @private(db_ctl=udc)
                def _read(**kwd):
                    _df = udc.read_dataframe(
                        **kwd
                    )
                    return _df
            else:
                def _read(**kwd):
                    _df = udc.read_dataframe(
                        **kwd
                    )
                    return _df
            df = _read(
                db_name=db_name,
                tb_name=tb_name,
                filter_datetime=filter_datetime,
                filter_keyword=filter_keyword,
                filter_columns=filter_columns,
                ascending=ascending,
                filter_row_limit=filter_row_limit,
            )
            self.redis.set_hash(
                db=db,
                name=tb_name,
                mapping={
                    "db_name": db_name,
                    "df": zlib.compress(pickle.dumps(df)),
                    "filter_datetime": json.dumps(filter_datetime) if filter_datetime is not None else "",
                    "filter_keyword": json.dumps(filter_keyword) if filter_keyword is not None else "",
                    "filter_columns": json.dumps(filter_columns) if filter_columns is not None else "",
                    "ascending": json.dumps(ascending) if ascending is not None else "",
                    "filter_row_limit": json.dumps(filter_row_limit) if filter_row_limit is not None else "",
                }
            )
            return df
        else:
            return pickle.loads(zlib.decompress(res_[b"df"]))

    @auto_retry()
    def read_one(self, db: int, name: str, key: str, columns_filter: Union[list, None] = None):
        """
        read dataframe from redis
        :param db:
        :param name:
        :param key:
        :param columns_filter:
        :return:
        """
        data = self.redis.get_hash(db=db, name=name, k=key, decode=False)
        res_df = pickle.loads(zlib.decompress(data))
        if columns_filter is None:
            return res_df
        else:
            return res_df[columns_filter].copy()

    @auto_retry()
    def read_all(
            self,
            db: int,
            name: str,
            raw_hash: bool = False,
            keyword_filter: Union[str, None] = None,
            match_index: bool = False,
            index: str = 'datetime',
            columns_filter: Union[list, None] = None
    ):
        """
        read all dataframes from redis, with a keyword_filter to get matching hash keys
        :param keyword_filter: use keyword to filter out the hash keys
        :param raw_hash: if True, return raw hashmap
        :param index: to be set as index
        :param match_index: if True, concat on given index
        :param db: int
        :param name: hashmap name
        :param columns_filter: which columns to extract
        :return:
        """
        hashmap = self.redis.get_hash(db=db, name=name, decode=False)
        if raw_hash:
            return hashmap
        if keyword_filter is None:
            pre_proc_hash = {k.decode('utf8'): pickle.loads(zlib.decompress(v)) for k, v in hashmap.items()}
        else:
            pre_proc_hash = {
                k.decode('utf8'): pickle.loads(zlib.decompress(v)) for k, v in hashmap.items() if keyword_filter in k.decode('utf8')
            }
        if match_index:
            res_df = pd.concat([v.set_index(index) for _, v in pre_proc_hash.items() if not v.empty], axis=1)
            res_df.columns = [k for k, v in pre_proc_hash.items() if not v.empty]
            empty_ls = [k for k, v in pre_proc_hash.items() if v.empty]
            if len(empty_ls) != 0:
                logger.warning(f"{', '.join(empty_ls)} are empty.")
        else:
            res_df = pd.concat([v for _, v in pre_proc_hash.items()], axis=0)

        if columns_filter is None:
            return res_df
        else:
            return res_df[columns_filter].copy()
