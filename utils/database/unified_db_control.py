"""
All database connection unified control interface
"""
import time
import pandas as pd
from typing import Any, Union
from utils.database import mongo_control
from utils.database import ck_control
from utils.tool.datetime_wrangle import map_datetime
from utils.tool.configer import Config
from utils.tool.logger import log
from utils.tool.decorator import auto_retry, singleton
from utils.custom.exception.errors import UnifiedDatabaseError

logger = log(__file__, "utils")


@singleton
class UnifiedControl:
    def __init__(
            self,
            db_type: str,
            host: Union[str, None] = None,
            port: Union[int, None] = None,
            user: Union[str, None] = None,
            pwd: Union[str, None] = None,
    ):
        """
        To initiate unified database control, you must identify which database to use in the config file.
        ie. mongo: mongodb
            clickhouse: clickhouse
            postgres: postgres db
            hdf5: hdf5
        """
        _conf = Config()
        _config = _conf.get_conf
        self.__private_config = _conf.get_private_conf
        self.replace_db_prefix = _config.get("Databases", "app_db_prefix")
        self.replace_tb_postfix = _config.get("Databases", "app_tb_postfix")
        self.current_db_level = list(_config.get("Databases", "current_db_level").split('-'))
        self.current_data_source = list(_config.get("Databases", "current_data_source").split('-'))
        self.db_type = db_type
        self.database = _config.get("Databases", db_type)
        if self.database == 'mongo':
            self.client = mongo_control.Mongo(host=host, port=port, user=user, pwd=pwd)
        elif self.database == 'clickhouse':
            self.client = ck_control.ClickHouse(host=host, port=port, user=user, pwd=pwd)
        else:
            raise UnifiedDatabaseError(
                "Sorry but currently we do not support this type of database. "
                "Please modify your database type and retry."
            )
        self.__origin_client = self.client
        self.__origin_database = self.database

        self.__priv_cli = None
        self.__priv_db = None

    def check_db_name(self, db_name: str):
        prefix = db_name.split('_')[0]
        if self.db_type == 'app':
            if prefix not in self.current_db_level and prefix != self.replace_db_prefix:
                db_name = f"{self.replace_db_prefix}_{db_name}"
            else:
                db_name = f"{self.replace_db_prefix}_{'_'.join(db_name.split('_')[1:])}"
        return db_name

    def check_tb_name(self, tb_name: str):
        postfix = tb_name.split('_')[-1]
        if self.db_type == 'app':
            if postfix not in self.current_data_source and postfix != self.replace_tb_postfix:
                tb_name = f"{tb_name}_{self.replace_tb_postfix}"
            else:
                tb_name = f"{'_'.join(tb_name.split('_')[:-1])}_{self.replace_tb_postfix}"
        return tb_name

    @auto_retry()
    def _switch_client(self):
        if self.__priv_cli is None:
            database = self.__private_config.get('PrivateDB', 'name', fallback=None)
            p_user = self.__private_config.get('PrivateDB', 'user', fallback=None)
            p_pwd = self.__private_config.get('PrivateDB', 'password', fallback=None)
            p_host = self.__private_config.get('PrivateDB', 'host', fallback=None)
            p_port = self.__private_config.getint('PrivateDB', 'port', fallback=None)
            if p_host is not None:
                if database == 'mongo':
                    self.client = mongo_control.Mongo(host=p_host, port=p_port, user=p_user, pwd=p_pwd)
                    self.database = database
                elif database == 'clickhouse':
                    self.client = ck_control.ClickHouse(host=p_host, port=p_port, user=p_user, pwd=p_pwd)
                    self.database = database
                else:
                    raise UnifiedDatabaseError(
                        "Sorry but currently we do not support this type of database. "
                        "Please modify your database type and retry."
                    )
                logger.info(f"Private database control activated client: {self.client} handle for {database}")
            else:
                logger.warning(
                    "Private database not designated. Data will be written into public database."
                )
            self.__priv_cli = self.client
            self.__priv_db = self.database
        else:
            self.client = self.__priv_cli
            self.database = self.__priv_db

    @auto_retry()
    def _restore_client(self):
        self.client = self.__origin_client
        self.database = self.__origin_database
        logger.info(f"Database control restored client: {self.client} handle for {self.database}")

    @auto_retry()
    def get_db_names(self):
        """
        Unified method to get all databases in one given database type
        """
        if self.database == 'mongo':
            result = self.client.get_db_names()
        elif self.database == 'clickhouse':
            result = [i[0] for i in self.client.get_db_names()] if len(self.client.get_db_names()) != 0 else []
        else:
            raise UnifiedDatabaseError(
                "I don't know how you got here, but currently we do not support this type of database. "
                "Please modify your database type and retry."
            )
        return result

    @auto_retry()
    def get_table_names(self, db_name: str) -> list:
        """
        unified method to get all table(collection) names in one given database
        :return: list of all table names
        """
        db_name = self.check_db_name(db_name)
        if self.database == 'mongo':
            result = self.client.get_col_names(db_name)
        elif self.database == 'clickhouse':
            result = self.client.get_col_names(db_name)
        else:
            raise UnifiedDatabaseError(
                "I don't know how you got here, but currently we do not support this type of database. "
                "Please modify your database type and retry."
            )
        return result

    @auto_retry()
    def insert_dataframe(
            self,
            df: pd.DataFrame,
            db_name: str,
            tb_name: str,
            set_index: Union[list, None] = None,
            partition: Union[list, None] = None,
            **kwargs
    ):
        """
        unified method to insert a dataframe to a given table in a given database
        (with a list of given column names that you want to set as indices)
        :param df:  dataframe
        :param db_name:  target database name
        :param tb_name:  target table name
        :param set_index:  a list of column names that you want to set as indices / None if indices are not needed.
        """
        db_name = self.check_db_name(db_name)
        tb_name = self.check_tb_name(tb_name)
        if isinstance(df, pd.DataFrame) is False or df.empty:
            return
        if self.database == 'mongo':
            _idx = [(i, -1) for i in set_index] if set_index is not None else None
            self.client.insert_dataframe(
                df=df,
                db=db_name,
                collection=tb_name,
                idx=_idx,
            )
        elif self.database == 'clickhouse':
            self.client.insert_dataframe(
                df=df,
                db=db_name,
                tb=tb_name,
                index=set_index,
                partition=partition,
                **kwargs
            )
        else:
            raise UnifiedDatabaseError(
                "I don't know how you got here, but currently we do not support this type of database. "
                "Please modify your database type and retry."
            )

    @staticmethod
    def _operand_map_for_sql(input_: Any) -> str:
        op_dict = {
            "gte": ">=",
            "lte": "<=",
            "gt": ">",
            "lt": "<",
            "eq": "=",
            "ne": "!=",
        }
        res = op_dict.get(input_)
        return res

    @auto_retry()
    def read_dataframe(
            self,
            db_name: Union[str, None] = None,
            tb_name: Union[str, None] = None,
            filter_datetime: Union[dict, None] = None,
            filter_keyword: Union[dict, None] = None,
            filter_columns: Union[list, None] = None,
            ascending: Union[list, None] = None,
            filter_row_limit: Union[int, None] = None,
            **kwargs
    ) -> pd.DataFrame:
        """
        unified method to read dataframe from given table in given database
        :param db_name: database name
        :param tb_name: table name
        :param filter_datetime: {'col7': {'lt': "2020-01-01 05:00:00", 'gt': "2021-01-01 09:00:00"}}
                                            # col7 in between two dates
        :param filter_keyword: to filter in all the data records that matches (equal to) the keyword,
                                eg:
                                {
                                    'col1': {'gte': 20},        # col1 data greater than or equal to 20
                                    'col2': {'lte': 120},       # col2 data less than or equal to 120
                                    'col3': {'ne': 'abc'},      # col3 not equal to 'abc'
                                    'col4': {'lt': 10000},      # col4 less than 10000
                                    'col5': {'gt': 6566}        # col5 greater than 6566
                                    'col6': {'eq': 100}         # col6 equal to 100
                                }
        :param filter_columns: to read desired columns only.
                                eg:
                                ['col1', 'col2', 'col3']        # to read dataframe that only contains these columns.
        :param filter_row_limit: limit the rows of the dataframe.
                                eg:
                                1                               # the returning dataframe will contain only 1 row
        :param ascending: to sort dataframe based on given columns
                                eg:
                                [
                                    ('col1': True),
                                    ('col2': False)
                                ]
                                the returning dataframe will be sorted based on ascending col1 and descending col2
        **kwargs: in case that certain database client contains certain special functions,
                    you could call to these functions via other keyword arguments.
        :return: dataframe
        """
        if db_name is not None:
            db_name = self.check_db_name(db_name)
        if tb_name is not None:
            tb_name = self.check_tb_name(tb_name)
        if self.database == 'mongo':
            res_df = self.__read_dataframe_by_mongo(
                db_name,
                tb_name,
                filter_datetime,
                filter_keyword,
                filter_columns,
                ascending,
                filter_row_limit,
                ** kwargs
            )
        elif self.database == 'clickhouse':
            res_df = self.__read_dataframe_by_clickhouse(
                db_name,
                tb_name,
                filter_datetime,
                filter_keyword,
                filter_columns,
                ascending,
                filter_row_limit,
                **kwargs
            )
        else:
            raise UnifiedDatabaseError(
                "I don't know how you got here, but currently we do not support this type of database. "
                "Please modify your database type and retry."
            )
        return res_df

    def make_filters_for_mongo(
            self,
            filter_datetime: Union[dict, None] = None,
            filter_keyword: Union[dict, None] = None,
    ):
        if filter_datetime is not None:
            dt_filt_ = {}
            for k, v in filter_datetime.items():
                dt_filt_[k] = {
                    f"${_ki}": map_datetime(vi) if not isinstance(vi, list) else[
                        map_datetime(i) for i in vi
                    ] for _ki, vi in v.items()
                }
        else:
            dt_filt_ = {}
        if filter_keyword is not None:
            kw_filt_ = {}
            for k, v in filter_keyword.items():
                kw_filt_[k] = {
                    f"${_ki}": vi for _ki, vi in v.items()
                }
        else:
            kw_filt_ = {}
        filt_ = {**dt_filt_, **kw_filt_}
        return filt_

    def make_filters_for_ck(
            self,
            filter_datetime: Union[dict, None] = None,
            filter_keyword: Union[dict, None] = None,
    ):
        sql_filt_ = f"where "
        sql_fw = ""
        if filter_keyword is not None:
            for k, v in filter_keyword.items():
                for ki, vi in v.items():
                    if ki == 'in' and isinstance(vi, list):
                        sql_fw += "("
                        for i in vi:
                            sql_fw += f"`{k}` == '{i}' or "
                        sql_fw = sql_fw.rstrip("or ")
                        sql_fw += ")"
                    else:
                        vi = vi if isinstance(vi, int) else f"'{vi}'"
                        sql_fw += f"`{k}` {self._operand_map_for_sql(ki)} {vi}"
                    sql_fw += ' and '
        if filter_datetime is not None:
            for k, v in filter_datetime.items():
                for ki, vi in v.items():
                    if ki == 'in' and isinstance(vi, list):
                        sql_fw += "("
                        for i in vi:
                            sql_fw += f"`{k}` == '{map_datetime(i)}' or "
                        sql_fw = sql_fw.rstrip("or ")
                        sql_fw += ")"
                    else:
                        sql_fw += f"`{k}` {self._operand_map_for_sql(ki)} '{map_datetime(vi)}'"
                    sql_fw += ' and '
        if sql_fw != "":
            sql_fw = sql_fw.rstrip(" and ")
            sql_fw = sql_filt_ + sql_fw
        return sql_fw

    def __read_dataframe_by_mongo(
            self,
            db_name: str,
            tb_name: str,
            filter_datetime: Union[dict, None] = None,
            filter_keyword: Union[dict, None] = None,
            filter_columns: Union[list, None] = None,
            ascending: Union[list, None] = None,
            filter_row_limit: Union[int, None] = None,
            **kwargs
    ):
        filt_ = self.make_filters_for_mongo(
            filter_datetime=filter_datetime,
            filter_keyword=filter_keyword
        )
        ascending = [(i[0], 1) if i[1] else (i[0], -1) for i in ascending] if ascending is not None else None
        res_df = self.client.read_dataframe(
            db=db_name,
            collection=tb_name,
            filter=filt_,
            ascending=ascending,
            columns=filter_columns,
            limit=filter_row_limit
        )
        return res_df

    def __read_dataframe_by_clickhouse(
            self,
            db_name: str,
            tb_name: str,
            filter_datetime: Union[dict, None] = None,
            filter_keyword: Union[dict, None] = None,
            filter_columns: Union[list, None] = None,
            ascending: Union[list, None] = None,
            filter_row_limit: Union[int, None] = None,
            **kwargs
    ):
        """
        with this method, only non-repetitive data will be read and returned.
        """
        if kwargs:
            res_df = self.client.read_dataframe_by_sql_str(**kwargs)
            return res_df
        else:
            if filter_columns is not None:
                filt_col_ = [f"`{i}`" for i in filter_columns]
                select_ = f"select distinct {', '.join(filt_col_)}"
            else:
                select_ = "select distinct *"
            sql_fw = self.make_filters_for_ck(
                filter_datetime=filter_datetime,
                filter_keyword=filter_keyword
            )
            if ascending is not None:
                sql_asc_ = "order by "
                asc_ = ascending[0]
                sql_asc_ += f"`{asc_[0]}` {'ASC' if asc_[1] else 'DESC'}"
            else:
                sql_asc_ = ""
            if filter_row_limit is not None:
                sql_limit_ = f"limit {filter_row_limit}"
            else:
                sql_limit_ = ""
            query_sql = f"{select_} from {db_name}.{tb_name} {sql_fw} {sql_asc_} {sql_limit_}"
            try:
                res_df = self.client.read_dataframe_by_sql_str(query_sql)
            except Exception as error:
                if "doesn't exist" in error.args[0]:
                    return pd.DataFrame()
                else:
                    raise Exception(error.args[0])
            else:
                return res_df

    @auto_retry()
    def get_table_index(self, db_name: str, tb_name: str):
        db_name = self.check_db_name(db_name)
        tb_name = self.check_tb_name(tb_name)
        if self.database == 'mongo':
            res_ = self.client.get_tb_index(db_name, tb_name)
        elif self.database == 'clickhouse':
            res_ = self.client.get_index(db_name, tb_name)
        else:
            raise UnifiedDatabaseError(
                "I don't know how you got here, but currently we do not support this type of database. "
                "Please modify your database type and retry."
            )
        return res_

    @auto_retry()
    def del_row(
            self,
            db_name: str,
            tb_name: str,
            filter_datetime: Union[dict, None] = None,
            filter_keyword: Union[dict, None] = None,
    ):
        db_name = self.check_db_name(db_name)
        tb_name = self.check_tb_name(tb_name)
        if self.database == 'mongo':
            fil_ = self.make_filters_for_mongo(filter_datetime, filter_keyword)
            self.client.del_rows(db_name, tb_name, fil_)
        elif self.database == 'clickhouse':
            fil_ = self.make_filters_for_ck(filter_datetime, filter_keyword)
            self.client.del_row(db_name, tb_name, fil_)

    @auto_retry()
    def del_columns(
            self,
            db_name: str,
            tb_name: str,
            columns: list
    ):
        db_name = self.check_db_name(db_name)
        tb_name = self.check_tb_name(tb_name)
        if self.database == 'mongo':
            self.client.del_columns(db_name, tb_name, columns)
        elif self.database == 'clickhouse':
            self.client.del_column(db_name, tb_name, columns)

    @auto_retry()
    def drop_duplicate_data(self, db_name: str, tb_name: str):
        db_name = self.check_db_name(db_name)
        tb_name = self.check_tb_name(tb_name)
        if self.database == 'mongo':
            pass
        elif self.database == 'clickhouse':
            self.client.drop_duplicate_data(db_name, tb_name)

    @auto_retry()
    def finishing(self):
        """To Clear up duplicates"""
        for db in [i for i in self.get_db_names() if 'data' in i]:
            for tb in self.get_table_names(db):
                while True:
                    try:
                        self.drop_duplicate_data(db, tb)
                    except Exception:
                        logger.error(f"Failed optimization: {db}.{tb}")
                        time.sleep(300)
                        continue
                    else:
                        break
        logger.info("All databases optimized.")

    @auto_retry()
    def get_table_size(self, db_name: str, tb_name: str):
        db_name = self.check_db_name(db_name)
        tb_name = self.check_tb_name(tb_name)
        if self.database == 'mongo':
            return self.client.get_table_size(db_name, tb_name)
        elif self.database == 'clickhouse':
            return self.client.get_table_size(db_name, tb_name)
