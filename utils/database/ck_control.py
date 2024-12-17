import re
from typing import Union
import clickhouse_driver as cd
import pandas as pd
from utils.tool.configer import Config
from utils.tool.logger import log
from utils.custom.exception.errors import UnifiedDatabaseError

logger = log(__file__, 'utils')


class ClickHouse:
    def __init__(
            self,
            host: Union[str, None] = None,
            port: Union[int, None] = None,
            user: Union[str, None] = None,
            pwd: Union[str, None] = None,
    ):
        conf_ = Config()
        config = conf_.get_conf
        host = config.get('Clickhouse', 'host') if host is None else host
        port = config.getint('Clickhouse', 'port') if port is None else port
        user = config.get('Clickhouse', 'user') if user is None else user
        password = config.get('Clickhouse', 'password') if pwd is None else pwd
        self.client = cd.Client(host=host, port=port, user=user, password=password)

    def execute_sql(self, sql_str: str):
        return self.client.execute(sql_str)

    def create_db(self, db: str, engine: str = 'Atomic'):
        create_database_sql = f'CREATE DATABASE IF NOT EXISTS {db} engine={engine}'
        self.client.execute(create_database_sql)

    def create_table(
            self,
            df: pd.DataFrame,
            db: str,
            tb: str,
            index: list,
            parti: Union[list, None] = None,
            misc: Union[str, None] = None
    ):
        """
        Automated table builder. Hell yeah!
        """
        if isinstance(index, list) is False or len(index) == 0:
            raise UnifiedDatabaseError("Index Declaration is a MUST-DO if you want to use Clickhouse as your database.")
        datatype_dic = self.__map_table_types(df)
        cont_ = ",".join([f"`{k}` {v}" if k not in index else f"`{k}` {v} NOT NULL" for k, v in datatype_dic.items()])
        sql_ = f"CREATE TABLE IF NOT EXISTS {db}.{tb} " \
               f"({cont_}) " \
               f"ENGINE = ReplacingMergeTree() "
        appendix_ = f"ORDER BY ({','.join(index)})  " if index is not None and len(index) != 0 else ""
        appendix_ += f"PARTITION BY ({','.join(parti)}) " if parti is not None and len(parti) != 0 else ""
        appendix_ += misc if misc is not None else ""
        sql_ += appendix_
        self.execute_sql(sql_)

    def __map_table_types(self, df: pd.DataFrame):
        """
        to map dataframe type into clickhouse table type.
        """
        type_dic = df.dtypes.to_dict()
        type_dic_for_ck = {}
        for k, v in type_dic.items():
            if 'int' in str(v).lower() or 'float' in str(v).lower():
                type_dic_for_ck[k] = 'Float64'
            elif 'date' in str(v).lower() or 'time' in str(v).lower():
                type_dic_for_ck[k] = 'Datetime64'
            else:
                type_dic_for_ck[k] = 'String'
        return type_dic_for_ck

    def __map_df_types(self, df: pd.DataFrame):
        type_dic = df.dtypes.to_dict()
        for k, v in type_dic.items():
            if 'int' in str(v).lower() or 'float' in str(v).lower():
                df[k] = df[k].astype('float')
            elif 'date' in str(v).lower() or 'time' in str(v).lower():
                df[k] = pd.to_datetime(df[k])
            else:
                df[k] = df[k].astype('str').fillna('')
        return df

    def get_type_dict(self, table):
        sql_str = f"select name, type from system.columns where table='{table}';"
        df = self.read_dataframe_by_sql_str(sql_str)
        df = df.set_index('name')
        type_dict = df.to_dict('dict')['type']
        return type_dict

    def read_dataframe_by_sql_str(self, sql_str):
        data, columns = self.client.execute(sql_str, columnar=True, with_column_types=True)
        if data != []:
            df = pd.DataFrame({re.sub(r'\W', '_', col[0]): pd.to_datetime(d) if 'date' in col[1].lower() or 'time' in col[1].lower() else d for d, col in zip(data, columns)})
            return df
        else:
            df = pd.DataFrame({}, columns=[col[0] for col in (columns)])
            return df

    def check_target(
            self,
            df: pd.DataFrame,
            db: str,
            tb: str,
            index: Union[list, None] = None,
            partition: Union[list, None] = None,
            misc: Union[str, None] = None
    ):
        """
        check if target database or table exits, build if not.
        """
        try:
            self.execute_sql(f'INSERT INTO {db}.{tb} VALUES 1')
        except Exception as error:
            if f"Database {db} doesn't exist" in error.args[0] or f"Database {db} does not exist" in error.args[0]:
                self.create_db(db)
                self.create_table(df, db, tb, index=index, parti=partition, misc=misc)
            elif f"Table {db}.{tb} doesn't exist" in error.args[0] or f"Table {db}.{tb} does not exist" in error.args[0]:
                self.create_table(df, db, tb, index=index, parti=partition, misc=misc)
            elif "Cannot parse input" in error.args[0]:
                pass
            else:
                logger.error(error.args[0])
        else:
            pass

    def to_sql(self, df, db, table):
        cols = ','.join(df.columns.tolist())
        df = self.__map_df_types(df)
        data = df.to_dict('records')
        sql_ = f"INSERT INTO {db}.{table} ({cols}) VALUES"
        self.client.execute(sql_, data, types_check=True)

    def get_col_names(self, db: str):
        sql_str = f"SELECT name FROM system.tables WHERE database = '{db}';"
        col_ls = self.client.execute(sql_str)
        col_ls = [i[0] for i in col_ls] if len(col_ls) != 0 else []
        return col_ls

    def get_db_names(self):
        sql_str = "SHOW DATABASES"
        dbs = self.client.execute(sql_str)
        return dbs

    def insert_dataframe(
            self,
            df: pd.DataFrame,
            db: str,
            tb: str,
            index: Union[list, None] = None,
            partition: Union[list, None] = None,
            misc: Union[str, None] = None,
            drop_duplicates: bool = False
    ):
        self.check_target(df, db, tb, index, partition, misc)
        if partition is not None and len(partition) != 0:
            for _, v in df.groupby(partition):
                self.to_sql(v, db, tb)
        else:
            self.to_sql(df, db, tb)
        logger.info(f"Data inserted into {db}.{tb}: Dataframe{df.shape}")
        if drop_duplicates:
            self.drop_duplicate_data(db, tb)

    def get_ddl(self, db: str, tb: str):
        sql_ = f"show create table {db}.{tb}"
        res = self.client.execute(sql_)
        res = ''.join(res[0][0])
        return res

    def get_index(self, db: str, tb: str):
        res = self.get_ddl(db, tb)
        idx_ = [i for i in res.split("\n") if "ORDER BY" in i][-1].lstrip("ORDER BY").strip().strip("(").strip(")")
        idx_ = idx_.split(', ')
        return idx_

    def del_row(self, db: str, tb: str, filter_sql: str):
        sql_ = f"alter table {db}.{tb} delete {filter_sql}"
        self.execute_sql(sql_)

    def del_column(self, db: str, tb: str, columns: list):
        for column_str in columns:
            sql = f"alter table {db}.{tb} drop column {column_str}"
            self.execute_sql(sql)

    def drop_db(self, db: str):
        drop_sql = f"DROP DATABASE IF EXISTS {db}"
        self.client.execute(drop_sql)

    def drop_table(self, db: str, table: str):
        drop_sql = f"DROP TABLE IF EXISTS {db}.{table}"
        self.client.execute(drop_sql)

    def drop_duplicate_data(self, db: str, table: str):
        drop_duplicate_data_sql = f" OPTIMIZE TABLE {db}.{table} final"
        self.client.execute(drop_duplicate_data_sql)

    def get_table_size(self, db: str, tb: str):
        sql_ = f"SELECT sum(data_uncompressed_bytes) from system.parts where " \
               f"(database in ('{db}')) and (table in ('{tb}'))"
        res = self.execute_sql(sql_)
        return round(res[0][0] / (1024 * 1024), 2)
