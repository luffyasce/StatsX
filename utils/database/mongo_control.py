from typing import Union
import pandas as pd
import pymongo
from pymongo.errors import DuplicateKeyError, BulkWriteError
from pymongo import ASCENDING, DESCENDING
from datetime import datetime
from utils.tool.configer import Config
from utils.tool.logger import log

logger = log(__file__, 'utils')


class Mongo:
    def __init__(
            self,
            host: Union[str, None] = None,
            port: Union[int, None] = None,
            user: Union[str, None] = None,
            pwd: Union[str, None] = None,
    ):
        conf_ = Config()
        config = conf_.get_conf
        host = config.get('Mongo', 'host') if host is None else host
        port = config.getint('Mongo', 'port') if port is None else port
        user = config.get('Mongo', 'user') if user is None else user
        pwd = config.get('Mongo', 'password') if pwd is None else pwd
        self.client = pymongo.MongoClient(
            host=host, port=port, username=user, password=pwd, maxIdleTimeMS=10000
        )

    def get_db_names(self):
        db_ls = self.client.list_database_names()
        return db_ls

    def get_col_names(self, db: str):
        _db = self.client[db]
        col_ls = _db.list_collection_names(filter={"name": {"$regex": r"^(?!system\.)"}})
        return col_ls

    def get_tb_index(self, db: str, tb: str):
        idx_ = self.client[db][tb].index_information()
        if len(idx_.items()) != 0:
            idx_.pop('_id_')
            idx_ = list(idx_.values())[0]['key'] if idx_ != {} else []
            idx_ = [i[0] for i in idx_]
            return idx_
        else:
            return []

    def insert_dataframe(
            self,
            df: pd.DataFrame,
            db: str,
            collection: str,
            idx: Union[None, list] = None,
            unique_idx: bool = True
    ):
        col = self.client[db][collection]
        if idx is not None:
            fields = [i[0] for i in idx]
            asc = [ASCENDING if i[1] == -1 else DESCENDING for i in idx]
            idx_ = list(zip(fields, asc))
            col.create_index(idx_, unique=unique_idx)
        try:
            result = col.insert_many(
                df.to_dict(orient='records')
            ).inserted_ids
        except BulkWriteError:
            indices = [i[0] for i in idx]
            self.update_dataframe(df, db, collection, indices)
        else:
            logger.info(f"Data inserted into {db}.{collection}: {len(result) if isinstance(result, list) else 0}")

    def update_dataframe(self, df: pd.DataFrame, db: str, collection: str, indices: list):
        col = self.client[db][collection]
        modified, upsert_ = 0, 0
        for i in df.to_dict(orient='records'):
            try:
                filt_ = {x: i[x] for x in indices}
                result_ = col.update_one(filt_, {'$set': i}, upsert=True)
            except DuplicateKeyError as error:
                raise error
            else:
                modified += result_.modified_count
                upsert_ += 1 if result_.upserted_id is not None else 0
        logger.info(f"Data updated (set) into {db}.{collection}: {modified} modified, {upsert_} upserted")

    def del_columns(
            self,
            db: str,
            collection: str,
            columns: list,
            filt_: Union[dict, None] = None
    ):
        filt_ = filt_ if filt_ is not None else {}
        col = self.client[db][collection]
        up_ = {"$unset": {i: "" for i in columns}}
        col.update_many(filter=filt_, update=up_)

    def del_rows(
            self,
            db: str,
            collection: str,
            filter: Union[dict, None] = None
    ):
        col = self.client[db][collection]
        col.delete_many(filter)

    def read_dataframe(
            self,
            db: str,
            collection: str,
            filter: Union[dict, None] = None,
            ascending: Union[list, None] = None,
            columns: Union[list, None] = None,
            limit: Union[int, None] = None,
    ):
        col = self.client[db][collection]
        limit = 0 if limit is None else limit
        loop = col.find(filter=filter, projection=columns, limit=limit, sort=ascending)
        result_ls = [i for i in loop]
        result = pd.DataFrame(result_ls)
        if '_id' in result.columns:
            result = result.drop(columns=['_id'])
        return result

    def get_table_size(self, db: str, tb: str):
        db = self.client[db]
        res = db.command('collstats', tb)['size']
        return round(res / (1024 * 1024), 2)


if __name__ == "__main__":
    localMongo = Mongo()
    remoteMongo = Mongo(host='192.168.1.23', port=27017)
    for db in remoteMongo.get_db_names():
        if db in ['local', 'admin', 'config']:
            continue
        for tb in remoteMongo.get_col_names(db):
            idx = remoteMongo.get_tb_index(db, tb)
            df = remoteMongo.read_dataframe(db, tb)
            new_idx = [(i, -1) for i in idx] if len(idx) != 0 else None
            localMongo.insert_dataframe(df, db, tb, new_idx)
