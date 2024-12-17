import pandas as pd
import redis
import pickle
import zlib
from typing import Any, Union
from utils.tool.configer import Config
from utils.tool.logger import log
from utils.tool.decorator import auto_retry

conf_ = Config()
config = conf_.get_conf


class Redis:
    def __init__(self):
        self.host = config.get('Redis', 'host')
        self.port = config.getint('Redis', 'port')
        self.pwd = config.get('Redis', 'password')

    @staticmethod
    def encode_dataframe(df: pd.DataFrame):
        return zlib.compress(pickle.dumps(df))

    @staticmethod
    def decode_dataframe(data):
        return pickle.loads(zlib.decompress(data))

    def create_handle(self, db: int, decode: bool = True):
        return redis.StrictRedis(
            host=self.host, port=self.port, password=self.pwd, db=db, decode_responses=decode
        )

    @auto_retry()
    def set_key(self, db: int, k: Any, v: Any, **kwargs):
        rds = self.create_handle(db)
        rds.set(k, v, **kwargs)

    @auto_retry()
    def get_key(self, db: int, k: Any, decode: bool):
        rds = self.create_handle(db, decode=decode)
        return rds.get(k)

    @auto_retry()
    def set_hash(self, db: int, name: Any, k: Any = None, v: Any = None, mapping: Any = None):
        rds = self.create_handle(db)
        if k is None:
            rds.hset(name, mapping=mapping)
        else:
            rds.hset(name, k, v, mapping)

    @auto_retry()
    def get_hash(self, db: int, name: Any, decode: bool, k: Any = None):
        rds = self.create_handle(db, decode=decode)
        if k is None:
            return rds.hgetall(name)
        else:
            return rds.hget(name, k)

    @auto_retry()
    def key_exist(self, db: int, k: Any):
        rds = self.create_handle(db)
        return rds.exists(k)

    @auto_retry()
    def del_key(self, db: int, keys: list):
        rds = self.create_handle(db)
        rds.delete(*keys)

    @auto_retry()
    def flush(self, db: Union[int, None] = None):
        if db is None:
            rds = self.create_handle(0)
            rds.flushall()
        else:
            rds = self.create_handle(db)
            rds.flushdb()


logger_msg = log(__file__, "utils")


class RedisMsg(Redis):
    """
    A msg queue on redis.
    message queue by default will not decode any responses.
    """
    def __init__(self, db: int, channel: str = 'default', decode: bool = False):
        super().__init__()
        self.channel = channel
        self.handle = self.create_handle(db, decode)
        self.subscriber = self.handle.pubsub()

        self.sub_stat = False

    @auto_retry()
    def push_msg(self, msg: bytes):
        self.handle.lpush(self.channel, msg)

    @auto_retry()
    def get_msg(self, timeout: int = 0):
        msg = self.handle.brpop(self.channel, timeout=timeout)
        return msg[1] if msg is not None else None

    @auto_retry()
    def pub(self, msg: Any):
        sub_num = self.handle.publish(channel=self.channel, message=msg)
        logger_msg.info(f"{self.channel} publishing: {sub_num} subscribers.")
        return sub_num

    def start_sub(self):
        if not self.sub_stat:
            self.subscriber.subscribe(self.channel)
            self.sub_stat = True

    def __sub(self, subscriber):
        for news in subscriber.listen():
            yield news

    @auto_retry()
    def sub(self):
        self.start_sub()
        try:
            for item in self.__sub(self.subscriber):
                yield item
        except KeyboardInterrupt or SystemExit:
            logger_msg.warning(f"End sub: {self.channel}")
        except Exception as err:
            logger_msg.error(f"Err on sub: {self.channel} | {err=}")
        finally:
            self.sub_stat = False   # 只要是退出循环，下次再call必须重新订阅一次
            self.subscriber.unsubscribe(self.channel)

