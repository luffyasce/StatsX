import hashlib
from datetime import datetime


class Hash:

    @classmethod
    def md5_stamp(cls, *args):
        stmp_ = hashlib.md5(
            f"{[*args]}-{datetime.now()}".encode('utf8')
        ).hexdigest()
        return stmp_

    @classmethod
    def md5_encode(cls, *args):
        ecd = hashlib.md5(
            f"{[*args]}".encode('utf8')
        ).hexdigest()
        return ecd

    @classmethod
    def all_params_encode(cls, *args, **kwargs):
        str_args = "-".join(args) + "-".join([f"{k}:{v}" for k, v in kwargs.items()])
        ecd = hashlib.md5(str_args.encode('utf8')).hexdigest()
        return ecd
