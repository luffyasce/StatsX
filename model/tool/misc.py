from typing import Any, Callable
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


def convert_to_str(val: Any):
    if isinstance(val, Callable):
        return val.__name__
    elif isinstance(val, (list, tuple)):
        return tuple(convert_to_str(i) for i in val)
    else:
        return val
