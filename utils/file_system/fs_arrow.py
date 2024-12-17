import os
import pandas as pd
import numpy as np
import pyarrow as arrow
from pyarrow import parquet as pq
from utils.tool.configer import Config


# TODO!

class FSArrow:
    def __int__(self):
        conf_ = Config()
        self.fs_path = os.path.join(conf_.path, os.sep.join(["docs", "fs_storage"]))
        print(self.fs_path)

    def save_dataframe(
            self, df: pd.DataFrame, folder: str, filename: str
    ):
        pass

if __name__ == "__main__":
    fs = FSArrow()