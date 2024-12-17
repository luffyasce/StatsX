import time
import pathos


class MultiTask:
    def __init__(self):
        self.core_num = pathos.multiprocessing.cpu_count()
        self.pool = pathos.multiprocessing.ProcessPool(ncpus=self.core_num)

    def general_multi_task(self, func, *args):
        """api for general multitasking"""
        res_ = self.pool.amap(func, *args)
        while not res_.ready():
            time.sleep(0.1)
        res_ls = res_.get()
        return res_ls

