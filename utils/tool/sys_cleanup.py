import os
import subprocess
from utils.tool.configer import Config


class SysClean:
    def __init__(self):
        conf = Config()
        self.pth = conf.path

    def clean_log(self):
        log_pth = os.path.join(self.pth, 'logs')
        subprocess.run("pm2 flush", shell=True, encoding='utf8')
        for f in os.listdir(log_pth):
            fp = os.path.join(log_pth, f)
            subprocess.run(f"> {fp}", shell=True, encoding='utf8')

    @staticmethod
    def free_mem():
        subprocess.run("sync", shell=True, encoding='utf8')
        subprocess.run("echo 1 > /proc/sys/vm/drop_caches", shell=True, encoding='utf8')
        subprocess.run("echo 2 > /proc/sys/vm/drop_caches", shell=True, encoding='utf8')
        subprocess.run("echo 3 > /proc/sys/vm/drop_caches", shell=True, encoding='utf8')
