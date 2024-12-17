"""
Define a base class for selenium
"""
import platform

import pandas as pd
import requests
import random
import socket
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from utils.tool.configer import Config


class Crawler:
    def __init__(self):
        configer = Config()
        self.conf = configer.get_conf

        self.last_reqest_full_params = {}

        self._last_ua_ = ''

    def __last_ua__(self):
        return self._last_ua_

    def init_session(self):
        session = requests.session()
        ua = self.__ua_generator()
        session.headers.update({'User-agent': ua})
        return session

    def selenium_get_browser(self, driver_path: str = None):
        if driver_path is None:
            driver_path = self.conf.get('CrawlerSelenium', 'webdriver')
        chrome_options = Options()
        if platform.system().lower() == 'linux':
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument("--headless")
        browser = webdriver.Chrome(
            executable_path=driver_path,
            options=chrome_options
        )
        return browser

    def __ua_generator(self):
        """
        :return: 返回一个随机抽取的user agent
        """
        uas = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:97.0) Gecko/20100101 Firefox/97.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.64",
            "Mozilla/5.0 (Linux; Android 11; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.105 Mobile Safari/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (Android 10; Mobile; rv:86.0) Gecko/86.0 Firefox/86.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36 OPR/62.0.3331.116",
            "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko",
            "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
        ]
        ua = random.choice(uas)
        self._last_ua_ = ua
        return ua

    def get_cookies(self, url: str, method: str = "GET", **kwargs):
        ua = self.__ua_generator()
        sess = requests.session()
        cookie_jar = sess.request(method=method, url=url, headers={'User-agent': ua}, **kwargs).cookies
        cookie_t = requests.utils.dict_from_cookiejar(cookie_jar)
        return cookie_t

    def __record_last_request__(self, req):
        self.last_reqest_full_params['url'] = req.request.url
        self.last_reqest_full_params['headers'] = req.request.headers
        self.last_reqest_full_params['body'] = req.request.body

    def request_with_raw_resp(self, url: str, method: str = "GET", header: dict = None, encoding='utf8', timeout: float = None, **kwargs):
        ua = self.__ua_generator()
        header = {**{'User-agent': ua}, **header} if header is not None else {'User-agent': ua}
        resp_ = requests.request(method, url, headers=header, timeout=timeout, **kwargs)
        return resp_

    def request_url(self, url: str, method: str = "GET", header: dict = None, encoding='utf8', timeout: float = None, **kwargs):
        ua = self.__ua_generator()
        header = {**{'User-agent': ua}, **header} if header is not None else {'User-agent': ua}
        resp_ = requests.request(method, url, headers=header, timeout=timeout, **kwargs)
        self.__record_last_request__(resp_)
        resp_.encoding = encoding
        resp_ = resp_.text
        return resp_

    def request_full_session(self, url: str, method: str = "GET", header: dict = None, encoding: str = 'utf8', timeout: float = None, **kwargs):
        ua = self.__ua_generator()
        header = {**{'User-agent': ua}, **header} if header is not None else {'User-agent': ua}
        resp_ = requests.request(method, url, headers=header, timeout=timeout, **kwargs)
        resp_.encoding = encoding
        cont = resp_.text
        req_head = resp_.request.headers
        resp_head = resp_.headers
        resp_code = resp_.status_code
        resp_cookie = resp_.cookies
        return {
            "resp_content": cont,
            "req_header": req_head,
            "resp_header": resp_head,
            "resp_code": resp_code,
            "resp_cookie": resp_cookie
        }

    @staticmethod
    def generate_socket():
        # Todo Not Tested Yet!
        return socket.socket()

    @staticmethod
    def socket_api(sock, url: str, port: int = None):
        # Todo Not Tested Yet!
        u_ = (url, port) if port is not None else (url, 443)
        sock.connect(u_)
        yield sock.recv(1024)

    @staticmethod
    def read_online_excel_with_auto_engine_switch(excel_data, **kwargs):
        try:
            df = pd.read_excel(excel_data, engine=None, **kwargs)
        except:
            try:
                df = pd.read_excel(excel_data, engine='xlrd', **kwargs)
            except:
                try:
                    df = pd.read_excel(excel_data, engine='openpyxl', **kwargs)
                except Exception as e:
                    try:
                        df = pd.read_csv(excel_data, **kwargs)
                    except Exception as e:
                        raise Exception(f"Failed to read the file with all engines: {e}")
        return df
