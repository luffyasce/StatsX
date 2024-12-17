import netifaces as ni


def get_local_ip():
    for interface in ni.interfaces():
        addrs = ni.ifaddresses(interface)
        if ni.AF_INET in addrs:
            ip = addrs[ni.AF_INET][0]['addr']
            if ip != '127.0.0.1':  # 排除本地回环地址
                return ip
    return None
