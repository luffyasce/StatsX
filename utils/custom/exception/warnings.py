
class SafeExitWarning(Exception):
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg


class AbnormalExitWarning(Exception):
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg
