"""
This is a customized error library.
"""


class DatetimeError(Exception):
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg


class UnifiedDatabaseError(Exception):
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg


class DynamicCodingError(Exception):
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg


class PartitionError(UnifiedDatabaseError):
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg


class CrawlerError(Exception):
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg


class LiveTradeTimeMatchingError(Exception):
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg


class NotValidTradingTimeError(Exception):
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg