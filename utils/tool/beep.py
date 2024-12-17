import os
import winsound


class Beep:
    def __init__(self):
        pass

    @classmethod
    def warning(cls):
        freq_ = 2800
        duration = 2000
        winsound.Beep(freq_, duration)

    @classmethod
    def emergency(cls):
        winsound.Beep(1500, 400)
        winsound.Beep(500, 400)
        winsound.Beep(1500, 400)
        winsound.Beep(500, 400)
        winsound.Beep(1500, 400)
        winsound.Beep(500, 400)

    @classmethod
    def start(cls):
        for i in range(1, 10):
            winsound.Beep(i * 100, 100)

