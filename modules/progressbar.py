# config for bill.py
# -*- coding: utf-8 -*-


import sys

"""
Progressbar:  прогресс-бар в консоли
bar = Progressbar('process', maximum=1000)
bar.update_progress(10)
...
bar.update_progress(256)
...
bar.update_progress(1000)
"""


class Progressbar(object):
    def __init__(self, info, maximum):
        self.info = info
        self.maximum = maximum

    def update_progress(self, progress):
        x = (progress*100)/self.maximum
        sys.stdout.write('\r{info}: [{prog}] {proc}%'.format(prog='#'*int((x/10)), proc=int(x), info=self.info))
        sys.stdout.flush()

    @staticmethod
    def go_new_line():
        sys.stdout.write('\n')
        sys.stdout.flush()


if __name__ == '__main__':

    import time

    bar = Progressbar('process', 1000)
    for i in (100, 150, 300, 500, 600, 800, 1000):
        bar.update_progress(i)
        time.sleep(1)
    Progressbar.go_new_line()

