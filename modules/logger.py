#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime


def _current_date():
    """
     Возвращает текущую дату в формате: YYYY-MM-DAY HH:MM:SS
    :return: например, '2022-06-19 10:11:56'
    """
    now = datetime.now()
    iso = now.isoformat()   # 2022-06-19T16:15:11.420121
    [date, time_iso] = iso.split('T')
    hms = time_iso.split('.')[0]
    return '{date} {time}'.format(date=date, time=hms)   # 2022-06-19 16:15:11


class Logger(object):
    """
    Логирование в файл
    Дополнительно вывод на экран если is_print=True
    log = Logger('/tmp/myfile.txt').log
    log('all ok')   - вывод в фал и на экран
    log('prosto tak', is_print=False) - вывод только в файл
    """
    def __init__(self, filename):
        self.filename = filename

    def log(self, text, is_print=True, is_newline=True):
        msg = _current_date()
        if type(text) is list or type(text) is tuple:
            msg += ' ' + ", ".join(str(elem) for elem in text)
        else:
            msg += ' ' + text

        with open(self.filename, 'a') as fp:
            fp.write(msg)
            if is_newline:
                fp.write('\n')

        if is_print:
            print(msg)
