#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Calendar - производственный календарь по дате определяет время звонка : D N W
"""
import MySQLdb
from datetime import datetime
from modules import cfg


class Calendar(object):
    """ Рабочий календарь ФГУП РСИ & ООО РСС """
    def __init__(self, dsn, table):
        self.year = int(table[1:5])     # table = Y2013M09
        self.month = int(table[6:8])
        db = MySQLdb.Connect(**dsn)
        cur = db.cursor()
        sql = "select cal from `calendar`.`calendar` where `year`={0:d} and `month`={1:d}".format(self.year, self.month)
        cur.execute(sql)
        if cur.rowcount == 1:
            self.cal = cur.fetchone()[0]
        cur.close()
        db.close()

    def dnw(self, dt):
        """
        Возвращает DNW по времени звонка
        :param dt: object datetime (2013-09-01 02:34:41)
        :return: D|N|W
        """
        if self.cal[dt.day-1] == '1':
            return 'W'

        if dt.hour in range(8, 20):
            return 'D'
        else:
            return 'N'

    @staticmethod
    def select_tar(dnw, tar0820, tar2008, tarw):
        """ выдирает тариф в зависимости от времени звонка D|N|W """
        if dnw == 'D':
            return tar0820
        elif dnw == 'N':
            return tar2008
        else:
            return tarw


if __name__ == '__main__':

    # определение типа дня по производственному календарю
    print("\n# календарь:")
    cal = Calendar(dsn=cfg.dsn_cal, table="Y2019M05")   # object Calendar
    for dt1 in (datetime(2019, 5, 1, 12, 41, 0),
                datetime(2019, 5, 6, 22, 41, 0),
                datetime(2016, 5, 6, 10, 41, 0)):
        _dnw = cal.dnw(dt1)
        print(("dt:{dt} dnw:{dnw}".format(dt=dt1, dnw=_dnw)))
