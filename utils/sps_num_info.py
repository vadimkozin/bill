#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
sps_num_info - возращает информацию по номеру СПС
sps_num_info.py --num=number --log=logfile

"""
import os
import sys
import re
import MySQLdb
from cfg import cfg

root = os.path.realpath(os.path.dirname(sys.argv[0]))
logfile = "{root}/log/{file}".format(root=root, file='num4gorod.log')


class SpsNumber(object):
    def __init__(self, dsn):
        """
        :param dsn: DSN к бд
        """
        self.db = MySQLdb.connect(**dsn)
        self.cursor = self.db.cursor()

    def __del__(self):
        self.cursor.close()
        self.db.close()

    def get_info_number(self, number):
        """
        возвращает информацию по номеру в виде строки
        """

        cursor = self.cursor

        if re.match('^\d{3}-\d{7}$', number):
            abc, num = number.split('-')
        elif re.match('^[78]?\d{10}', number):
            abc = number[:3]
            num = number[3:]
        else:
            return None

        sql = "SELECT `abc`, `fm`, `to`, `capacity`, `zona`, `stat`, `oper`, `region` FROM `defCode` " \
              "WHERE abc='{abc}' and '{num}' BETWEEN `fm` and `to`".format(abc=abc, num=num)

        cursor.execute(sql)
        if cursor.rowcount == 0:
            return None

        line = cursor.fetchone()
        abc, fm, to, capacity, zona, stat, oper, region = line
        res = "abc:{abc} fm:{fm} to:{to} capacity:{capacity:<8} zona:{zona}  type:{stat}  oper:{oper:<28} region:{region}".\
            format(abc=abc, fm=fm, to=to, capacity=capacity, zona=zona, stat=stat, oper=oper, region=region)
        return res


if __name__ == '__main__':

    numbers = (
        '953-9758002',
        '978-5768979',
        '981-6664279',
        '996-9166596',
    )

    sn = SpsNumber(dsn=cfg.dsn_tar)
    for num in numbers:
        info = sn.get_info_number(num)
        print("{num} -> {info}".format(num=num, info=info))
