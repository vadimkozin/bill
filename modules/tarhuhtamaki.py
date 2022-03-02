#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
tarhuhtamaki - модуль тарификации отдельного МН-тарифа для Хухтамаки(1320)
"""
import MySQLdb
from modules import cfg


class TarHuhtamaki(object):
    """
    тарифы для Хухтамаки(1320)
    """
    def __init__(self, dsn, table):
        """
        :param dsn: dictionary dsn     (dsn_tar)
        :param cur: cursor
        :param table: table of tariff (tar_h)
        """
        self.dsn = dsn
        self.table = table
        self.names = dict()
        self.tariff = list()
        self._read_()

    def _read_(self):
        db = MySQLdb.Connect(**self.dsn)
        cur = db.cursor()

        sql = "SELECT code, dest, tar FROM {table} WHERE code RLIKE '^[0-9]+$' ORDER BY code DESC".\
            format(table=self.table)
        cur.execute(sql)

        for line in cur:
            code, dest, tar = line
            self.tariff.append(dict(code=code, dest=dest, tar=tar))

        cur.close()
        db.close()

    def print_tar(self):
        for line in self.tariff:
            print(line)

    def get_info(self, number):
        if len(number) < 7:
            return None

        if number.startswith('10'):
            number = number[2:]

        for tar in self.tariff:
            if number.startswith(tar['code']):
                return tar
        return None


if __name__ == '__main__':
    tar = TarHuhtamaki(dsn=cfg.dsn_tar, table='tar_h')
    tar.print_tar()

    _numbers = ['10891001533292', '10989264190564', '1097246309630', '10884950702143', '10566505500', '105',
                '1048225895500', '1041447340714', '10375173631260', '1037322408300', '101700702033']

    for num in _numbers:
        info = tar.get_info(num)
        print('{number}: {info}'.format(number=num, info=info))
