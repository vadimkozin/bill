#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import time
import logging
import MySQLdb
import traceback
from io import open

"""
 codedef_info - Модуль по ПОЛУЧЕНИЮ информации о кодах сотовой подвижной связи (СПС)

  как использовать:
  import codedef

  # экземпляр
  cdef = CodedefInfo(dsn=cfg.dsn_tar, tabcode='defCode')

  # Определить является ли number номером ВЗ-связи
  number='89010136501'
  vz = cdef.is_codevz(number) # True | False

  # получить инфо по  ВЗ-номеру
  number='89010136501'
  code, zona, stat, region = cdef.get_mysql_code_zona_stat_reg(num) # запрос к mysql-серверу
  print code, zona, stat, region                                    # '901013', 4, 'mg', 'Республика Адыгея'

 - - - - - - - - - - - - - - - - - - - - - - -  - - - - - - - - -  - - - - - - - - - - - -  - - - - - - - - - - - - - -
 особенности реализации:
 defCode - все коды СПС (abc, fm, to, capacity, zona, stat, oper, region, area)
  пример одной записи:
  abc       = 901
  fm        = 2020000
  to        = 2029999
  capacity  = 10000
  zona      = 0
  stat      = vz
  oper      = ОАО "МТТ"
  region    = г. Москва
  area      = '-'  (пространство включающее в себя регион)
  Коды загружаются с сайта Россвязи в базу данных MySql (в модуле codedef.py)
"""


def xprint(msg):
    print (msg)


class CodedefInfo(object):
    """
    Информация о кодах сотовой связи
    """

    def __init__(self, dsn, tabcode):
        """
        :param dsn: параметры подключения к БД
        :param tabcode: таблица с def-кодами tarif.defCode
        """
        self.dsn = dsn                      # dsn подключения к базе
        self.db = MySQLdb.Connect(**dsn)    # db - активная ссылка на базу
        self.cur = self.db.cursor()         # cur - курсор
        self.tabCode = tabcode              # таблица с кодами (tarif.defCode)

        self.codevz = list()                # (9160000000-9169999999,9210000000-9219999999,...)  коды СПС

        self.read_codevz()

    def __del__(self):
        self.cur.close()
        self.db.close()

    def read_codevz(self):
        """
        Чтение кодов ВЗ связи в список (9001230000-9001239999,9991230000-9991239999,..)
        :return: количество элементов в списке
        """
        step = 0
        sql = "select `abc`, `fm`, `to` from `{table}` where `stat`='vz'".format(table=self.tabCode)
        self.cur.execute(sql)
        for line in self.cur:
            abc, fm, to = line
            _fm = "{abc}{fm}".format(abc=abc, fm=fm)    # 9160000000
            _to = "{abc}{fm}".format(abc=abc, fm=to)    # 9169999999
            x = "{fm}-{to}".format(fm=_fm, to=_to)      # 9160000000-9169999999
            self.codevz.append(x)
            step += 1

        self.codevz.sort()
        return step

    def is_codevz(self, code):
        """
        Возвращает является ли code номером ВЗ-связи
        :param code: номер типа 916021xxxx или 8916021xxxx
        :return: True|False
        """
        if len(code) == 11 and code.startswith('89'):  # 8916021xxxx
            code = code[1:]

        for x in self.codevz:
            _fm, _to = x.split('-')  # 9160000000-9169999999
            if _fm <= code <= _to:
                return True

        return False

    def print_codevz(self, filename=None):
        """ Печать всех кодов ВЗ-связи
        :param filename: если есть, то дополнительно печать в файл
        """
        f = None
        if filename:
            f = open(filename, "wt", encoding='utf8', )

        n = 0
        for a in self.codevz:
            xprint(a)
            if f:
                f.write(a + "\n")
            n += 1

        st = "itogo:{n} records".format(n=n)
        xprint(st)
        if f:
            f.write(st + "\n")
            f.close()

    def get_mysql_code_zona_stat_reg(self, number):
        """
        Запрос к mysql: ищем инфо по номеру number
        :param number: (8)9034230000
        :return: (code, zona, stat, region): ('903423', 4, 'mg', 'Республика Дагестан')
        !stat in (mg, vz, sp)
        """
        num = number
        if len(num) == 11 and (num.startswith('89') or num.startswith('79')):
            num = number[1:]  # 9034230000
        abc = num[:3]  # 903
        code = num[3:]  # 4230000

        sql = "select `zona`, `stat`, `region` from `defCode` where `abc`='{abc}' " \
              "and {code} >= `fm` and {code} <= `to`".format(abc=abc, code=code)

        self.cur.execute(sql)
        zona, stat, region = (-1, '-', '-')
        if self.cur.rowcount > 0:
            zona, stat, region = self.cur.fetchone()

        return "{abc}{code}".format(abc=abc, code=code), zona, stat, region


if __name__ == '__main__':

    try:
        import cfg
        # РАЗЛИЧНЫЕ ТЕСТЫ ---------------------
        cdef = CodedefInfo(dsn=cfg.dsn_tar, tabcode='defCode')

        xprint("# проверка номера на ВЗ-связь:")
        for prefix in ('9160258525', '9425010000', '9160218525', '9030005755'):
            xprint("{prefix} - is VZ: {result}".format(prefix=prefix, result=cdef.is_codevz(prefix)))

        xprint("---")

        xprint("# определение информации по номеру:")
        numbers = ('89010019999', '89010136501', '89160218525', '79161303979', '9825345269', '9164657112', '9265370878',
                   '9296451321', '9175812988', '9258362114', '79057958148', '79200331414', '79600298005')

        t1 = time.time()
        for num1 in numbers:
            _code, _zona, _stat, _region = cdef.get_mysql_code_zona_stat_reg(num1)
            xprint("{code}  {zona}  {stat}   {region}".format(code=_code, zona=_zona, stat=_stat, region=_region))
        xprint('time:{time:.3f}s'.format(time=time.time() - t1))

        xprint("---")

        # печать кодов ВЗ-связи
        # cdef.print_codevz()

    except MySQLdb.Error as e:
        xprint(e)
    except Exception as e:
        traceback.print_exc()
