#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
разные функции
"""
import re
import time
from io import open


class Func(object):
    @staticmethod
    def sec2min(sec):
        """ second to min with round to up """
        return int(sec/60) + (0, 1)[sec % 60 > 0]

    @staticmethod
    def cost(sec, tar1min):
        """ calculate cost """
        return round(Func.sec2min(sec) * tar1min, 2)

    @staticmethod
    def progress_(step, records, current):
        """
        print progress in %
        :param step: step by step in progress
        :param records: all records
        :param current: current procent (1-100)
        :return: proc_pred
        """
        proc = int((float(step) / records) * 100)
        if proc != current:
            current = proc
            print("%02d%%" % proc)
        return current

    @staticmethod
    def arraycode(code1, code2):
        """
        code1 = '100'; code2= '30-35, 50, 60'
        return: [10030,10031,10032,10033,10034,10035,10050,10060]
        """
        abc = []
        if not code2:
            abc.append("{code1}".format(code1=code1))
            return abc

        lst = code2.strip().split(',')
        for item in lst:
            item = item.strip()
            if item.find('-') > 0:
                start, stop = item.split('-')
                lenstart = len(start)
                for x in range(int(start), int(stop)+1):
                    abc.append("{code1}{x}".format(code1=code1, x=Func.strz(x, lenstart)))
            else:
                abc.append("{code1}{x}".format(code1=code1, x=item))
        return abc

    @staticmethod
    def strz(val, lenstr):
        """
        val = '90', lenstr=3;  return '090'
        """
        sval = str(val)
        n = lenstr - len(sval)
        if n >= 0:
            r = '0000000000'[:n] + sval
        else:
            r = sval
        return r

    @staticmethod
    def date_first_day_month(table):
        return "%s-%s-01" % (table[1:5], table[6:8])  # Y2013M09 => 2013-09-01

    @staticmethod
    def prn_ruler():
        print("         10        20        30        40        50        60        70        80        90       100  ")
        print("0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012")

    @staticmethod
    def save_noexist_number(db, table, idx, fm, fmx, to, stat, filename):
        """
        Сохранение информации по неизвестному номеру fm/fmx в filename
        :param db: имя базы данных
        :param table: таблица
        :param idx: код записи в таблице
        :param fm: исходящий телефонный номер, первый из пары fm/fmx
        :param fmx: исходящий телефонный номер, второй из пары fm/fmx
        :param to: вызываемый телефонный номер
        :param stat: тип звонка (M W S Z V G)
        :param filename: имя лог-файла для сохранения инфо
        """
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        st = "{ts} {fm}/{fmx} -> {to}/{stat} {db}.{table}:{idx}\n".\
            format(db=db, table=table, fm=fm, fmx=fmx, to=to, stat=stat, ts=ts, idx=idx)
        f = open(filename, "at", encoding='utf8')
        f.write(st)
        f.close()

    @staticmethod
    def get_cid_for_billing(cid, org, uf):
        """
        Определяет тарифный план номера клиента
        :param cid : код клиента (владелец исходящего номера)
        :param org : принадлежность номера к организации (R-РСС; G-ФГУП РСИ)
        :param uf : тип клиента (u-юрлицо f-физлицо)
        :return : возвращает код клиента из таблицы тарификации (фактически тарифный план)
        """
        """
        коды клиентов в таблицах тарифов (ФГУП + РСС)
        cids = (84, 273, 549, 760, 787, 952, 953, 957, 1171, 1172)
        84=957 - минимальные тарифы = агентским
        549 - квартирный сектор для ФГУП РСИ (626-e)
        953 - организации (юрлица) для ФГУП РСИ (626-e)
        org=R:
          273,549,760,787,952 - для клиентов с этими кодами
          оставшиеся:
          1171 - юрлица
          1172 - физлица
        org=G:
          273,549,760,787,952 - для клиентов с этими кодами
          оставшиеся:
          953 - юрлица
        """

        # индивидуальные тарифы
        if int(cid) in (84, 273, 549, 760, 787, 952, 957, 58):
            return cid

        # тарифы по умолчанию для юр_лиц/физ_лиц
        if org == 'R':
            # return (1172, 1171)[uf == 'u'] # так оно правильно, но
            return (549, 1171)[uf == 'u']    # такое принято решение 2016-03-14
        elif org == 'I':
            return 957  # ВПН
        elif org == 'G':
            return 953  # юр_лица, а физ_лица(549) учитываются в индивид_тарифах
        else:
            return 0

    @staticmethod
    def prepare_to(to):
        """
        проверка номера вызываемого абонента
        """
        # 810800xxxxxxx -> 8800xxxxxxx
        re_800 = re.compile('^(7|8)10800(\d{7})')
        tox = to
        m = re_800.match(to)
        if m:
            tox = '8' + to[3:]
        return tox

    @staticmethod
    def get_number_7digits(num):
        """
        проверяет номер на префиксы 8495; 495; 8499; 499
        и возвращает номер без префикса
        """
        if num.startswith('8495') or num.startswith('8499'):
            return num[4:]
        elif num.startswith('499') or num.startswith('495'):
            return num[3:]
        return num

    @staticmethod
    def get_number_fm2(fm, fmx):
        """
        возвращает номер для печати в отчётах (fm2)
        :param fm: поле fm
        :param fmx: поле fmx
        :return: fm2
        """
        re_253xx = re.compile('^253\d{2}$')
        re_8120xxx = re.compile('^8120\d{3}$')
        re_8124xxx = re.compile('^8124\d{3}$')
        re_8125xxx = re.compile('^8125\d{3}$')
        re_fmx = re.compile('^(7|8)?(495|499)')
        re_626642 = re.compile('^(626|642)\d{4}')

        # is_len_fmx11 = len(fmx) == 11 # < 2021_08
        is_len_fmx10 = len(fmx) == 10   # >= 2021_08

        if not is_len_fmx10:
            return '?'

        if fm == fmx:
            return fm[-7:]   # 84996428448 84996428448 -> 6428448
        if fm[1:] == fmx:
            return fm[-7:]   # 74996428464 4996428464 -> 6428464
        if re_626642.match(fm) and re_fmx.match(fmx):
            return fm        # 6428289 4996428289 -> 6428289
        # if re_253xx.match(fm) and fmx.startswith('8499'):
        if re_253xx.match(fm) and re_fmx.match(fmx):
            return fmx[-7:]  # 25331 84996428466 -> 6428466 (cid=58)
        # if re_8120xxx.match(fm) and fmx.startswith('8495'):
        if re_8120xxx.match(fm) and re_fmx.match(fmx):
            return fm[-3:]   # 8120101 84956269082 -> 101 (cid=53)
        # if re_8124xxx.match(fm) and fmx.startswith('8495'):
        if re_8124xxx.match(fm) and re_fmx.match(fmx):
            return fm[-3:]   # 8124203 84956269696 -> 203 (cid=273)
        # if re_8125xxx.match(fm) and fmx.startswith('8499'):
        if re_8125xxx.match(fm) and re_fmx.match(fmx):
            return fm       # 8125207 84996428459 -> 8125207 (cid=58)


if __name__ == '__main__':

    for _uf in ('u', 'f'):
        for _org in ('R', 'G'):
            for _cid in (273, 84, 999, 15, 957, 549):
                cid_bill = Func.get_cid_for_billing(_cid, _org, _uf)
                print("{cid}/{org}/{uf} --> {cid_bill}".format(cid=_cid, org=_org, uf=_uf, cid_bill=cid_bill))
            print("--")
        print("----")

    # 810800xxxxxxx -> 8800xxxxxxx
    _to = '8108001234567'
    _tox = Func.prepare_to(_to)
    print("{to}->{tox}".format(to=_to, tox=_tox))

    print("\n# удаление префикса:")
    for n1 in ('84951234567', '4951234567', '4991234567', '84991234567', '1234567'):
        n2 = Func.get_number_7digits(n1)
        print("{num1:15s} -> {num2}".format(num1=n1, num2=n2))

    print("\n# номер для отчётов fm3:")
    for fm, fmx in (('25331', '84996428466'), ('8120101', '84956269082'), ('8124203', '84956269696'),
                    ('8125207', '84996428459'), ('84956261000', '84956261000')):
        fm3 = Func.get_number_fm2(fm, fmx)
        print("{fm} {fmx} ->{fm3}".format(fm=fm, fmx=fmx, fm3=fm3))
