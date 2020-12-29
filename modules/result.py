#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
result - суммарная информация по отобранным связям
 res = Result()
 res.add('mg', sec=110, min=2, sum=3.65, suma=1.55)
 res.add('mn', sec=185, min=3, sum=33.15, suma=10.18)
 ...
 print res.result('mg')
 print res.result('mn')
"""


class ResultItem(object):
    """
    Один объект для итогов
    """
    def __init__(self):
        self.sec = 0    # секунд
        self.min = 0    # минут
        self.sum = 0    # сумма клиентская
        self.suma = 0   # сумма агентская
        self.count = 0  # количество связей


class Result(object):
    """
    Итоги
    """
    items = ('mg', 'mn', 'vz', 'mgs', 'gd', 'vm')
    items_mg = ('mg', 'mn', 'vz', 'mgs')

    def __init__(self):
        self.sts = dict()
        for item in self.items:
            self.sts[item] = ResultItem()

    def add(self, sts, sec, min, sum, suma):

        if sts in self.sts:
            p = self.sts[sts]
        else:
            return

        p.sec += sec
        p.min += min
        p.sum += sum
        p.suma += suma
        p.count += 1

    def result(self, sts):
        """
        Возвращает строку с результатом для вида связи sts: mg,mn,...
        """
        if sts in self.sts:
            p = self.sts[sts]
        else:
            return

        return "{sts}:({count}/{sec}/{min}/{sum}/{suma}) "\
            .format(sts=sts, count=p.count, sec=p.sec, min=p.min, sum=round(p.sum, 2), suma=round(p.suma, 2))

    def result_all(self):
        """
        по ВСЕМ видам связи возвращает строку с результатами
        """
        r = ""
        for sts in self.items:
            r += self.result(sts)
        return r

    def get_result_mg(self):
        """
        Возвращает итоги по общему направлению МГ ('mg', 'mn', 'vz', 'mgs')
        :return: dict(sec, min, sum, suma)
        """
        r = dict(sec=0, min=0, sum=0, suma=0)
        for sts in self.items_mg:
            p = self.sts[sts]
            r['sec'] += p.sec
            r['min'] += p.min
            r['sum'] += p.sum
            r['suma'] += p.suma
        return r


if __name__ == '__main__':
    res = Result()
    res.add('mg', sec=110, min=2, sum=3.65, suma=1.55)
    res.add('mn', sec=185, min=3, sum=33.15, suma=10.18)
    res.add('mg', sec=1000, min=17, sum=85.05, suma=15.05)

    for sts in ('mg', 'mn', 'vz', 'gd', 'vm', 'mgs'):
        print(res.result(sts))

    print(res.result_all())

    # результат только по МГ/МН/ВЗ
    r = res.get_result_mg()
    print(r)
    print('min_mg:', r['min'])





