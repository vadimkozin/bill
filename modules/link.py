#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
link - полная информация по одной связи
"""


class Link(object):
    # строка для печати
    output = "{id:<6} {dt:<22} {fm_fmx:<25} |{cust_info:<43}| {to_tox:<36}|  {code_zona:<16} {stat_sts:<8} " \
             "{sec_min:<10} {tar_tara:<12} {sum_suma:<14} {desc_nid:<30}"

    def __init__(self):
        self.id = 0             # код анализируемой (текущей) записи в таблице
        self.dt = None          # дата-время вызова
        self.dnw = None         # DNW - время звонка (нужно для тарифов вед_МГ ФГУП РСИ)
        self.fm = None          # номер от 1
        self.fmx = None         # номер от 2
        self.fm2 = None         # номер от для отчётов
        self.to = None          # номер куда 1
        self.tox = None         # номер куда 2
        self.to2 = None         # номер куда для отчётов
        self.vpn = False        # номер из ВПН-номеров ?
        self.cust = None        # название клиента
        self.cid = 0            # код клиента (cust id) - из telefon.tkrss.ru
        self.org = None         # код принадлежности номера (R, G, I)
        self.uf = None          # код организации (u, f)
        self.code = None        # код направления (8312, 9160, 380, ...)
        self.zona = -2          # тарифная зона по России (1-6, 0-Moсква, -1 для зарубежья)
        self.sts = None         # тип звонка (mg mn vz mgs vm gd)
        self.stat = None        # тип звонка (M W S Z V G)
        self.st = None          # тип звонка (GD MG VZ: G->GD, MWS->MG, Z->VZ ) для отчётов Access
        self.nid = 0            # код направления
        self.desc = None        # направление (Новосибирск)
        self.name = None        # укрупнённое направление, напр. Вся сотовая кроме ВЗ = 'Россия моб.'
        self.sec = 0            # секунд
        self.min = 0            # полных минут, 50 сек = 1 мин
        self.tar = 0            # клиентский тариф за 1 мин.
        self.tara = 0           # агентский тариф за 1 мин.
        self.sum = 0            # клиентская сумма за разговор без НДС для юрлиц и с НДС для физлиц
        self.sum2 = 0           # клиентская сумма за разговор без НДС
        self.suma = 0           # агентская сумма за разговор без НДС
        self.op = None          # поток (m-Информ, q-Сервис)
        self.tid_t = 0          # код тарифного плана -> tariff.tariff_tel.tid
        self.pid = 0            # код клиента квартирного сектора для cid=549

    def prn_title(self):
        """
        Печать заголовка
        """
        print('-' * 200)
        print(self.output.format(id='id', dt='dt/dnw', fm_fmx='fm/fmx/vpn', to_tox='to/tox/op', cust_info='cust_info',
                                 code_zona='code/zona', stat_sts='stat/sts', desc_nid='desc/nid', sec_min='sec/min',
                                 tar_tara='tar/tara',
                                 sum_suma='sum/suma'))
        print('-' * 200)

    def prn(self):
        """
        Печать всех элементов
        """
        idx = "{idx}".format(idx=self.id)
        dt = "{dt} {dnw}".format(dt=self.dt, dnw=self.dnw)
        fm_fmx = "{fm}/{fmx}/{vpn}".format(fm=self.fm, fmx=self.fmx, vpn='+' if self.vpn else '-')
        to_tox = "{to}/{tox}/{op}".format(to=self.to, tox=self.tox, op=self.op)
        cust_info = "{cid}.{org}/{uf}/'{cust}'".format(cid=self.cid, org=self.org, uf=self.uf, cust=self.cust)
        code_zona = "{code}/{zona}".format(code=self.code, zona=self.zona)
        stat_sts = "{stat}/{sts}".format(stat=self.stat, sts=self.sts)
        desc_nid = "'{desc}'({nid})".format(desc=self.desc, nid=self.nid)
        sec_min = "{sec}/{min}".format(sec=self.sec, min=self.min)
        tar_tara = "{tar}/{tara}".format(tar=self.tar, tara=self.tara)
        sum_suma = "{sum}/{suma}".format(sum=self.sum, suma=self.suma)

        print(self.output.format(id=idx, dt=dt, fm_fmx=fm_fmx, to_tox=to_tox, cust_info=cust_info, code_zona=code_zona,
                                 stat_sts=stat_sts, desc_nid=desc_nid, sec_min=sec_min, tar_tara=tar_tara,
                                 sum_suma=sum_suma))


if __name__ == '__main__':
    lnk = Link()
    lnk.cid = 282
    print(lnk.cid)
    lnk.__init__()
    print(lnk.cid)

    lnk.prn()
