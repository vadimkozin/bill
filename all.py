#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Весь биллинг в одном файле:
all.py --year=2022 --month=6 [--mail=address@mail.ru]

Выполнит скрипты один за другим:
1) load.py  -- загрузит данные из SMG
2) bill.py  -- посчитает МГ/МН/ВЗ
3) mts.py -- сделает отчёты на МГ/МН/ВЗ для загрузки на портал МТС
4) mest.py  -- посчитает местную связь (у некоторых новых клиентов есть пункт про подсчёт местной связи)
5) local.py -- посчитает повремёнку (> 450 мин местной)
6) compress.py - сожмёт результат (zip) в виде bill_2022_06.zip
7) отправит результат по почте на address@mail.ru
ps. если ключ --mail в командной строке не указан, тогда поищет ключ mail в cfg.py

"""
import time
import optparse
import traceback
from cfg import cfg
from modules import logger
from load import main as load
from bill import main as bill
from mts import main as mts
from mest import main as mest
from local import main as local

flog = cfg.paths['logging']['all']  # лог-файл
log = logger.Logger(flog).log       # ф-ия логгер


class AllBilling(object):
    def __init__(self, year, month):
        """
        Весь Биллинг
        :param year: год
        :param month: месяц
        """
        self.year = year
        self.month = month
        self.table = 'Y{year:04d}M{month:02d}'.format(year=int(year), month=int(month))     # Y2022M06
        self.period = '{year:04d}_{month:02d}'.format(year=int(year), month=int(month))     # 2022_06
        self.delimiter = '--------------------------'
        self.t1 = time.time()

    def start(self):
        log('billing {period}.'.format(period=self.period))
        self.t1 = time.time()

    def end(self):
        log("work: {0:0.2f} sec".format(time.time() - self.t1))
        log('.')

    def load(self):
        print(self.delimiter)
        log('# 1) load:')
        load(year=self.year, month=self.month)

    def bill(self):
        print(self.delimiter)
        log('# 2) bill:')
        bill(year=self.year, month=self.month)

    def mts(self):
        print(self.delimiter)
        log('# 3) mts:')
        mts(year=self.year, month=self.month)

    def mest(self):
        print(self.delimiter)
        log('# 4) mest:')
        mest(year=self.year, month=self.month)

    def local(self):
        print(self.delimiter)
        log('# 5) local:')
        local(year=self.year, month=self.month)


if __name__ == '__main__':
    p = optparse.OptionParser(description="All action for billing ",
                              prog="all.py", version="0.1a",
                              usage="all.py --year=year --month=month [--log=file] [--email=address]")

    p.add_option('--year', '-y', action='store', dest='year', help='year, example 2022')
    p.add_option('--month', '-m', action='store', dest='month', help='month in range 1-12')
    p.add_option('--log', '-l', action='store', dest='log', default=flog, help='logfile')
    p.add_option('--email', '-e', action='store', dest='mail', default=cfg.email, help='address email for result')

    opts, args = p.parse_args()

    if not opts.year or not opts.month or not opts.log or not opts.mail:
        print(p.print_help())
        exit(1)

    try:
        ab = AllBilling(year=opts.year, month=opts.month)

        ab.start()
        ab.load()   # загружаем
        ab.bill()   # считаем
        ab.mts()    # csv-файлы для МТС
        ab.mest()   # местная
        ab.local()  # повремёнка
        ab.end()

    except Exception as e:
        log(e.args)
        traceback.print_exc(file=open(flog, "at"))
