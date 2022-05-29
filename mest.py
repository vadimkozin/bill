#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Биллинг местной связи ,это не повремёнка, а конкретно местная связь - оплата за каждую минуту
Есть клиенты у которых считаем такую связь.
См. таблицу tarif.tariff_tel.city > 0 , например, city=0.43 - 43 копейки за куждую минуту местной связи

run: mest.py --year=2022 --month=3    // расчёт местной связи за март-2022

таблицы как основа:
tarif.mest_book - книга по местной связи
{account, period, cid, uf, dt, min, cost1min, sum, prim }

На примере 2022_03:
1) Результат за месяц кладём в tarif.mest_book (period=2022_03)
2) По результату формируем отчёт в ./mest/2022_03_mest.xls
-
"""
import optparse
import traceback
import time
import pymysql
import logging
from datetime import datetime
from cfg import cfg, ini
from modules import utils as ut
from modules.xlsmest import BillMestXls

path_result = cfg.paths['result']       # корень для результатов
dir_result = cfg.paths['mest']['dir']   # под-директория для файлов с местной связью (очень мало)
flog = cfg.paths['logging']['mest']     # лог-файл
shema = "{root}/sql/table_mest_book.sql".format(root=cfg.root) # sql-схема твблицы местной связи (mest_book)


def xlog(msg, out_console=True):
    """
    Пишет в лог и на консоль если out_console=True
    :param msg: сообщение для логирования
    :param out_console: True|False флажок
    :return:
    """
    log.info(msg)
    if out_console:
        print(msg)


def execute(cursor, sql, save_db=True):
    """
    Выполнение запроса на изменение(INSERT, UPDATE) к БД
    :param cursor: курсор
    :param sql: sql-запрос
    :param save_db: флажож True|False - сохраняем или нет в БД
    :return: кол-во select/insert/update записей
    """
    if save_db:
        cursor.execute(sql)
        return cursor.rowcount
    else:
        return 0


def get_last_account(cursor, table, field='account'):
    """
    Возвращает последний номер счёта account
    :param cursor: курсор
    :param table: таблица
    :param field: название поля у которого вычисляем последний номер
    :return: последний account
    """
    sql = "SELECT max(`{field}`) FROM {table}".format(field=field, table=table)
    cursor.execute(sql)
    max_number = cursor.fetchone()[0]
    if not max_number:
        max_number = 0

    return max_number


class BillingMest(object):
    """
    Телефонный биллинг местных связей
    """

    def __init__(self, opts, dsn_bill, dsn_tar, dsn_cust):
        """
        Биллинг местной связи
        :param dsn_bill: параметры подключения к базе биллинга
        :param dsn_tar: параметры подключения к базе тарифов
        :param dsn_cust: параметры подключения к базе клиентов
        # :param info: текст для логирования
        :param opts: параметры
        """
        self.opts = opts
        self.dsn_bill = dsn_bill
        self.dsn_tar = dsn_tar
        self.dsn_cust = dsn_cust
        self.cid2tar = dict()   # отображение кода клиента на на тариф местной связи: cid->cost1min

    def _read_mest_tar(self):
        """
        Чтение тарифов (стоимость 1 минуты) для местной связи
        (создание отношения cid2tar: cid->cost1min)
        :param dsn: параметры подключения к базе тарифов
        :param table: таблица с тарифными планами клиентов (tarif.tariff_tel)
        :return: cid2tar - мапа cid->cust1min для клиентов, у которых нужно считать местную связь
        """
        db = pymysql.Connect(**self.dsn_tar)

        cursor = db.cursor()
        table = self.opts.table_tariff

        cid2tar = {}

        # клиенты с оплачиваемой местной связью
        sql = 'SELECT `cid`, `city` cost1min FROM {table} WHERE `city`>0 ORDER BY `cid`'.format(table=table)

        cursor.execute(sql)
        for line in cursor:
            cid, cost1min = line
            cid2tar[cid] = cost1min
        cursor.close()
        db.close()
        return cid2tar

    def bill(self):
        t1 = time.time()
        period = self.opts.period
        table_book = self.opts.table_book

        xlog('period: {period}'.format(period=period))

        db = pymysql.Connect(**self.dsn_bill)
        cursor = db.cursor()
        cursor_insert = db.cursor()

        # клиенты с оплачиваемой местной связью
        cid2tar = self._read_mest_tar()

        cid2sum = {}  # cid => {cid, sum_min, cost1min, summa}

        # сумма к оплате для клиентов с оплачиваемой местной связью*
        for cid in cid2tar:
            cost1min = cid2tar[cid]
            table = self.opts.table_bill
            sql = "SELECT cid, sum(min) sum_min, '{cost1min}' AS `cost1min`, sum(min)*{cost1min} AS `summa`" \
                  " FROM bill.{table} d WHERE cid={cid} AND stat='G' GROUP BY cid".\
                format(cost1min=cost1min, table=table, cid=cid)

            cursor.execute(sql)
            for line in cursor:
                cid, sum_min, cost1min, summa = line
                cid2sum[cid] = {'cid': cid, 'min': sum_min, 'cost1min': cost1min, 'summa': summa}

        # удаляем записи из книги местной связи за период
        sql = "DELETE FROM {table} WHERE period='{period}'".format(table=table_book, period=period)
        rows = execute(cursor, sql)
        xlog('deleted {rows} rows from {table}, period={period}'.format(rows=rows, table=table_book, period=period))

        # последний account
        account = get_last_account(cursor, table=table_book)

        # добавляем записи в книгу местной связи за период
        count = 0
        date_now = ut.sqldate(datetime.date(datetime.now()))
        for cid in cid2sum:
            account += 1
            it = cid2sum[cid]

            sql = "INSERT INTO {table} (account, period, cid, uf, dt, min, cost1min, sum, prim) " \
                  "VALUES ('{account}', '{period}', '{cid}', '{uf}', '{dt}', '{min}', " \
                  "'{cost1min}', '{sum}', '{prim}')".\
                format(table=table_book, account=account, period=period, cid=cid, uf='u',
                       dt=date_now, min=it['min'], cost1min=it['cost1min'], sum=it['summa'], prim='+')
            count += execute(cursor_insert, sql)

        xlog('added {count} rows into {table}, period={period}'.
             format(count=count, table=table_book, period=period))

        cursor.close()
        cursor_insert.close()
        db.close()

        t2 = time.time()
        xlog("work: {0:0.2f} sec".format(t2 - t1))


if __name__ == '__main__':
    p = optparse.OptionParser(description="billing of mest calls ",
                              prog="mest.py", version="0.1a", usage="mest.py --year=year --month=month [--log=file]")

    p.add_option('--year', '-y', action='store', dest='year', help='year, example 2021')
    p.add_option('--month', '-m', action='store', dest='month', help='month in range 1-12')
    p.add_option('--log', '-l', action='store', dest='log', default=flog, help='logfile')
    p.add_option("--reset", "-r",
                 action="store_true", dest="reset", default=False,
                 help="option only for compatibility with bill.py")

    opt, args = p.parse_args()

    # параметры в командной строке - в приоритете
    if not (opt.year and opt.month):
        opt.year = ini.year
        opt.month = ini.month

    if not opt.year or not opt.month or not opt.log:
        print(p.print_help())
        exit(1)

    opt.table_bill = 'Y{year:04d}M{month:02d}'.format(year=int(opt.year), month=int(opt.month))
    opt.period = '{year:04d}_{month:02d}'.format(year=int(opt.year), month=int(opt.month))

    opt.table_tariff = 'tarif.tariff_tel'
    opt.table_book = 'bill.mest_book'
    opt.table_customers = 'customers.Cust'

    logging.basicConfig(
        filename=opt.log, level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S", format='%(asctime)s %(message)s', )
    log = logging.getLogger('app')

    try:
        # Биллинг местной связи -> результат в bill.mest_book
        mest = BillingMest(opt, dsn_bill=cfg.dsn_bill2, dsn_tar=cfg.dsn_tar, dsn_cust=cfg.dsn_cust)
        mest.bill()

        # Результат из таблицы bill.mest_book преобразуем в xls-файл
        xls = BillMestXls(dsn=cfg.dsn_bill2, year=opt.year, month=opt.month, path=path_result, directory=dir_result)
        xls.create_file()

        xlog('.')

    except pymysql.Error as e:
        log.exception(str(e))
        print(e)
    except RuntimeError as e:
        log.exception(str(e))
        print(e)
    except Exception as e:
        log.exception(str(e))
        traceback.print_exc(file=open(opt.log, "at"))
        traceback.print_exc()
