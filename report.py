#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Создание отчётов для выгрузки оператору MTC:
исходные данные: таблица типа Y2015M11
получаем:
rss         - объём оказанных услуг по направлениям(МГ/МН/ВЗ) с детализацией по клиентам
rss_book    - книга продаж
rss_serv    - подробно услуги (МГ ВЗ) для книги продаж
rss_bookf   - книга извещений для физлиц
rss_servf   - подробно услуги (МГ ВЗ) для книги извещений
rss_akt     - акт выполненных работ
+
файлы для выгрузки оператору на портал : oper/rss/2015_12/cp1251  и oper/inf/2015_12/cp1251

"""
import os
import sys
import optparse
import logging
import MySQLdb
import traceback
import time
import datetime
from io import open
from modules import cfg
from modules import utils as ut
from modules import customers
from modules import xlsreports
import ini

root = os.path.realpath(os.path.dirname(sys.argv[0]))
pathsql = "{root}/sql/reports/".format(root=root)   # файлы sql-команд для создания таблиц
pathreports = "{root}/reports/".format(root=root)   # файлы для выгрузки оператору МТС (utf-8)
path_results = "{root}/results".format(root=root)   # файлы с результатом по МГ/ВЗ (utf-8) для выст_счетов

flog = "{root}/log/{file}".format(root=root, file='report.log')     # лог-файл
stat2service = {'M': 'MG', 'S': 'MG', 'W': 'MN', 'Z': 'VZ'}         # мапа stat -> service

# отчёты (csv-файлы) для выгрузки оператору
file_reports = dict(
    services=('MG', 'VZ', 'ALL'),
    reports=dict(detal='детал_уступки_треб', book='книга_продаж', amount='объём_оказ_услуг', total='отчет_оператора',
                 itog='итог'),
    ext='csv',
)


def get_custlist(data, uf, cidexclude, custlist):
    """
    Возвращает список кодов клиентов
    :param data: ссылка на данные
    :param uf: u-юрлица f-физлица
    :param cidexclude: исключаемый кортеж клиентов (1,2,100,...)
    :param custlist: ссылка на массив с результатом
    :return: размер списка
    """
    [custlist.append(q.cid) for q in data.values() if q.cid not in cidexclude and q.uf == uf]
    custlist.sort()
    return len(custlist)


def boolsql(b):
    """
    Преобразует True->1, False->0
    :param b: bool
    :return: 1 or 0
    """
    return 1 if b else 0


def stat2serv(stat):
    """
    Преобразует stat в service
    :param stat: M,W,S,Z
    :return: MG,MN,VZ
    """
    global stat2service
    return stat2service.get(stat, '-')


def create_table(dsn, filename, table):
    """
    Обёртка для создания таблицы c логированием
    :param dsn:
    :param filename:
    :param table:
    :return:
    """
    ut.create_table(dsn, filename, table)
    log.info("create table: `{name}`".format(name=table))


def insert_db(cursor, table, **kwargs):
    """
    Вставка строки в таблицу table
    :param cursor: курсор
    :param table: таблица
    :param kwargs: хеш параметров
    :return: количество вставленных строк
    """
    keys = list(kwargs.keys())
    sql = "INSERT INTO `{table}` (".format(table=table)

    for key in keys:
        sql += "`{key}`,".format(key=key)

    sql = sql[:-1] + ") VALUES ("

    for key in keys:
        sql += "'{val}',".format(val=kwargs[key])

    sql = sql[:-1] + ")"

    cursor.execute(sql)
    return cursor.rowcount


def prepare_table(dsn, file_sql, table, year, month, delete_period=False):
    """
    Если таблица не существует, то создаём её, а если есть, то удаляем период если delete_period=True
    :param dsn:
    :param file_sql: имя файла с командой создания таблицы
    :param table: название таблицы
    :param year: год
    :param month: месяц
    :param delete_period: True | False - удалять или нет период year+month из таблицы
    :return: созданная таблица или удалённый период из таблицы
    """
    db = MySQLdb.Connect(**dsn)
    cursor = db.cursor()
    try:
        cursor.execute("UPDATE `{table}` SET id=-1 WHERE id=-1". format(table=table))
        if delete_period:
            cursor.execute("DELETE FROM `{table}` WHERE `year`='{year}' AND `month`='{month}'".format
                           (table=table, year=year, month=month))
            log.info("delete {records} records from `{table}` year={year} month={month}".format
                     (table=table, year=year, month=month, records=cursor.rowcount))

    except MySQLdb.Error as e:
        if e[0] == 1146:    # (1146, "Table doesn't exist")
            create_table(dsn=dsn, filename=file_sql, table=table)
        else:
            log.warning(str(e))
            print(e)
    cursor.close()
    db.close()


class ReportSumma(object):
    """ Итоговые суммы в отчётах по ВЗ, МГ, МН """
    def __init__(self, sum=0, nds=0, vsego=0):
        self.sum = sum
        self.nds = nds
        self.vsego = vsego

    def out(self):
        return "sum:{sum:0.2f}  nds:{nds:0.2f}  vsego:{vsego:0.2f}".format(sum=self.sum, nds=self.nds, vsego=self.vsego)


class AtomSumma(object):
    """ Итоговые суммы за период """
    def __init__(self, serv, sumraw, sumcust, sumoper, sumsec, summin, calls):
        self.serv = serv    # MG, MN, VZ
        self.calls = calls          # всего звонков по направлению
        self.sumraw = sumraw        # сумма без НДС
        self.sumcust = sumcust      # сумма клиенту, для юрлиц sumraw=sumcust
        self.sumoper = sumoper      # сумма оператору по агентским тарифам
        self.sumsec = sumsec        # всего секунд по направлению
        self.summin = summin        # всего минут по направлению


class AtomSumDir(object):
    """
    Итоговые суммы по направлениям (МГ МН ВЗ) по всем клиентам
    """
    def __init__(self, cid, uf):
        """
        :param cid: Customer id
        :param uf: 'u' | 'f'
        :return:
        """
        self.cid = cid
        self.uf = uf
        self.vz = AtomSumma('vz', 0, 0, 0, 0, 0, 0)
        self.mg = AtomSumma('mg', 0, 0, 0, 0, 0, 0)
        self.mn = AtomSumma('mn', 0, 0, 0, 0, 0, 0)

    def setvz(self, sumraw, sumcust, sumoper, sumsec, summin, calls):
        self.vz.serv = 'vz'
        self.vz.sumraw = sumraw
        self.vz.sumcust = sumcust
        self.vz.sumoper = sumoper
        self.vz.sumsec = sumsec
        self.vz.summin = summin
        self.vz.calls = calls

    def setmg(self, sumraw, sumcust, sumoper, sumsec, summin, calls):
        self.mg.serv = 'mg'
        self.mg.sumraw = sumraw
        self.mg.sumcust = sumcust
        self.mg.sumoper = sumoper
        self.mg.sumsec = sumsec
        self.mg.summin = summin
        self.mg.calls = calls

    def setmn(self, sumraw, sumcust, sumoper, sumsec, summin, calls):
        self.mn.serv = 'mn'
        self.mn.sumraw = sumraw
        self.mn.sumcust = sumcust
        self.mn.sumoper = sumoper
        self.mn.sumsec = sumsec
        self.mn.summin = summin
        self.mn.calls = calls


"""
sd = AtomSumDir(999, 'u')
sd.setvz(1, 2, 3, 4, 5)
sd.setmg(10, 11, 12, 13, 14)
sd.setmn(100, 111, 112, 113, 114)
a=list()
a.append(sd)

h = {}
h['9'] = sd
h['9'].vz.sumraw += 1000
b = h['9']

print h['9'].uf, h['9'].cid, h['9'].vz.sumraw, h['9'].mn.sumraw
print b.uf, b.cid, b.vz.sumraw, b.mn.sumraw
print a[0].uf, a[0].cid, a[0].vz.sumraw, a[0].mn.sumraw
"""


class FirstStep(object):
    """
    Документы для оператора - первый шаг
    Из таблицы периода YyyyyMmm формируются объекты (таблицы и структуры) с итоговыми данными
    таблицы: rss и inf
    """

    def __init__(self, opts, first, firstks):
        self.dsn = cfg.dsn_bill2
        self.db = MySQLdb.Connect(**self.dsn)
        self.cur = self.db.cursor()
        self.opts = opts
        self.first = first       # словарь AtomSumDir - итоговые суммы по направлениям (MG MN VZ) для всех клиентов
        self.firstks = firstks   # словарь AtomSumDir - итоговые суммы по направлениям (MG MN VZ) для квартир. сектора

    def prepare_data(self, oper, file_sql, delete_period=False):
        """
        Создание (наполнение) первичной таблицы данными ( таблица `rss` or `inf` )
        :param oper: 'q' or 'm'  (q => rss, m => rsi)
        :param file_sql: file with sql-command for create table
        :param delete_period: if True - delete all data in period
        :return:
        """
        opts = self.opts
        table_data = opts.table               # Y2015M11
        cursor = self.cur
        table = cfg.operator[oper]['tab1']    # rss or inf

        # Если таблица не существует, то создаём её
        prepare_table(dsn=self.dsn, file_sql=file_sql, table=table, year=opts.year, month=opts.month,
                      delete_period=delete_period)

        # Выборка необходимых записей из YyyyyMyy и сохранение результата в rss или inf
        sql = cfg.sqls['tab1'].format(tableYYYYMM=table_data, minsec=cfg.calc['minsec'], operator=oper)
        cursor.execute(sql)
        step = 0
        for line in cursor:
            uf, cid, pid, stat, dir, sumraw, sumcust, sumoper, sumsec, summin, calls = line
            serv = stat2serv(stat)
            kwargs = dict(
                table=table, year=opts.year, month=opts.month, cid=cid, pid=pid, uf=uf, stat=stat, serv=serv, dir=dir,
                sumraw=sumraw, sumcust=sumcust, sumoper=sumoper, sumsec=sumsec, summin=summin, calls=calls
            )
            insert_db(cursor=cursor, **kwargs)
            step += 1
        log.info('add {step} records in `{table}`'.format(step=step, table=table))

        # Услуга '800' - особый случай, для клиента бесплатно, а оператор платит за инициирование вызовов
        sql = cfg.sqls['free800'].format(tableYYYYMM=table_data, minsec=cfg.calc['minsec'], operator=oper)
        cursor.execute(sql)
        print(sql)
        step = 0
        summin = 0
        for line in cursor:
            if line[0] is None:
                break
            sumsec, summin, calls = line
            q = cfg.free800
            stat, serv, cid, dir, uf = (q['stat'], q['serv'], q['cid'], q['dir'], 'x')
            sumraw, sumcust, sumoper = (0, 0, 0)
            kwargs = dict(
                table=table, year=opts.year, month=opts.month, cid=cid, pid=0, uf=uf, stat=stat, serv=serv, dir=dir,
                sumraw=sumraw, sumcust=sumcust, sumoper=sumoper, sumsec=sumsec, summin=summin, calls=calls
            )
            insert_db(cursor=cursor, **kwargs)
            step += 1
        log.info('add {step} records in `{table}` for 800-free: {summin} min`'.format
                 (step=step, table=table, summin=summin))

    def fill_data_struct(self, oper):
        """
        Заполнение структур данными: self.first и self.firstks
        :param oper: 'q' or 'm'  (q => rss, m => inf)
        :return:
        """
        opts = self.opts
        cursor = self.cur
        table = cfg.operator[oper]['tab1']
        # юрлица
        sql = "SELECT `uf`, `cid`, `serv`, sum(`sumraw`) sumraw, sum(`sumcust`) sumcust, sum(`sumoper`) sumoper, " \
              "sum(`sumsec`) sumsec, sum(`summin`) summin , sum(`calls`) sumcalls FROM `{table}` " \
              "WHERE `year`={year} AND `month`={month} GROUP BY uf, cid, serv".\
            format(table=table, year=opts.year, month=opts.month)
        cursor.execute(sql)
        for line in cursor:
            uf, cid, serv, sumraw, sumcust, sumoper, sumsec, summin, sumcalls = line
            if cid in self.first:
                a = self.first[cid]
            else:
                a = AtomSumDir(cid, uf)
            if serv == "MG":
                a.setmg(sumraw, sumcust, sumoper, sumsec, summin, sumcalls)
            elif serv == "MN":
                a.setmn(sumraw, sumcust, sumoper, sumsec, summin, sumcalls)
            elif serv == "VZ":
                a.setvz(sumraw, sumcust, sumoper, sumsec, summin, sumcalls)

            self.first[cid] = a
        log.info("fill data struct: {records} records for oper '{oper}'(u)".format(records=len(self.first), oper=oper))

        # кв.сектор, необходим только для расчёта ФГУП РСИ, так как для РСС физлица в общей таблице клиентов (uf=f)
        # 2016-02: кв.сектор стал нужен и для РСС
        sql = "SELECT `uf`, `pid`, `serv`, sum(`sumraw`) sumraw, sum(`sumcust`) sumcust, sum(`sumoper`) sumoper, " \
              "sum(`sumsec`) sumsec, sum(`summin`) summin , sum(`calls`) sumcalls FROM `{table}` " \
              "WHERE `year`={year} AND `month`={month} AND cid=549 GROUP BY uf, pid, serv".\
            format(table=table, year=opts.year, month=opts.month)
        cursor.execute(sql)
        for line in cursor:
            uf, pid, serv, sumraw, sumcust, sumoper, sumsec, summin, sumcalls = line
            if pid in self.firstks:
                a = self.firstks[pid]
            else:
                a = AtomSumDir(pid, uf)
            if serv == "MG":
                a.setmg(sumraw, sumcust, sumoper, sumsec, summin, sumcalls)
            elif serv == "MN":
                a.setmn(sumraw, sumcust, sumoper, sumsec, summin, sumcalls)
            elif serv == "VZ":
                a.setvz(sumraw, sumcust, sumoper, sumsec, summin, sumcalls)

            self.firstks[pid] = a
        log.info("fill data struct: {records} records for oper '{oper}'(f)".
                 format(records=len(self.firstks), oper=oper))

    def print_info_customers(self):
        """ Печать данных по клиентам для отладки """
        custs = list()
        [custs.append(q.cid) for q in self.first.values()]
        custs.sort()
        for cid in custs:
            a = self.first[cid]
            for q in (a.mg, a.mn, a.vz):
                print("{cid} {uf} {serv}: {sumraw} {sumcust} {sumoper} {sumsec} {summin} (raw/cust/oper sec/min)".
                      format(cid=a.cid, uf=a.uf, serv=q.serv, sumraw=q.sumraw, sumcust=q.sumcust, sumoper=q.sumoper,
                             sumsec=q.sumsec, summin=q.summin))
        print("itogo: {items} items".format(items=len(self.first)))

    def print_info_custks(self):
        """ Печать данных по квартирному сектору для отладки """
        custs = list()
        [custs.append(q.cid) for q in self.firstks.values()]
        custs.sort()
        for cid in custs:
            a = self.firstks[cid]
            for q in (a.mg, a.mn, a.vz):
                print("{cid} {uf} {serv}: {sumraw} {sumcust} {sumoper} {sumsec} {summin} (raw/cust/oper sec/min)".
                      format(cid=a.cid, uf=a.uf, serv=q.serv, sumraw=q.sumraw, sumcust=q.sumcust, sumoper=q.sumoper,
                             sumsec=q.sumsec, summin=q.summin))
        print("itogo_ks: {items} items".format(items=len(self.firstks)))


class Book(object):
    """ Работа с данными, создание книги продаж и пр. """
    def __init__(self, opts, cust, custks, first, firstks):
        """
        Исходные данные:
        :param cust: массив данных по клиентам
        :param custks: массив данных по клиентам квартирного сектора
        :param first:   хеш - итоговые суммы по направлениям AtomSumDir
        :param firstks: хеш - итоговые суммы по направлениям AtomSumDir для квартирного сектора
        :return:
        """
        self.cust = cust
        self.custks = custks
        self.first = first
        self.firstks = firstks
        self.dsn = cfg.dsn_bill2
        self.db = MySQLdb.Connect(**self.dsn)
        self.cur = self.db.cursor()
        self.opts = opts

    def __del__(self):
        self.cur.close()
        self.db.close()

    def create_book_ks(self, oper, tab_bookf, tab_servf):

        """
        Дополнение книги извещений для КВАРТИРНОГО СЕКТОРА (cid=549)
        :param oper: оператор q или m (q=>rss, m=>rsi)
        :param tab_bookf: книга извещений для физлиц (rsi-bookf)
        :param tab_servf: расшифровка сервисов МГ/ВЗ для книги извещений физлиц (rsi-servf)
        :return:
        """
        # 2015-11-24
        # квартирный сектор (cid=549) сейчас учитывается только для inf, а для rss физлица+юрлица в одной таблице
        # 2016-02-01 кв.сектор (cid=549) перешёл в РСС (новые 626-е номера)
        data = custom = None

        # 2016-02-01:  VVV vvv
        if oper == 'q':
            sum, nds, vsego = self.create_book_ks_rss(oper, tab_bookf, tab_servf)
            return sum, nds, vsego

        if oper == 'q': data = self.first; custom = self.cust
        elif oper == 'm': data = self.firstks; custom = self.custks

        cursor = self.cur
        opts = self.opts

        custs = list()
        get_custlist(data=data, uf='f', cidexclude=(cfg.free800['cid'],), custlist=custs)

        # последний номер извещения
        account_fiz = ut.get_last_account(cursor, table=tab_bookf, year=opts.year)
        if oper == 'q' and opts.year == 2015 and opts.month == 10:
            account_fiz = 1     # особый случай - извещение с номером 1 уже есть за 2015_10 для rss (q)
        elif oper == 'm' and opts.year == 2015 and opts.month == 11:
            account_fiz = 1033     # особый случай

        sum_cust_mgmn = sum_cust_vz = nds_mgmn = nds_vz = records_book = records_bookserv = 0

        period = ut.year_month2period(opts.year, opts.month, month_char='_')    # Y2015_11
        date_now = ut.sqldate(datetime.date.today())

        for cid in custs:
            a = data[cid]
            # a) итоги (МГ+ВЗ) по каждому физлицу в книге извещений для физлиц
            # для физлиц ндс уже включён в стоимость ( sumcust - сумма клиенту c НДС )
            xsum_cust_mgmn = ut.rnd(a.mg.sumcust + a.mn.sumcust)
            xsum_cust_vz = ut.rnd(a.vz.sumcust)
            xnds_mgmn = ut.nds2(xsum_cust_mgmn)                             # nds = val * 20/120
            xnds_vz = ut.nds2(xsum_cust_vz)                                 # nds = val * 20/120
            sum_fiz = ut.rnd(a.mg.sumcust + a.mn.sumcust + a.vz.sumcust)    # сумма физлицу (внутри НДС)
            calls = a.mg.calls + a.mn.calls + a.vz.calls                    # всего звонков по МГ+МН+ВЗ

            xcid = xpid = None
            if oper == 'q': xcid, xpid = (cid, 0)
            elif oper == 'm':xcid, xpid = (549, cid)
            account_fiz += 1
            kwargs = dict(
                table=tab_bookf,
                account=account_fiz, cid=xcid, pid=xpid, period=period, year=opts.year, month=opts.month, date=date_now,
                calls=calls, sum=sum_fiz, paydate='11111111', paysum=0, paydebt=sum_fiz, fa='+'
            )
            insert_db(cursor=cursor, **kwargs)
            records_book += 1

            # ---------------------------
            # b) подробно услуги (МГ ВЗ) для каждого извещения
            arr = (
                (1, 'MG', cfg.book['MG'], xsum_cust_mgmn, xnds_mgmn, xsum_cust_mgmn-xnds_mgmn),
                (2, 'VZ', cfg.book['VZ'], xsum_cust_vz, xnds_vz, xsum_cust_vz-xnds_vz),
            )
            for nn, serv, serv_id, sum, nds, vsego in arr:
                if sum == 0:
                    continue

                q = custom.get(cid)
                prim = "{name}({cid})".format(name=q.custalias, cid=cid) if q else '-'
                kwargs = dict(
                    table=tab_servf,
                    account=account_fiz, year=opts.year, month=opts.month, serv_id=serv_id, serv=serv, nn=nn, sum=sum,
                    unit='q', amount=1, nds=nds, vsego=vsego, prim=prim
                )
                insert_db(cursor=cursor, **kwargs)
                records_bookserv += 1

            # --------------------------
            # c) суммарно по всем физлицам - одна строка в книге продаж для юрлиц
            sum_cust_mgmn += xsum_cust_mgmn
            sum_cust_vz += xsum_cust_vz
            nds_mgmn += xnds_mgmn
            nds_vz += xnds_vz

        log.info("add {records}(f) records in `{tab_bookf}`".format(tab_bookf=tab_bookf, records=records_book))
        log.info("add {records}(f) records in `{tab_servf}`".format(tab_servf=tab_servf, records=records_bookserv))

        vsego = ut.rnd(sum_cust_mgmn + sum_cust_vz)
        nds = ut.rnd(nds_mgmn + nds_vz)
        sum = ut.rnd(vsego - nds)

        return sum, nds, vsego

    def create_book_ks_rss(self, oper, tab_bookf, tab_servf):

        """
        Только для РСС (случай когда есть клиенты кв_сектора(549) + физ_лица (uf=f)
        Дополнение книги извещений для КВАРТИРНОГО СЕКТОРА (cid=549)
        :param oper: оператор q или m (q=>rss, m=>rsi)
        :param tab_bookf: книга извещений для физлиц (rsi-bookf)
        :param tab_servf: расшифровка сервисов МГ/ВЗ для книги извещений физлиц (rsi-servf)
        :return:
        """
        # 2016-02-01 кв.сектор (cid=549) перешёл в РСС (новые 626-е номера)
        data = custom = None

        # if oper == 'q'  : data = self.first; custom = self.cust
        # elif oper == 'm': data = self.firstks; custom = self.custks

        cursor = self.cur
        opts = self.opts

        # последний номер извещения
        account_fiz = ut.get_last_account(cursor, table=tab_bookf, year=opts.year)

        sum_cust_mgmn = sum_cust_vz = nds_mgmn = nds_vz = records_book = records_bookserv = 0

        period = ut.year_month2period(opts.year, opts.month, month_char='_')    # Y2015_11
        date_now = ut.sqldate(datetime.date.today())

        # физ_лица (uf=f) , кроме 549
        data, custom = (self.first, self.cust)
        custs = list()
        get_custlist(data=data, uf='f', cidexclude=(cfg.free800['cid'], 549), custlist=custs)

        for cid in custs:
            a = data[cid]
            # a) итоги (МГ+ВЗ) по каждому физлицу в книге извещений для физлиц
            # для физлиц ндс уже включён в стоимость ( sumcust - сумма клиенту c НДС )
            xsum_cust_mgmn = ut.rnd(a.mg.sumcust + a.mn.sumcust)
            xsum_cust_vz = ut.rnd(a.vz.sumcust)
            xnds_mgmn = ut.nds2(xsum_cust_mgmn)                             # nds = val * 20/120
            xnds_vz = ut.nds2(xsum_cust_vz)                                 # nds = val * 20/120
            sum_fiz = ut.rnd(a.mg.sumcust + a.mn.sumcust + a.vz.sumcust)    # сумма физлицу (внутри НДС)
            calls = a.mg.calls + a.mn.calls + a.vz.calls                    # всего звонков по МГ+МН+ВЗ

            xcid = xpid = None
            # if oper == 'q': xcid, xpid = (cid, 0)
            # elif oper == 'm':xcid, xpid = (549, cid)
            xcid, xpid = (cid, 0)

            account_fiz += 1
            kwargs = dict(
                table=tab_bookf,
                account=account_fiz, cid=xcid, pid=xpid, period=period, year=opts.year, month=opts.month, date=date_now,
                calls=calls, sum=sum_fiz, paydate='11111111', paysum=0, paydebt=sum_fiz, fa='+'
            )
            insert_db(cursor=cursor, **kwargs)
            records_book += 1

            # ---------------------------
            # b) подробно услуги (МГ ВЗ) для каждого извещения
            arr = (
                (1, 'MG', cfg.book['MG'], xsum_cust_mgmn, xnds_mgmn, xsum_cust_mgmn-xnds_mgmn),
                (2, 'VZ', cfg.book['VZ'], xsum_cust_vz, xnds_vz, xsum_cust_vz-xnds_vz),
            )
            for nn, serv, serv_id, sum, nds, vsego in arr:
                if sum == 0:
                    continue

                q = custom.get(cid)
                prim = "{name}({cid})".format(name=q.custalias, cid=cid) if q else '-'
                kwargs = dict(
                    table=tab_servf,
                    account=account_fiz, year=opts.year, month=opts.month, serv_id=serv_id, serv=serv, nn=nn, sum=sum,
                    unit='q', amount=1, nds=nds, vsego=vsego, prim=prim
                )
                insert_db(cursor=cursor, **kwargs)
                records_bookserv += 1

            # --------------------------
            # c) суммарно по всем физлицам - одна строка в книге продаж для юрлиц
            sum_cust_mgmn += xsum_cust_mgmn
            sum_cust_vz += xsum_cust_vz
            nds_mgmn += xnds_mgmn
            nds_vz += xnds_vz

        # квартирный сектор (549)
        data, custom = (self.firstks, self.custks)
        custs = list()
        get_custlist(data=data, uf='f', cidexclude=(cfg.free800['cid'],), custlist=custs)

        for cid in custs:
            a = data[cid]
            # a) итоги (МГ+ВЗ) по каждому физлицу в книге извещений для физлиц
            # для физлиц ндс уже включён в стоимость ( sumcust - сумма клиенту c НДС )
            xsum_cust_mgmn = ut.rnd(a.mg.sumcust + a.mn.sumcust)
            xsum_cust_vz = ut.rnd(a.vz.sumcust)
            xnds_mgmn = ut.nds2(xsum_cust_mgmn)                             # nds = val * 20/120
            xnds_vz = ut.nds2(xsum_cust_vz)                                 # nds = val * 20/120
            sum_fiz = ut.rnd(a.mg.sumcust + a.mn.sumcust + a.vz.sumcust)    # сумма физлицу (внутри НДС)
            calls = a.mg.calls + a.mn.calls + a.vz.calls                    # всего звонков по МГ+МН+ВЗ

            xcid = xpid = None
            # if oper == 'q': xcid, xpid = (cid, 0)
            # elif oper == 'm':xcid, xpid = (549, cid)
            xcid, xpid = (549, cid)
            account_fiz += 1
            kwargs = dict(
                table=tab_bookf,
                account=account_fiz, cid=xcid, pid=xpid, period=period, year=opts.year, month=opts.month, date=date_now,
                calls=calls, sum=sum_fiz, paydate='11111111', paysum=0, paydebt=sum_fiz, fa='+'
            )
            insert_db(cursor=cursor, **kwargs)
            records_book += 1

            # ---------------------------
            # b) подробно услуги (МГ ВЗ) для каждого извещения
            arr = (
                (1, 'MG', cfg.book['MG'], xsum_cust_mgmn, xnds_mgmn, xsum_cust_mgmn-xnds_mgmn),
                (2, 'VZ', cfg.book['VZ'], xsum_cust_vz, xnds_vz, xsum_cust_vz-xnds_vz),
            )
            for nn, serv, serv_id, sum, nds, vsego in arr:
                if sum == 0:
                    continue

                q = custom.get(cid)
                prim = "{name}({cid})".format(name=q.custalias, cid=cid) if q else '-'
                kwargs = dict(
                    table=tab_servf,
                    account=account_fiz, year=opts.year, month=opts.month, serv_id=serv_id, serv=serv, nn=nn, sum=sum,
                    unit='q', amount=1, nds=nds, vsego=vsego, prim=prim
                )
                insert_db(cursor=cursor, **kwargs)
                records_bookserv += 1

            # --------------------------
            # c) суммарно по всем физлицам - одна строка в книге продаж для юрлиц
            sum_cust_mgmn += xsum_cust_mgmn
            sum_cust_vz += xsum_cust_vz
            nds_mgmn += xnds_mgmn
            nds_vz += xnds_vz


        log.info("add {records}(f) records in `{tab_bookf}`".format(tab_bookf=tab_bookf, records=records_book))
        log.info("add {records}(f) records in `{tab_servf}`".format(tab_servf=tab_servf, records=records_bookserv))

        vsego = ut.rnd(sum_cust_mgmn + sum_cust_vz)
        nds = ut.rnd(nds_mgmn + nds_vz)
        sum = ut.rnd(vsego - nds)

        return sum, nds, vsego

    def create_akt(self, oper, tab1, tab_akt):
        """
        Создание акта сдачи-приёмки, подтверждающего выполнение обязательств оператором
        :param oper:  q | m
        :param tab1: исходная таблица с итогами за месяц
        :param tab_akt: таблица, куда положим результат

        06-08-2018 Ответ Ксении Соловьёвой на вопрос как считается плата за инициирование
        "Плата за инициирование = сумма длительностей звонков в секундах / 60 - длительность звонков в сек. /60 на 8-800"
        ( Другими словами в ПИ не влючаются звонки на 8800 )

        :return:
        """
        opts = self.opts
        cursor = self.cur
        year, month = (opts.year, opts.month)
        period = ut.year_month2period(year, month, month_char='_')  # Y2016_01
        pikoff = cfg.operator[oper]['pikoff']   # сейчас 0.23

        sql = "SELECT sum(`sumraw`) `sumraw`, sum(`sumcust`) `sumcust`, sum(`sumoper`) `sumoper`, " \
              "sum(`summin`) `summin`, round(sum(`sumsec`)/60,0) `summin2` FROM `{table}` " \
              "WHERE `year`='{year}' AND `month`='{month}' ".format(table=tab1, year=year, month=month)
        sql_800 = sql + "AND `uf` = 'x'"
        sql_mg = sql + "AND uf <> 'x' AND serv IN ('MG','MN')"
        sql_vz = sql + "AND uf <> 'x' AND serv IN ('VZ')"

        #  call_include= True | False -  нужны или нет звонки по агентскому договору

        q_800 = dict(stat='800', sql=sql_800, call_include=False)
        q_mg = dict(stat='MWS', sql=sql_mg, call_include=True)
        q_vz = dict(stat='Z', sql=sql_vz, call_include=True)

        for x in (q_800, q_mg, q_vz):
            cursor.execute(x['sql'])
            sumraw, sumcust, sumoper, summin, summin2 = cursor.fetchone()
            summin3 = summin2
            if not x['call_include']:
                summin2 = 0
            us = ut.rnd(sumoper)                  # условная стоимость
            pi = ut.rnd(float(summin2) * pikoff)  # платёж за инициирование вызовов
            av = ut.rnd(sumraw - (us + pi))       # агентское вознаграждение
            nds = ut.rnd(ut.nds(av))              # ндс агентского вознаграждения
            kwargs = dict(
                table=tab_akt, year=year, month=month, period=period, stat=x['stat'], min=summin, min2=summin2,
                min3=summin3,
                sum=sumraw, us=us, pi=pi, av=av, nds=nds
            )
            insert_db(cursor=cursor, **kwargs)

    def create_book(self, oper, file_tab_book, file_tab_bookf, file_tab_serv, file_tab_akt, delete_period=False):
        """
        Создание (наполнение) книги продаж и пр. данными ( `rss-book`, `rss-bookf`, `rss-serv`, `rss-servf`, `rss-akt`,)
        :param oper:            'q' or 'm'  (q => rss-book, m => rsi-book)
        :param file_tab_book:   file with sql-command for create table rss-book
        :param file_tab_bookf:  file with sql-command for create table rss-bookf
        :param file_tab_serv:   file with sql-command for create table rss-serv
        :param file_tab_akt:    file with sql-command for create table rss-akt
        :param delete_period:   if True - delete all data in period
        :return:
        """
        opts = self.opts
        cursor = self.cur
        tab1 = cfg.operator[oper]['tab1']           # rss - исходная таблица
        tab_book = cfg.operator[oper]['book']       # rss_book - книга продаж (юл)
        tab_bookf = cfg.operator[oper]['bookf']     # rss_bookf - книга извещений (фл)
        tab_serv = cfg.operator[oper]['serv']       # rss_serv - подробно по напр. для rss-book
        tab_servf = cfg.operator[oper]['servf']     # rss_servf - физлица подробно по напр. для rss-bookf
        tab_akt = cfg.operator[oper]['akt']         # rss_akt - акт сдачи-приёмки

        # Если таблицы не существует, то создаём её
        prepare_table(dsn=self.dsn, file_sql=file_tab_book, table=tab_book, year=opts.year, month=opts.month,
                      delete_period=delete_period)
        prepare_table(dsn=self.dsn, file_sql=file_tab_bookf, table=tab_bookf, year=opts.year, month=opts.month,
                      delete_period=delete_period)
        prepare_table(dsn=self.dsn, file_sql=file_tab_serv, table=tab_serv, year=opts.year, month=opts.month,
                      delete_period=delete_period)
        prepare_table(dsn=self.dsn, file_sql=file_tab_serv, table=tab_servf, year=opts.year, month=opts.month,
                      delete_period=delete_period)
        prepare_table(dsn=self.dsn, file_sql=file_tab_akt, table=tab_akt, year=opts.year, month=opts.month,
                      delete_period=delete_period)

        # сбор данных
        period1, period2 = ut.dateperiod(opts.year, opts.month)[1:]
        period1 = ut.sqldate(period1)     # 20151101
        period2 = ut.sqldate(period2)     # 20151130
        date = datesf = period2           # 20151130
        datept = dateopl = ut.sqldate(datetime.date(9999, 11, 11))   # 99991111
        sumopl, sumbook, fperiod, fbreak = (0, 0, True, False)
        # kat_id, fx, rash_id, title_id = ('TLF', '*', 60, 19)      # 60 с 01-10-2017 (RSS)
        kat_id, fx, rash_id, title_id = ('TLF', '*', 61, 19)        # 61 с 01-01-2021 (A2)

        unit, amount = ('q', 1)

        account = ut.get_last_account(cursor, table=tab_book, year=opts.year)  # последний номер счёта

        if oper == 'q':
            if opts.year == 2015 and opts.month == 10:
                account = 152   # особый случай, для 2015_10 последний счёт должен быть 152
        elif oper == 'm':
            if opts.year == 2015 and opts.month == 11:
                account = 381   # особый случай

        # юридические лица
        # a) итоги (МГ+ВЗ) по каждому юрлицу - одна строка (один счёт) в книге продаж для юрлиц (tab_book)
        # b) подробно услуги (отдельно МГ и ВЗ) для каждого счёта в расшифровке услуг юрлиц (tab_serv)
        custs = list()
        get_custlist(data=self.first, uf='u', cidexclude=(cfg.free800['cid'],), custlist=custs)

        records_book = records_bookserv = 0
        for cid in custs:

            a = self.first[cid]
            sum_mgmn = ut.rnd(a.mg.sumraw + a.mn.sumraw)
            sum_vz = ut.rnd(a.vz.sumraw)
            nds_mgmn = ut.nds(sum_mgmn)
            nds_vz = ut.nds(sum_vz)
            sum = ut.rnd(sum_mgmn + sum_vz)
            nds = ut.rnd(nds_mgmn + nds_vz)
            dolg = vsego = ut.rnd(sum + nds)

            account += 1
            sf, pt = (account, 0)
            q = self.cust.get(cid)
            uf = q.uf
            prim = "{name}({cid}){uf}".format(name=q.custalias, uf=uf, cid=cid) if q else '-'

            # a) книга продаж - итоги для клиента - одна строка на все услуги - один счёт
            kwargs = dict(
                table=tab_book,
                account=account, year=opts.year, month=opts.month, kat_id=kat_id, fx=fx, date=date, rash_id=rash_id,
                title_id=title_id, cid=cid, sf=sf, datesf=datesf, pt=pt, datept=datept, period1=period1,
                period2=period2, fperiod=boolsql(fperiod), sum=sum, nds=nds, vsego=vsego, dateopl=dateopl,
                sumopl=sumopl, dolg=dolg, sumbook=sumbook, prim=prim, fbreak=boolsql(fbreak), uf=uf
            )
            insert_db(cursor=cursor, **kwargs)
            records_book += 1

            # b) подробно услуги (МГ ВЗ) для каждого счёта
            arr = (
                (1, 'MG', cfg.book['MG'], sum_mgmn, nds_mgmn, sum_mgmn+nds_mgmn),
                (2, 'VZ', cfg.book['VZ'], sum_vz, nds_vz, sum_vz+nds_vz),
            )
            for nn, serv, serv_id, sum, nds, vsego in arr:
                if sum == 0:
                    continue

                kwargs = dict(
                    table=tab_serv,
                    account=account, year=opts.year, month=opts.month, serv_id=serv_id, serv=serv, nn=nn, sum=sum,
                    unit=unit, amount=amount, nds=nds, vsego=vsego, prim=prim
                )
                insert_db(cursor=cursor, **kwargs)
                records_bookserv += 1
        log.info("add {records}(u) records in `{tab_book}`".format(tab_book=tab_book, records=records_book))
        log.info("add {records}(u) records in `{tab_serv}`".format(tab_serv=tab_serv, records=records_bookserv))

        # физические лица:
        # a) итоги (МГ+ВЗ) по каждому физлицу в книге извещений tab_bookf
        # b) суммарно по всем физлицам - одна строка в книге продаж для юрлиц tab_book с номером счёта=0
        # ps. для oper=m физлица-это клиенты квартирного сектора и не входят в общую таблицу клиентов
        prim = "Физические лица"
        if oper == 'm':
            # a) итоги (МГ+ВЗ) по каждому физлицу в книге извещений tab_bookf
            sum, nds, vsego = self.create_book_ks(oper=oper, tab_bookf=tab_bookf, tab_servf=tab_servf)
            # b) суммарно по всем физлицам - одна строка в книге продаж для юрлиц tab_book с номером счёта=0
            kwargs = dict(
                table=tab_book,
                account=0, year=opts.year, month=opts.month, kat_id=kat_id, fx=fx, date=date, rash_id=rash_id,
                title_id=title_id, cid=549, sf=0, datesf=datesf, pt=0, datept=datept, period1=period1,
                period2=period2, fperiod=boolsql(fperiod), sum=sum, nds=nds, vsego=vsego, dateopl=dateopl,
                sumopl=sumopl, dolg=vsego, sumbook=sumbook, prim=prim, fbreak=boolsql(fbreak), uf='f'
            )
            insert_db(cursor=cursor, **kwargs)
            log.info("add {records}(f) records in `{tab_book}`".format(tab_book=tab_book, records=1))

        elif oper == 'q':
            # a) итоги (МГ+ВЗ) по каждому физлицу в книге извещений tab_bookf
            sum, nds, vsego = self.create_book_ks(oper=oper, tab_bookf=tab_bookf, tab_servf=tab_servf)
            # b) суммарно по всем физлицам - одна строка в книге продаж для юрлиц tab_book с номером счёта=0
            kwargs = dict(
                table=tab_book,
                account=0, year=opts.year, month=opts.month, kat_id=kat_id, fx=fx, date=date, rash_id=rash_id,
                title_id=title_id, cid=549, sf=0, datesf=datesf, pt=0, datept=datept, period1=period1,
                period2=period2, fperiod=boolsql(fperiod), sum=sum, nds=nds, vsego=vsego, dateopl=dateopl,
                sumopl=sumopl, dolg=vsego, sumbook=sumbook, prim=prim, fbreak=boolsql(fbreak), uf='f'
            )
            insert_db(cursor=cursor, **kwargs)
            log.info("add {records}(f) records in `{tab_book}`".format(tab_book=tab_book, records=1))

        self.create_akt(oper=oper, tab1=tab1, tab_akt=tab_akt)


class DiffSum(object):
    """
    Объект - разница в сумме по направлению
    """
    def __init__(self, sum=0, nds=0, vsego=0, serv='MG', dir='Россия моб.'):
        self.sum = sum
        self.nds = nds
        self.vsego = vsego
        self.serv = serv            # MG, MN, VZ
        #self.akt_stat = akt_stat    # Z, MWS, 800
        self.dir = dir              # направление, где будем изменять копейки (или Организация для детал_уступки_треб)


class OperatorData(object):
    """
    Создаются файлы для выгрузки оператору
    """
    def __init__(self, opts, oper, reports, files, cust, custks):
        self.opts = opts
        self.oper = oper
        self.reports = reports
        self.files = files
        self.cust = cust
        self.custks = custks
        self.dsn = cfg.dsn_bill2
        self.db = MySQLdb.Connect(**self.dsn)
        self.cur = self.db.cursor()
        self.opts = opts

    def __del__(self):
        self.cur.close()
        self.db.close()


class OperatorDataRss(OperatorData):
    """
        договор Речсвязьсервис и МТС на МГМНВЗ связь
        создание текстовых файлов для выгрузки на портал оператору МТС
    """

    def create_book(self, filename, result):
        """
        создание книги продаж для оператора в виде файла
        :param filename: имя файла для записи
        :param result: для возврата как: result.sum, result.nds, result.vsego
        :return: кортеж итогов (sum, nds, vsego)
        """
        # год; месяц; дата счёта; номер счёта; абонент; инн; кпп; дата оплаты; всего; стоим без ндс18; ндс18;
        # стоим без ндс10; ндс10; стоим с ндс0; освобождённые
        opts = self.opts
        cursor = self.cur
        tab_book = cfg.operator[oper]['book']       # rss-book - книга продаж (юл)
        tab_bookf = cfg.operator[oper]['bookf']     # rss-bookf - книга извещений (фл)
        tab_serv = cfg.operator[oper]['serv']       # rss-serv - подробно по напр. для rss-book
        tab_servf = cfg.operator[oper]['servf']     # rss-servf - физлица подробно по напр. для rss-bookf

        sum_sum, sum_nds, sum_vsego = (0, 0, 0)
        f = open(filename, "wt", encoding='utf8')
        sql = "SELECT `account`, `date`, `cid`, `uf`, `sum`, `nds`, `vsego` FROM `{table}` WHERE `year`='{year}' " \
              "AND `month`='{month}' ORDER BY `id`".format(table=tab_book, year=opts.year, month=opts.month)
        cursor.execute(sql)
        for line in cursor:
            account, date, cid, uf, sum, nds, vsego = line
            sum_sum += sum; sum_nds += nds; sum_vsego += vsego
            vsego, sum, nds = (ut.dec(vsego), ut.dec(sum), ut.dec(nds))
            if uf == 'u':
                q = cust.get_cust(cid)
                abonent, inn, kpp = (ut.double_q(q.custname), ut.inn(q.inn), ut.kpp(q.kpp))
            else:
                abonent, inn, kpp = ('"Физические лица"', '000000000000', '000000000')

            st = "{year};{month};{date};{prefix}-{account};{abonent};{inn};{kpp};{dataopl};" \
                 "{vsego};{sum20};{nds20};{sum18};{nds18};{sum10};{nds10};{sum0};{free}".\
                format(year=opts.year, month=opts.month, date=ut.formatdate(date, '.'), account=account,
                       abonent=abonent, inn=inn, kpp=kpp, dataopl='', vsego=vsego,
                       sum20=sum, nds20=nds,
                       sum18='', nds18='',
                       sum10='', nds10='', sum0='', free='', prefix=cfg.operator[oper]['prefix'])
            f.write(st + '\n')

        f.close()
        result.sum, result.nds, result.vsego = (ut.rnd(sum_sum), ut.rnd(sum_nds), ut.rnd(sum_vsego))
        return result.sum, result.nds, result.vsego

    def check_dogdate(self, cid, dog_date):
        """
        Проверка даты договора
        :param cid: код длиента
        :param dog_date: дата договора (не должна == 11.11.1111)
        :return:
        """
        if dog_date == '11.11.1111' or dog_date == None:
            log.warning("cust: {cid} have wrong dog date".format(cid=cid))

    def create_detal(self, filename, result):
        """
        РСС/МТС: создание файла "детализации услуг прав требования" для оператора (договор МТС и РСС)
        :param filename: имя файла для записи
        :param result: для возврата как: result.sum, result.nds, result.vsego
        :return: кортеж итогов (sum, nds, vsego)
        """
        # год; месяц; абонент; номер договора; дата договора;сумма с ндс;
        opts = self.opts
        cursor = self.cur
        tab_book = cfg.operator[oper]['book']       # rss-book - книга продаж (юл)
        tab_bookf = cfg.operator[oper]['bookf']     # rss-bookf - книга извещений (фл)
        tab_serv = cfg.operator[oper]['serv']       # rss-serv - подробно по напр. для rss-book
        tab_servf = cfg.operator[oper]['servf']     # rss-servf - физлица подробно по напр. для rss-bookf

        sum_sum, sum_nds, sum_vsego = (0, 0, 0)
        f = open(filename, "wt", encoding='utf8')
        # юрлица
        sql = "SELECT `account`, `date`, `cid`, `uf`, `sum`, `nds`, `vsego` FROM `{table}` WHERE `year`='{year}' " \
              "AND `month`='{month}' AND `uf`='u' ORDER BY `id`".format(table=tab_book, year=opts.year, month=opts.month
        )
        cursor.execute(sql)
        for line in cursor:
            account, date, cid, uf, sum, nds, vsego = line
            sum_sum += sum; sum_nds += nds; sum_vsego += vsego
            vsego, sum, nds = (ut.dec(vsego), ut.dec(sum), ut.dec(nds))
            q = self.cust.get_cust(cid)
            abonent, dog_number, dog_date = (ut.double_q(q.custname), q.dog_rss, ut.formatdate(q.dog_date_rss, '.'))
            st = "{year};{month};{abonent};{dog_number};{dog_date};{vsego}".format(
                year=opts.year, month=opts.month, abonent=abonent, dog_number=dog_number, dog_date=dog_date, vsego=vsego
            )
            f.write(st + '\n')
            self.check_dogdate(cid, dog_date)

        # физлица делятся на физлица (в таблице клиентов uf=f ) и на кв_сектор
        # физлица (cid<>549)
        sql = "SELECT `cid`, `pid`, `sum`, `account` FROM `{table}` WHERE `year`='{year}' AND `month`='{month}' " \
              "and cid <>549 ORDER BY `id`".format(table=tab_bookf, year=opts.year, month=opts.month)
        alist = list()  # этот список счетов исключим из второго запроса(ниже)
        cursor.execute(sql)
        for line in cursor:
            cid, pid, sum, account = line
            alist.append(str(account))
            sum_vsego += ut.rnd(sum); nds = ut.nds2(sum); sum_nds += ut.rnd(nds); sum_sum += ut.rnd(sum-nds)
            q = self.cust.get_cust(cid)
            abonent, dog_number, dog_date = (ut.double_q(q.custname), q.dog_rss, ut.formatdate(q.dog_date_rss, '.'))
            st = "{year};{month};{abonent};{dog_number};{dog_date};{vsego}".format(
                year=opts.year, month=opts.month, abonent=abonent, dog_number=dog_number, dog_date=dog_date, vsego=sum)
            f.write(st + '\n')
            self.check_dogdate(cid, dog_date)

        accounts = ','.join(alist)
        # -----
        # кв_сектор(549): нет договоров с МТС, поэтому по регламенту МТС все такие физ-лица одной строкой
        sql = "SELECT sum(`sum`) `sum`, sum(`nds`) `nds`, sum(`vsego`) `vsego` FROM `{tab_servf}` " \
              "WHERE `year`='{year}' AND `month`='{month}' ".\
            format(tab_servf=tab_servf, year=opts.year, month=opts.month)

        if len(accounts) > 0:
            sql += " AND `account` NOT IN ({accounts})".format(accounts=accounts)

        cursor.execute(sql)
        sum_f, nds_f, vsego_f = cursor.fetchone()
        if sum_f:
            sum_vsego += ut.rnd(sum_f); sum_nds += ut.rnd(nds_f); sum_sum += ut.rnd(vsego_f)
            abonent, dog_number, dog_date = ('"Физические лица"', '---', '01.01.1900')
            st = "{year};{month};{abonent};{dog_number};{dog_date};{vsego}".format(year=opts.year,
                month=opts.month, abonent=abonent, dog_number=dog_number, dog_date=dog_date, vsego=sum_f)
            f.write(st + '\n')
        # -----

        f.close()
        result.sum, result.nds, result.vsego = (ut.rnd(sum_sum), ut.rnd(sum_nds), ut.rnd(sum_vsego))
        return result.sum, result.nds, result.vsego

    def create_amount(self, filename, result, diff=None):
        """
        Cоздание файла "объём оказанных услуг по направлениям" для оператора
        :param filename: имя файла для записи
        :param result: для возврата как: result.sum, result.nds, result.vsego
        :param diff: None или объект DiffSum - то есть надо учесть разницу в сумме с book
        :return: кортеж итогов (sum, nds, vsego)
        """
        # год; месяц; вид услуги; направление; код направления; минуты; стоимость без ндс; ндс; лицевой счёт МТС
        opts = self.opts
        cursor = self.cur
        tab1 = cfg.operator[oper]['tab1']       # rss - исходная итоговая таблица
        cds = Codemts(dsn=cfg.dsn_tar, table='komstarCode')     # cds['Австрия'] => 43

        filename2 = '_2.'.join(filename.split('.'))  # файл без НДС (всего 8 полей) - в регламенте формат2
        f = open(filename, "wt", encoding='utf8')
        f2 = open(filename2, "wt", encoding='utf8')

        sql = "SELECT `serv`, `dir`, sum(`sumraw`) `sumraw`, sum(`summin`) `summin` FROM `{table}` " \
              "WHERE `year`='{year}' AND `month`='{month}' GROUP BY `serv`, `dir`".format(
            table=tab1, year=opts.year, month=opts.month
        )
        cursor.execute(sql)
        sum_sum, sum_nds, sum_vsego = (0, 0, 0)
        for line in cursor:
            serv, dir, sumraw, summin = line
            nds = ut.nds(sumraw)

            if diff and (abs(diff.sum) + abs(diff.nds)) != 0:
                if dir == diff.dir:
                    log.info("+ amount: {dir} sum: {sumraw} + {dsum} = {sss}".format(
                        dir=dir, sumraw=sumraw, dsum=diff.sum, sss=sumraw + diff.sum))
                    log.info("+ amount: {dir} nds: {nds} + {dnds} = {sss}".format(
                        dir=dir, nds=nds, dnds=diff.nds, sss=nds + diff.nds))
                    sumraw += diff.sum
                    nds += diff.nds

            sum_sum += sumraw; sum_nds += nds; sum_vsego += sumraw+nds
            sumraw, nds, serv = (ut.dec(sumraw), ut.dec(nds), cfg.servrus[serv])
            code = cds.name2code(dir)
            st = "{year};{month};{serv};{dir};{code};{min};{sum};{nds};{account}".format(
                year=opts.year, month=opts.month, serv=serv, dir=dir, code=code, min=summin, sum=sumraw, nds=nds,
                account=cfg.operator[oper]['account']
            )
            f.write(st + '\n')

            st = "{year};{month};{serv};{dir};{code};{min};{sum};{account}".format(
                year=opts.year, month=opts.month, serv=serv, dir=dir, code=code, min=summin, sum=sumraw,
                account=cfg.operator[oper]['account']
            )
            f2.write(st + '\n')

        f.close()
        f2.close()
        result.sum, result.nds, result.vsego = (ut.rnd(sum_sum), ut.rnd(sum_nds), ut.rnd(sum_vsego))
        return result.sum, result.nds, result.vsego

    def create_total(self, filename, result, diff=None):
        """
        Создание файла "отчёт оператора"
        :param filename: имя файла для записи
        :param result: для возврата как: result.sum, result.nds, result.vsego
        :param diff: None или объект DiffSum - то есть надо учесть разницу в сумме с book
        :return: кортеж итогов (sum, nds, vsego)
        """
        # в файле содержатся итоговые данные, которые затем вводятся вручную в форму портала
        # в файле содержатся 3 строчки - по трем видам услуг ВЗ, МГ, МН
        # ВЗ: sum: xxxx,xx nds: xxxx,xx vsego: xxxx,xx
        # МГ: sum: xxxx,xx nds: xxxx,xx vsego: xxxx,xx
        # МН: sum: xxxx,xx nds: xxxx,xx vsego: xxxx,xx

        opts = self.opts
        cursor = self.cur
        tab1 = cfg.operator[oper]['tab1']       # rss- исходная итоговая таблица

        f = open(filename, "wt", encoding='utf8')
        out = dict()
        sum_sum, sum_nds, sum_vsego, sum_min = (0, 0, 0, 0)
        sql = "SELECT `serv`, sum(`sumraw`) `sumraw`, sum(`summin`) `summin` FROM `{table}` " \
              "WHERE `year`='{year}' AND `month`='{month}' GROUP BY `serv`".format(
                table=tab1, year=opts.year, month=opts.month)
        cursor.execute(sql)
        for line in cursor:
            serv, sumraw, summin = line
            nds = ut.nds(sumraw)

            if diff and (abs(diff.sum) + abs(diff.nds)) != 0:
                if serv == diff.serv:
                    log.info("+ total: {serv} sum: {sumraw} + {dsum} = {sss}".format(
                        serv=serv, sumraw=sumraw, dsum=diff.sum, sss=sumraw + diff.sum))
                    log.info("+ total: {serv} nds: {nds} + {dnds} = {sss}".format(
                        serv=serv, nds=nds, dnds=diff.nds, sss=nds + diff.nds))
                    sumraw += diff.sum
                    nds += diff.nds

            sum_sum += sumraw; sum_nds += nds; sum_vsego += sumraw+nds; sum_min += summin
            sumraw, nds, servrus, vsego = (ut.dec(sumraw), ut.dec(nds), cfg.servrus[serv], ut.dec(sumraw+nds))
            st = "{servrus}: sum: {sum} \tnds:{nds} \tvsego: {vsego}  \tmin:{min}".format(
                servrus=servrus, min=summin, sum=sumraw, nds=nds, vsego=vsego
            )
            out[serv] = st
        for srv in ('VZ', 'MG', 'MN'):
            if srv in out:
                f.write(out[srv] + '\n')

        st = "sumsum: {sumsum} \tsumnds:{sumnds} \tsumvsego: {sumvsego}  \tsummin:{summin}".format(
            sumsum=ut.dec(sum_sum), sumnds=ut.dec(sum_nds), sumvsego=ut.dec(sum_vsego), summin=sum_min
        )

        f.write('-----------------------------------------------------------------\n')
        f.write(st + '\n')
        f.close()
        result.sum, result.nds, result.vsego = (ut.rnd(sum_sum), ut.rnd(sum_nds), ut.rnd(sum_vsego))
        return result.sum, result.nds, result.vsego

    def get_akt(self, oper, result):
        """
        Итоговая сумма из акта (rss-akt)
        :param oper: оператор  q или m
        :param result: для возврата как: result.sum, result.nds, result.vsego
        :return: кортеж итогов (sum, nds, vsego)
        """
        opts = self.opts
        cursor = self.cur
        year, month = (opts.year, opts.month)
        tab_akt = cfg.operator[oper]['akt']       # rss_akt
        sql = "SELECT sum(`sum`) FROM `{table}` WHERE `year`='{year}' AND `month`='{month}'".format(
            table=tab_akt, year=year, month=month)
        cursor.execute(sql)
        sum = cursor.fetchone()[0]
        result.sum, result.nds, result.vsego = (ut.rnd(sum), 0, 0)
        return result.sum, result.nds, result.vsego

    def update_akt(self, oper, stat, dsum):
        """
        Обновление строки в акте на несколько копеек {dsum} для направления (stat)
        :param oper: q или m (q=rss, m-rsi)
        :param stat: Z, MWS, 800
        :param dsum: несколько копеек
        :return:
        """
        opts = self.opts
        cursor = self.cur
        year, month = (opts.year, opts.month)
        tab_akt = cfg.operator[oper]['akt']       # rss_akt

        sql = "SELECT `sum`, `us`, `pi` FROM `{table}` WHERE `year`='{year}' AND `month`='{month}' AND `stat`='{stat}'".\
            format(table=tab_akt, year=year, month=month, stat=stat)
        cursor.execute(sql)
        sum, us, pi = cursor.fetchone()

        nsum = ut.rnd(sum + dsum)       # новая клиентская сумма
        av = ut.rnd(nsum - (us + pi))   # агентское вознаграждение
        nds = ut.rnd(ut.nds(av))        # ндс агентского вознаграждения

        sql = "UPDATE `{table}` SET `sum`={nsum}, `av`='{av}', `nds`='{nds}' WHERE `year`='{year}' " \
              "AND `month`='{month}' AND `stat`='{stat}'".format(
                table=tab_akt, year=year, month=month, stat=stat, nsum=nsum, av=av, nds=nds)
        cursor.execute(sql)

        log.info("+ akt update `{table}` set `sum`: {sum}+{dsum}={nsum}, av={av}, nds={nds} where `stat`='{stat}'".
                 format(table=tab_akt, sum=sum, dsum=dsum, nsum=nsum, stat=stat, av=av, nds=nds))

    def create_files_rss(self):
        """ создание файлов для выгрузки оператору для РСС  """
        # for srv in self.reports['services']:
        #    for rep in self.reports['reports']:
        #        print "{srv}.{rep} => {file}".format(srv=srv, rep=rep, file=self.files[srv][rep])

        reports = ('book', 'detal', 'amount', 'total', 'akt')
        x = dict()
        for k in reports:
            x[k] = ReportSumma()

        # для rss общий договор на МГ/ВЗ и нужны 4 файла по ключам: 'book', 'detal', 'amount', 'total'

        self.create_book(self.files['ALL']['book'], x['book'])
        self.create_detal(self.files['ALL']['detal'], x['detal'])
        self.create_amount(self.files['ALL']['amount'], x['amount'])
        self.create_total(self.files['ALL']['total'], x['total'])
        self.get_akt(self.oper, x['akt'])

        f = open(self.files['ALL']['itog'], 'wt')
        for k in reports:
            st = "[{k:8s}] {out}".format(k=k, out=x[k].out())
            print(st)
            f.write(st + '\n')
        f.close()

        # ликвидация разницы копеек в book и amount
        d = DiffSum()
        d.sum = ut.rnd(x['book'].sum - x['amount'].sum)
        d.nds = ut.rnd(x['book'].nds - x['amount'].nds)
        d.vsego = ut.rnd(x['book'].vsego - x['amount'].vsego)
        d.dir = 'Россия моб.'
        d.serv = 'MG'

        if abs(d.sum)+abs(d.nds) != 0:
            self.create_amount(self.files['ALL']['amount'], x['amount'], diff=d)

        # ликвидация разницы копеек в book и total
        d = DiffSum()
        d.sum = ut.rnd(x['book'].sum - x['total'].sum)
        d.nds = ut.rnd(x['book'].nds - x['total'].nds)
        d.vsego = ut.rnd(x['book'].vsego - x['total'].vsego)
        d.dir = 'Россия моб.'
        d.serv = 'MG'

        if abs(d.sum)+abs(d.nds) != 0:
            self.create_total(self.files['ALL']['total'], x['total'], diff=d)

        # ликвидация разницы копеек в book и akt
        dsum = ut.rnd(x['book'].sum - x['akt'].sum)
        if abs(dsum)+abs(d.nds) != 0:
            self.update_akt(self.oper, stat='MWS', dsum=dsum)
            self.get_akt(self.oper, x['akt'])

        print("--")
        f = open(self.files['ALL']['itog'], 'at')
        f.write("--\n")
        for k in reports:
            st = "[{k:8s}] {out}".format(k=k, out=x[k].out())
            print(st)
            f.write(st + '\n')
        f.close()

    def create_files(self):
        """ создание файлов для оператора  """

        if self.oper == 'q':
            self.create_files_rss()
        elif self.oper == 'm':
            pass


class OperatorDataInf(OperatorData):
    """
    (РСИ) создание файлов для загрузки на портал МТС
    2 договора: на ВЗ и МГМН
    """

    def create_book(self, serv, filename, result):
        """
        (РСИ) создание книги продаж для оператора в виде файла для сервиса serv
        :param serv: код сервиса: VZ , MG
        :param filename: имя выходного файла
        :param result: для возврата как: result.sum, result.nds, result.vsego
        :return: кортеж итогов (sum, nds, vsego)
        """
        # год; месяц; дата счёта; номер счёта; абонент; инн; кпп; дата оплаты; всего; стоим без ндс18; ндс18;
        # стоим без ндс10; ндс10; стоим с ндс0; освобождённые
        oper = self.oper
        opts = self.opts
        cursor = self.cur
        year, month = (opts.year, opts.month)
        tab_book = cfg.operator[oper]['book']       # inf_book - книга продаж (юл)
        tab_bookf = cfg.operator[oper]['bookf']     # inf_bookf - книга извещений (фл)
        tab_serv = cfg.operator[oper]['serv']       # inf_serv - подробно по напр. для rss-book
        tab_servf = cfg.operator[oper]['servf']     # inf_servf - физлица подробно по напр. для rss-bookf

        sum_sum, sum_nds, sum_vsego = (0, 0, 0)
        f = open(filename, "wt", encoding='utf8')

        # юрлица
        sql = "SELECT b.`account`, b.`date`, b.`cid`, s.`sum`, s.`nds`, s.`vsego` FROM `{tab_book}` b" \
              " JOIN `{tab_serv}` s ON b.`account`=s.`account` WHERE b.`year`='{year}' AND b.`month`='{month}' " \
              " AND s.`year`='{year}' AND s.`month`='{month}' AND b.`uf`='u' AND s.`serv`='{serv}'".\
            format(tab_book=tab_book, tab_serv=tab_serv, year=year, month=month, serv=serv)

        date = None
        cursor.execute(sql)
        for line in cursor:
            account, date, cid, sum, nds, vsego = line
            sum_sum += sum; sum_nds += nds; sum_vsego += vsego
            vsego, sum, nds = (ut.dec(vsego), ut.dec(sum), ut.dec(nds))
            q = cust.get_cust(cid)
            abonent, inn, kpp = (ut.double_q(q.custname), ut.inn(q.inn), ut.kpp(q.kpp))

            st = "{year};{month};{date};{prefix}-{account};{abonent};{inn};{kpp};{dataopl};{vsego18};{sum18};{nds18};" \
                 "{sum10};{nds10};{sum0};{free}".format(year=year, month=month, date=ut.formatdate(date, '.'),
                    account=account,abonent=abonent, inn=inn, kpp=kpp, dataopl='', vsego18=vsego, sum18=sum, nds18=nds,
                    sum10='', nds10='', sum0='', free='', prefix=cfg.operator[oper]['prefix'])
            f.write(st + '\n')

        # физлица
        sql = "SELECT sum(s.`sum`) `sum`, sum(s.`nds`) `nds`, sum(s.`vsego`) `vsego` FROM `{tab_servf}` s " \
              "WHERE s.`year`='{year}' AND s.`month`='{month}' AND s.`serv`='{serv}'".\
            format(tab_servf=tab_servf, year=year, month=month, serv=serv)
        cursor.execute(sql)
        sum_f, nds_f, vsego_f = cursor.fetchone()
        if not sum_f:
            sum_f = nds_f = vsego_f = 0
        abonent, inn, kpp, account = ('"Физические лица"', '000000000000', '000000000', '000')
        st = "{year};{month};{date};{prefix}-{account};{abonent};{inn};{kpp};{dataopl};{vsego18};{sum18};{nds18};" \
             "{sum10};{nds10};{sum0};{free}".format(year=year, month=month, date=ut.formatdate(date, '.'),
            account=account,abonent=abonent, inn=inn, kpp=kpp, dataopl='', vsego18=vsego_f, sum18=sum_f, nds18=nds_f,
            sum10='', nds10='', sum0='', free='', prefix=cfg.operator[oper]['prefix'])
        f.write(st + '\n')

        f.close()
        result.sum, result.nds, result.vsego = (ut.rnd(sum_sum+sum_f), ut.rnd(sum_nds+nds_f), ut.rnd(sum_vsego+vsego_f))
        return result.sum, result.nds, result.vsego

    def create_detal(self, serv, filename, result):
        """
        (РСИ) создание файла "детализации услуг прав требования" для оператора
        :param serv: код сервиса: VZ , MG
        :param filename: имя выходного файла
        :param result: для возврата как: result.sum, result.nds, result.vsego
        :return: кортеж итогов (sum, nds, vsego)
        """

        # год; месяц; абонент; номер договора; дата договора;сумма с ндс;
        oper = self.oper
        opts = self.opts
        cursor = self.cur
        year, month = (opts.year, opts.month)
        tab_book = cfg.operator[oper]['book']       # inf_book - книга продаж (юл)
        tab_bookf = cfg.operator[oper]['bookf']     # inf_bookf - книга извещений (фл)
        tab_serv = cfg.operator[oper]['serv']       # inf_serv - подробно по напр. для inf_book
        tab_servf = cfg.operator[oper]['servf']     # inf_servf - физлица подробно по напр. для inf_bookf

        sum_sum, sum_nds, sum_vsego = (0, 0, 0)
        f = open(filename, "wt", encoding='utf8')
        # юрлица
        sql = "SELECT b.`account`, b.`date`, b.`cid`, s.`sum`, s.`nds`, s.`vsego` FROM `{tab_book}` b" \
              " JOIN `{tab_serv}` s ON b.`account`=s.`account` WHERE b.`year`='{year}' AND b.`month`='{month}' " \
              " AND s.`year`='{year}' AND s.`month`='{month}' AND b.`uf`='u' AND s.`serv`='{serv}'".\
            format(tab_book=tab_book, tab_serv=tab_serv, year=year, month=month, serv=serv)

        cursor.execute(sql)
        for line in cursor:
            account, date, cid, sum, nds, vsego = line
            sum_sum += sum; sum_nds += nds; sum_vsego += vsego
            vsego, sum, nds = (ut.dec(vsego), ut.dec(sum), ut.dec(nds))
            q = self.cust.get_cust(cid)
            abonent, dog_number, dog_date = (ut.double_q(q.custname), q.dog_rsi, ut.formatdate(q.dog_date_rsi, '.'))
            st = "{year};{month};{abonent};{dog_number};{dog_date};{vsego}".format(
                year=year, month=month, abonent=abonent, dog_number=dog_number, dog_date=dog_date, vsego=vsego
            )
            f.write(st + '\n')

        # физлица.
        # У Речсвязьинформ нет договоров физлиц c МТС на услуги связи. Поэтому по регламенту МТС одной строкой
        # у физлиц: sum = nds+vsego (так как nds внутри суммы)
        sql = "SELECT sum(`sum`) `sum`, sum(`nds`) `nds`, sum(`vsego`) `vsego` FROM `{tab_servf}` " \
              "WHERE `year`='{year}' AND `month`='{month}' AND `serv`='{serv}'".\
            format(tab_servf=tab_servf, year=year, month=month, serv=serv)
        cursor.execute(sql)
        sum_f, nds_f, vsego_f = cursor.fetchone()
        if not sum_f:
            sum_f = nds_f = vsego_f = 0
        sum_vsego += ut.rnd(sum_f); sum_nds += ut.rnd(nds_f); sum_sum += ut.rnd(vsego_f)
        abonent, dog_number, dog_date = ('"Физические лица"', '---', '01.01.1900')
        st = "{year};{month};{abonent};{dog_number};{dog_date};{vsego}".\
            format(year=year, month=month, abonent=abonent, dog_number=dog_number, dog_date=dog_date, vsego=sum_f)
        f.write(st + '\n')
        f.close()
        result.sum, result.nds, result.vsego = (ut.rnd(sum_sum), ut.rnd(sum_nds), ut.rnd(sum_vsego))
        return result.sum, result.nds, result.vsego

    def create_amount(self, serv, filename, result, diff=None):
        """
        (РСИ) создание файла "детализации услуг прав требования" для оператора
        :param serv: код сервиса: VZ , MG
        :param filename: имя выходного файла
        :param result: для возврата как: result.sum, result.nds, result.vsego
        :param diff: None или объект DiffSum - то есть надо учесть разницу в сумме с book
        :return: кортеж итогов (sum, nds, vsego)
        """
        # год; месяц; вид услуги; направление; код направления; минуты; стоимость без ндс; ндс; лицевой счёт МТС
        oper = self.oper
        opts = self.opts
        cursor = self.cur
        year, month = (opts.year, opts.month)
        tab1 = cfg.operator[oper]['tab1']       # inf - исходная итоговая таблица
        serv2files = dict(VZ="'VZ'", MG="'MG','MN'")
        cds = Codemts(dsn=cfg.dsn_tar, table='komstarCode')     # cds['Австрия'] => 43

        f = open(filename, "wt", encoding='utf8')

        filename2 = filename + '2'  # без НДС, всего 8 полей (формат 2 по регламенту)
        f2 = open(filename2, "wt", encoding='utf8')

        sql = "SELECT `serv`, `dir`, sum(`sumraw`) `sumraw`, sum(`summin`) `summin` FROM `{table}` " \
              "WHERE `year`='{year}' AND `month`='{month}' AND `serv` IN ({serv}) GROUP BY `serv`, `dir`".\
            format(table=tab1, year=year, month=month, serv=serv2files[serv])
        cursor.execute(sql)
        sum_sum, sum_nds, sum_vsego = (0, 0, 0)
        for line in cursor:
            serv, dir, sumraw, summin = line
            nds = ut.nds(sumraw)

            if diff and (abs(diff.sum) + abs(diff.nds)) != 0:
                if dir == diff.dir:
                    log.info("+ amount: {dir} sum: {sumraw} + {dsum} = {sss}".format(
                        dir=dir, sumraw=sumraw, dsum=diff.sum, sss=sumraw + diff.sum))
                    log.info("+ amount: {dir} nds: {nds} + {dnds} = {sss}".format(
                        dir=dir, nds=nds, dnds=diff.nds, sss=nds + diff.nds))
                    sumraw += diff.sum
                    nds += diff.nds

            sum_sum += sumraw; sum_nds += nds; sum_vsego += sumraw+nds
            sumraw, nds, serv = (ut.dec(sumraw), ut.dec(nds), cfg.servrus[serv])
            code = cds.name2code(dir)
            st = "{year};{month};{serv};{dir};{code};{min};{sum};{nds};{account}".format(
                year=year, month=month, serv=serv, dir=dir, code=code, min=summin, sum=sumraw, nds=nds,
                account=cfg.operator[oper]['account']
            )
            f.write(st + '\n')

            st = "{year};{month};{serv};{dir};{code};{min};{sum};{account}".format(
                year=year, month=month, serv=serv, dir=dir, code=code, min=summin, sum=sumraw,
                account=cfg.operator[oper]['account']
            )
            f2.write(st + '\n')

        f.close()
        f2.close()
        result.sum, result.nds, result.vsego = (ut.rnd(sum_sum), ut.rnd(sum_nds), ut.rnd(sum_vsego))
        return result.sum, result.nds, result.vsego

    def create_total(self, filename, result, diff=None):
        """
        (РСИ) Создание файла "отчёт оператора"
        :param filename: имя файла для записи
        :param result: для возврата как: result.sum, result.nds, result.vsego
        :param diff: None или объект DiffSum - то есть надо учесть разницу в сумме с book
        :return: кортеж итогов (sum, nds, vsego)
        """
        # в файле содержатся итоговые данные, которые затем вводятся вручную в форму портала
        # в файле содержатся 3 строчки - по трем видам услуг ВЗ, МГ, МН
        # ВЗ: sum: xxxx,xx nds: xxxx,xx vsego: xxxx,xx
        # МГ: sum: xxxx,xx nds: xxxx,xx vsego: xxxx,xx
        # МН: sum: xxxx,xx nds: xxxx,xx vsego: xxxx,xx

        opts = self.opts
        cursor = self.cur
        tab1 = cfg.operator[oper]['tab1']       # rss- исходная итоговая таблица

        f = open(filename, "wt", encoding='utf8')
        out = dict()
        sum_sum, sum_nds, sum_vsego, sum_min = (0, 0, 0, 0)
        sql = "SELECT `serv`, sum(`sumraw`) `sumraw`, sum(`summin`) `summin` FROM `{table}` " \
              "WHERE `year`='{year}' AND `month`='{month}' GROUP BY `serv`".format(
                table=tab1, year=opts.year, month=opts.month)
        cursor.execute(sql)
        for line in cursor:
            serv, sumraw, summin = line
            nds = ut.nds(sumraw)

            if diff and (abs(diff.sum) + abs(diff.nds)) != 0:
                if serv == diff.serv:
                    log.info("+ total: {serv} sum: {sumraw} + {dsum} = {sss}".format(
                        serv=serv, sumraw=sumraw, dsum=diff.sum, sss=sumraw + diff.sum))
                    log.info("+ total: {serv} nds: {nds} + {dnds} = {sss}".format(
                        serv=serv, nds=nds, dnds=diff.nds, sss=nds + diff.nds))
                    sumraw += diff.sum
                    nds += diff.nds

            sum_sum += sumraw; sum_nds += nds; sum_vsego += sumraw+nds; sum_min += summin
            sumraw, nds, servrus, vsego = (ut.dec(sumraw), ut.dec(nds), cfg.servrus[serv], ut.dec(sumraw+nds))
            st = "{servrus}: sum: {sum} \tnds:{nds} \tvsego: {vsego}  \tmin:{min}".format(
                servrus=servrus, min=summin, sum=sumraw, nds=nds, vsego=vsego
            )
            out[serv] = st
        for srv in ('VZ', 'MG', 'MN'):
            if srv in out:
                f.write(out[srv] + '\n')

        st = "sumsum: {sumsum} \tsumnds:{sumnds} \tsumvsego: {sumvsego}  \tsummin:{summin}".format(
            sumsum=ut.dec(sum_sum), sumnds=ut.dec(sum_nds), sumvsego=ut.dec(sum_vsego), summin=sum_min
        )

        f.write('-----------------------------------------------------------------\n')
        f.write(st + '\n')
        f.close()
        result.sum, result.nds, result.vsego = (ut.rnd(sum_sum), ut.rnd(sum_nds), ut.rnd(sum_vsego))
        return result.sum, result.nds, result.vsego

    def create_files(self):
        """ РСИ: создание файлов для выгрузки оператору """
        # for srv in self.reports['services']:
        #    for rep in self.reports['reports']:
        #        print "{srv}.{rep} => {file}".format(srv=srv, rep=rep, file=self.files[srv][rep])

        serv = ('VZ', 'MG')
        reports = ('book', 'detal', 'amount', 'total', 'akt')
        x = dict()
        for s in serv:
            x[s] = dict()
            for r in reports:
                x[s][r] = ReportSumma()

        x['ALL'] = dict()
        x['ALL']['total'] = ReportSumma()

        # для inf (информ) 2 договора: на МГМН и ВЗ и нужны 3 на MG + 3 файла на VZ
        # файлы по ключам: 'book', 'detal', 'amount', 'total'

        for s in serv:
            self.create_book(serv=s, filename=self.files[s]['book'], result=x[s]['book'])

        for s in serv:
            self.create_detal(serv=s, filename=self.files[s]['detal'], result=x[s]['detal'])

        for s in serv:
            self.create_amount(serv=s, filename=self.files[s]['amount'], result=x[s]['amount'])

        self.create_total(self.files['ALL']['total'], x['ALL']['total'])


class Codemts(object):
    """
    коды МТС (лёгкая версия). только однозначное отображение имени на код
    cds = Codemts(dsn=dsn_code, table)
    code = cds.name2code('Австрия')  # 43
    """
    def __init__(self, dsn, table):
        self.dsn = dsn          # dsn -> db tariff
        self.table = table      # komstarCode
        self.codes = dict()     # codes['name'] => code, ex: codes['Австрия']=43
        self.size = 0           # количество направлений в self.codes

        db = MySQLdb.connect(**self.dsn)
        cur = db.cursor()
        sql = "select `type`,`name`, `code1`, `code2` from `komstarCode`".format(table=table)
        cur.execute(sql)
        for line in cur:
            _type, name, code1, code2 = line
            k = code1
            if code2:
                if code2.find(',') == -1 and code2.find('-') == -1:
                    k += code2
                else:
                    continue
            self.codes[name] = k

        # adds = {'г. Москва * Московская область': '79', 'Внутризоновая': '79', 'Россия моб.': '79',
        #         'Moskow-Sot': '79',  'г.Казань': '78432', 'г.Екатеринбург': '73432',
        #         'Услуга "800"': '7800', 'НАБЕРЕЖНЫЕ ЧЕЛНЫ': '7855', 'г.Новосибирск': '73832', 'г.Самара': '784622',
        #         'г.Челябинск': '73512'}
        # for name, code in adds.items():
        for name, code in cfg.name2code.items():
            self.codes[name] = code

        self.size = len(self.codes)

    def name2code(self, name):
        """
        Возвращает тел_код по название направления
        :param name: название направления, ex. Австрия
        :return: тел_код направления, ex. 43;  и если не найден, возвращает 0
        """
        return self.codes.get(name, 0)

    def prn_codes(self):
        """
        Печатает все коды: Название -> код
        :return:
        """
        names = list(self.codes.keys())
        names.sort()
        for name in names:
            print("{name} : {code}".format(name=name, code=self.name2code(name)))
        print("всего : {size} направлений".format(size=self.size))


        # sql = cfg.sqls['free800'].format(tableYYYYMM=table_data, minsec=cfg.calc['minsec'], operator=oper)
        # cursor.execute(sql)

def update_rss_bookf(dsn, period):
    """
    Обновление таблицы bill.rss_bookf перед формирование результатов за период (result)
    :param dsn: DSN
    :param period: период напр. 2021_03
    :return: количество обновлённых записей
    """
    db = MySQLdb.connect(**dsn)
    cursor = db.cursor()

    sql = "update rss_bookf b JOIN customers.CustKS ks ON b.pid=ks.pid SET b.xcid=ks.cid where b.period='Y{period}'".\
        format(period=period)

    cursor.execute(sql)

    updated_rows = cursor.rowcount

    cursor.close()
    db.close()

    return updated_rows


if __name__ == '__main__':
    p = optparse.OptionParser(description="Billing.telefon.reports - create reports for operator MTS",
                              prog="reports.py", version="0.1a", usage="reports.py --tab=table [--log=namefile]")

    p.add_option('--tab', '-t', action='store', dest='table', help='table, ex.Y2015M11')
    p.add_option('--log', '-l', action='store', dest='log', default='log/report.log', help='logfile')

    opts, args = p.parse_args()
    opts.log = flog

    opts.table = ini.table
    opts.year, opts.month = ut.period2year_month(opts.table)
    opts.file_tab1 = "{path}{file}".format(path=pathsql, file='tab_tab1.sql')
    opts.file_tab_book = "{path}{file}".format(path=pathsql, file='tab_book.sql')
    opts.file_tab_serv = "{path}{file}".format(path=pathsql, file='tab_serv.sql')
    opts.file_tab_bookf = "{path}{file}".format(path=pathsql, file='tab_bookf.sql')
    opts.file_tab_akt = "{path}{file}".format(path=pathsql, file='tab_akt.sql')

    if not opts.table or not opts.log:
        print(p.print_help())
        exit(1)

    # update rss_bookf b JOIN customers.CustKS ks ON b.pid=ks.pid SET b.xcid=ks.cid where b.period='Y2021_02';
    # rows = update_rss_bookf(dsn=cfg.dsn_bill2, period=ini.period)
    # xls = xlsreports.BillReportXls(dsn=cfg.dsn_bill2, year=ini.year, month=ini.month, path=path_results)
    # xls.create_file()
    # exit(1)

try:
    logging.basicConfig(
        filename=opts.log, level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S", format='%(asctime)s %(message)s', )

    t1 = time.time()
    log = logging.getLogger('app')

    # данные по клиентам
    cust = customers.Cust(dsn=cfg.dsn_bill2)
    custks = customers.CustKs(dsn=cfg.dsn_bill2)
    custitems = cust.customers
    custksitems = custks.customers
    # custks.print_cust()


    # 1) сбор итоговых данных в таблицы
    # for oper in ('q', 'm'):
    opers = ('q',)
    for oper in opers:
        # данные
        first = dict()  # исходные данные для формирования книг продаж
        firstks = dict()  # исходные данные для формирования книги извещений для rsi
        ob = FirstStep(opts, first=first, firstks=firstks)

        # сбор первичных данных в таблицу rss(q) или inf(m)
        ob.prepare_data(oper=oper, file_sql=opts.file_tab1, delete_period=True)

        # наполнение структур данными
        ob.fill_data_struct(oper=oper)
        # ob.print_info_customers()
        # ob.print_info_custks()

        # создание книги продаж и книги извещений: rss-book, rss-bookf, rss-serv, rss-servf
        bk = Book(opts, cust=custitems, custks=custksitems, first=first, firstks=firstks)
        bk.create_book(oper=oper, file_tab_book=opts.file_tab_book, file_tab_serv=opts.file_tab_serv,
                       file_tab_bookf=opts.file_tab_bookf, file_tab_akt=opts.file_tab_akt,  delete_period=True)

        if oper == 'q':
            log.info('-')

    # 2) создание текстовых файлов для выгрузки на портал оператора связи
    for oper in opers:
        # создание директорий для файлов на выгрузку оператору
        path_org = "{pathreports}{org}".format(pathreports=pathreports, org=cfg.operator[oper]['org'])
        path_files = "{path_org}/{year:04d}_{month:02d}".format(path_org=path_org, year=opts.year, month=opts.month)
        path_cp1251 = "{path_files}/cp1251".format(path_files=path_files)

        ut.makedir(path_org)
        ut.makedir(path_files)
        ut.makedir(path_cp1251)

        # список файлов на выгрузку: operator_files['MG']['detal'], ...
        operator_files = dict()
        ut.get_operator_files(year=opts.year, month=opts.month, path=path_files, reports=file_reports,
                              files=operator_files)

        # создание текстовых файлов для оператора
        if oper == 'q':
            tx = OperatorDataRss(opts, oper=oper, reports=file_reports, files=operator_files, cust=cust, custks=custks)
            tx.create_files()
        elif oper == 'm':
            tx = OperatorDataInf(opts, oper=oper, reports=file_reports, files=operator_files, cust=cust, custks=custks)
            tx.create_files()

        ut.copy_files_new_charset(path1=path_files, path2=path_cp1251, ext='csv', code1='utf8', code2='cp1251')

    log.info('..')

    log.info('reports in xls..')
    updated_rows = update_rss_bookf(dsn=cfg.dsn_bill2, period=ini.period)
    log.info('updated {rows} records in bill.rss_bookf'.format(rows=updated_rows))
    xls = xlsreports.BillReportXls(dsn=cfg.dsn_bill2, year=ini.year, month=ini.month, path=path_results)
    xls.create_file()
    log.info('.')


except MySQLdb.Error as e:
    log.warning(e)
    print(e)
except RuntimeError as e:
    log.warning(str(e))
    print(e)
except Exception as e:
    log.warning(str(e))
    traceback.print_exc()
