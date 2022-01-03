#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Биллинг локальной (местной связи) , она-же повремёнка
run: local.py --year=2021 --month=1    // расчёт повремёнки за январь-2021

таблицы как основа:
tarif.loc_numbers_tar - таблица с тарифами местной связи по номерам
tarif.loc_stream_tar - таблица с тарифами по номерам в потоке
bill.loc_numbers - таблица с результатами по номерам клиентов
bill.loc_stream - таблица с результатами по номерам в потоке
bill.loc_book - таблица с книгой счетов местной связи
bill.loc_book_s - таблица с книгой счетов местной связи по номерам в потоке (еще нет)
-
customers.Cust.tid_l - код тарифа расчёта местной связи -> tarrif.loc_tariff.tid
"""
import os
import sys
import re
import optparse
import traceback
import time
import MySQLdb
import logging
from datetime import datetime
from modules import cfg
from modules import utils
from modules.progressbar import Progressbar
import ini

root = os.path.realpath(os.path.dirname(sys.argv[0]))
flog = "{root}/log/{file}".format(root=root, file='local.log')
shema = "{root}/sql/local".format(root=root)


def get_path(file, prefix=shema):
    return '{prefix}/{file}'.format(prefix=prefix, file=file)


local_tables = [
    {'dsn': cfg.dsn_bill, 'table': 'loc_book', 'shema': get_path('table_loc_book.sql')},
    {'dsn': cfg.dsn_bill, 'table': 'loc_numbers', 'shema': get_path('table_loc_numbers.sql')},
    {'dsn': cfg.dsn_bill, 'table': 'loc_stream', 'shema': get_path('table_loc_stream.sql')},
    # {'dsn': cfg.dsn_tar, 'table': 'loc_numbers_tar', 'shema': get_path('table_loc_numbers_tar')},
    # {'dsn': cfg.dsn_tar, 'table': 'loc_stream_tar', 'shema': get_path('table_loc_stream_tar')},
]


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


def create_tables(tables):
    for d in tables:
        table = utils.create_table_if_no_exist(dsn=d['dsn'], table=d['table'], tab_template=d['shema'])
        if table:
            xlog("created table: '{table}'".format(table=table))


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


class StreamItem(object):
    """ Данные тарификации клиента для номеров в потоке """
    def __init__(self, cid, lines, freemin, prev, reg, period_on, period_off):
        self.cid = cid          # код cid (customers.CustID)
        self.lines = lines      # кол-во линий
        self.freemin = freemin  # бесплатных минут на 1 линию
        self.prev = prev        # плата за 1 минуту  превышения
        self.reg = reg          # шаблон(ы) через запятую на отбор номеров  из таблицы из поля fm3
        self.period_on = period_on  # период начала рассчета местных связей по потоку
        self.period_off = period_off    # период окончания рассчета местных связей по потоку


class Stream(object):
    """
    Тарифы по клиентам для номеров в потоке, элементы мапы: объекты StreamItem
    """
    def __init__(self, dsn_tar, dsn_stream, dsn_bill, table_stream_tar, table_stream, table_bill, period):
        """
        :param dsn_tar: dsn тарифов номеров в потоке
        :param dsn_stream: dsn результатов по номерам в потоке
        :param dsn_bill: dsn данных разговров
        :param table_stream_tar: таблица с тарифами
        :param table_stream: таблица для результата
        :param table_bill: таблица c данными вызовов
        :param period: период, 2021_01
        """
        self.dsn_tar = dsn_tar
        self.dsn_stream = dsn_stream
        self.dsn_bill = dsn_bill
        self.table_stream = table_stream
        self.table_stream_tar = table_stream_tar
        self.table_bill = table_bill    # Y2021M01
        self.period = period            # 2021_01
        self._cid2tar = dict()          # мапа соответствия cid->{lines, freemin, prev, ..}
        self._customers = self._prepare_tariff_and_customers(period)

    def _prepare_tariff_and_customers(self, period):
        """
        1) Создание словаря тарифов для номеров в потоке self.cid2tar
        2) Возвращает список клиентов, участвующих в вычислении по потоку
        """
        db = MySQLdb.Connect(**self.dsn_tar)
        cursor = db.cursor()

        # 1) Создание словаря тарифов
        sql = "SELECT `cid`, `lines`, `freemin`, `prev`, `reg`, `period_on`, `period_off` FROM {table}".\
            format(table=self.table_stream_tar)

        cursor.execute(sql)
        for line in cursor:
            cid, lines, freemin, prev, reg, period_on, period_off = line
            self._cid2tar[cid] = StreamItem(cid, lines, freemin, prev, reg, period_on, period_off)

        # 2) Создание списка клиентов, участвующих в вычислении по потоку по периоду
        sql = "SELECT cid FROM {table} WHERE '{period}' BETWEEN `period_on` AND `period_off` ORDER BY cid".\
            format(table=self.table_stream_tar, period=period)

        cid_list = []
        cursor.execute(sql)

        for cid in cursor:
            cid_list.append(cid[0])

        cursor.close()
        db.close()

        return cid_list

    def get_tar(self, cid):
        """
        Возвращает инфо по тарифу(словарь) для клиента cid
        :param cid: код клиента
        :return: словарь с информацией StreamItem по тарифам для клиента с кодом cid
        """
        return self._cid2tar.get(cid, {})

    def print(self):
        """
        Печать тарифов для номеров в потоке
        :return:
        """
        cids = list(self._cid2tar.keys())
        cids.sort()
        print(cids)
        for cid in cids:
            tar = self.get_tar(cid)
            rlike = self.get_sql_rlike('fm3', tar.reg)
            print("{cid}->{freemin} freemin, '{reg}' reg, rlike:{rlike}".
                  format(cid=cid, freemin=tar.freemin, reg=tar.reg, rlike=rlike))

    def get_sql_rlike(self, field, template):
        """
        Возвращает строку SQL RLIKE для вычисления номеров в потоке
        :param field: поле по которому фильтруем
        :param template: шаблон(ы) в виде списка через запятую '[0-9]{3},8120[0-9]{3}'
        :return: примерно следующее: "`fm3` RLIKE '[0-9]{3}' OR `fm3` RLIKE '8120[0-9]{3}"
        """
        sql = []
        template_list = template.split(',')
        for tpl in template_list:
            sql.append("`{field}` RLIKE '{tpl}'".format(field=field, tpl=tpl))
        return ' OR '.join(sql)

    def save_data_stream(self, period):
        """
        Сохранение данных по потоку за период period в loc_stream
        :param period: период, например, 2021_01
        :return: киличество сохранённых строк
        """
        db = MySQLdb.Connect(**self.dsn_stream)
        cursor = db.cursor()
        field = 'fm3'

        # удаление записей из stream за period если они там есть
        sql = "DELETE FROM {table} WHERE period='{period}'".format(table=self.table_stream, period=period)
        r = execute(cursor, sql)
        xlog('stream: deleted {records} records from {table} for {period}'.
             format(records=r, table=self.table_stream, period=period))

        account = get_last_account(cursor=cursor, table=self.table_stream)
        records = 0

        for cid in self._customers:
            tar = self.get_tar(cid)
            rlike = self.get_sql_rlike(field, tar.reg)
            sql = "SELECT {field} number, sum(min) AS sum_min FROM {table} WHERE (cid={cid} AND stp='+') " \
                  "GROUP BY {field} HAVING ({rlike})".format(field=field, table=self.table_bill, cid=cid,
                                                             rlike=rlike)

            cursor.execute(sql)

            for line in cursor:
                number, sum_min = line
                account += 1
                sql = "INSERT INTO {table} (`account`, `cid`, `period`, `number`, `min`) " \
                      "VALUES('{account}', '{cid}', '{period}', '{number}', '{min}')".\
                    format(table=self.table_stream, account=account, cid=cid, period=self.period,
                           number=number, min=sum_min)
                # print(sql)
                records += execute(cursor, sql)

        xlog('stream: insert {records} records in table {table} for {period}'.
             format(records=records, table=self.table_stream, period=period))


def set_local_tariff_for_customers(dsn):
    """
    Установка кода тарифа местной связи в клиентской базе (делается один раз)
    call: set_local_tariff_for_customers(cfg.dsn_cust2)
    :param dsn:
    :return:
    """
    db = MySQLdb.Connect(**dsn)
    cursor = db.cursor()

    table = 'customers.Cust'
    requests = list()

    # тариф=1 ab450_pr0.6, по умолчанию:  (450мин входит в абон_плату и превышение по 0.6руб за 1 мин)
    requests.append('UPDATE {table} SET `tid_l`=1'.format(table=table))
    # тариф=2, безлимит: Хайленд(29);БизнесЛогистика(682);ЕВРОТРАНС(626);Интех(628);СМБ(204)
    requests.append('UPDATE {table} SET `tid_l`=2 WHERE `CustID` IN (29,682,626,628,204)'.format(table=table))
    # тариф=3, ab450_pr0.45: Индивидульный тариф(абон450мин_превышение0.45руб): Доминанта(20); РАМБ(289)
    requests.append('UPDATE {table} SET `tid_l`=3 WHERE `CustID` IN (20,289)'.format(table=table))

    for sql in requests:
        cursor.execute(sql)
        xlog('update {records} records: {sql}'.format(records=cursor.rowcount, sql=sql))
    cursor.close()
    db.close()


class BillingLocal(object):
    """
    Телефонный биллинг лосальных связей (повремёнка)
    """

    def __init__(self, opts):
        """
        :param opts: параметры
        """
        self.opts = opts
        self.cid2tid = dict()   # отображение кода клиента на код тарифа по повремёнке: cid->tid
        self.cid2type = dict()  # отображение кода клиента на тип клиента: cid->uf
        self.tar = dict()       # отображение кода тарифа повремёнки на данные тарифа: tid->{abmin, cost1min}
        self.field_from = 'fm3'     # поле для from местной связи

    def _read_local_tar(self, dsn, table):
        """
        Создание мапы tid->{abmin, cost1min}
        :param dsn: параметры подключения к базе тарифов
        :param table: таблица с тарифами местной связи
        :return:
        """
        db = MySQLdb.Connect(**dsn)
        cursor = db.cursor()

        sql = 'SELECT `tid`, `abmin`, `cost1min` FROM {table}'.format(table=table)
        cursor.execute(sql)
        for line in cursor:
            tid, abmin, cost1min = line
            self.tar[tid] = {'abmin': abmin, 'cost1min': cost1min}
        cursor.close()
        db.close()

    def get_tar(self, tid):
        """ Возвращает тариф в виде словаря {abmin, cost1min} для рассчёта повремёнки """
        return self.tar.get(tid, {})

    def print_local_tar(self):
        """ Печатает все тарифы по локальной(местной) связи """
        tid_list = self.tar.keys()
        for tid in tid_list:
            t = self.get_tar(tid)
            print('{tid}->{abmin}мин,  превыш: {cost1min} руб/мин'.format(tid=tid, abmin=t['abmin'],
                                                                          cost1min=t['cost1min']))

    def _read_local_tar_by_customer(self, dsn, table):
        """
        Create map: cid->tid для повремёнки
        :param dsn: параметры подключения к базе клиентов
        :param table: таблица с клиентами
        """
        db = MySQLdb.Connect(**dsn)
        cursor = db.cursor()

        sql = 'SELECT `CustID`, `CustType` customer_type, `tid_l` FROM {table}'.format(table=table)
        cursor.execute(sql)
        for line in cursor:
            cid, customer_type, tid = line
            self.cid2tid[cid] = tid
            self.cid2type[cid] = customer_type
        cursor.close()
        db.close()

    def get_tid(self, cid):
        """ Возвращает код тарифа для рассчёта повремёнки """
        return self.cid2tid.get(cid, 0)

    def get_type(self, cid):
        """ Возвращает тип клиента (u|f) """
        return self.cid2type.get(cid, '-')

    def print_cid2tid(self):
        """ Печатает отображение кода клиента на код тарифа локальной(местной) связи """
        keys = list(self.cid2tid.keys())
        keys.sort()
        for cid in keys:
            print('{cid}->{tid}'.format(cid=cid, tid=self.get_tid(cid)))

    @staticmethod
    def _get_stream_customers(dsn, table):
        """
        Возвращает список клиентов, у которых местная связь по потоку
        :param dsn: параметры подключения
        :param table: таблица
        :return: список в виде '1,23,324'
        """
        db = MySQLdb.Connect(**dsn)
        cursor = db.cursor()
        sql = "SELECT `cid` FROM {table} ORDER BY `cid`".format(table=table)
        stream_cid_list = []
        cursor.execute(sql)
        for line in cursor:
            cid = line[0]
            stream_cid_list.append(cid)
        cursor.close()
        db.close()
        return ','.join(map(str, stream_cid_list))

    def _delete_if_exist(self, dsn):
        """
        Удаление записей из loc_book и loc_numbers за период если они там есть
        :param dsn:
        :return:
        """
        db = MySQLdb.Connect(**dsn)
        cursor = db.cursor()

        requests = list()
        requests.append("DELETE FROM {table} WHERE period='{period}'".
                        format(table=self.opts.table_book, period=self.opts.period))
        requests.append("DELETE FROM {table} WHERE period='{period}'".
                        format(table=self.opts.table_numbers, period=self.opts.period))

        for sql in requests:
            records = execute(cursor, sql)
            xlog('delete {records} records: {sql}'.format(records=records, sql=sql))

    def _marked_local_calls(self, cursor):
        """
        Маркировка местных вызовов для биллинга (stp='+')
        :param cursor: курсор на таблицу с данными (Y2021M11)
        :return: количество промаркированных записей
        """
        marked = 0

        sql = "UPDATE {table} SET `stp`='+' WHERE `_stat`='G' AND " \
              "(`{field_from}` LIKE '626%' OR `fmx` LIKE '8495626%')".\
            format(table=self.opts.table_bill, field_from=self.field_from)
        records = execute(cursor, sql)
        xlog('marked {records} 626x records'.format(records=records))
        marked += records

        sql = "UPDATE {table} SET `stp`='+' WHERE `_stat`='G' AND " \
              "(`{field_from}` LIKE '642%' OR `fmx` LIKE '8499642%')".\
            format(table=self.opts.table_bill, field_from=self.field_from)
        records = execute(cursor, sql)
        xlog('marked {records} 642x records'.format(records=records))
        marked += records

        return marked

    def _get_cust_for_billing(self, cursor, exclude_cid_list):
        """
        Возвращает список потенциальных клиентов для местного биллинга
        :param cursor: курсор на таблицу с данными (Y2021M11)
        :param exclude_cid_list: список исключаемых клиентов
        :return: список клиентов (1,2,3)
        """
        cid_list = []

        # выборка только записей юр-лиц (uf='u') помеченных для биллинга местной связи (stp='+')
        sql = "SELECT cid FROM {table} WHERE stp='+' AND uf='u' GROUP BY cid HAVING (cid NOT IN ({exclude_cid_list}))".\
            format(table=self.opts.table_bill, exclude_cid_list=exclude_cid_list)
        cursor.execute(sql)
        customers_list = cursor.fetchall()

        for x in customers_list:
            cid_list.append(x[0])

        return cid_list

    def _calc_results(self, cursor, cid_list):
        """
        По каждому клиенту из списка cid_list определяем возможное превышение и сохраняем его в loc_numbers
        :param cursor: курсор на таблицу с данными (Y2021M11)
        :param cid_list: список клиентов (223,56,300,...)
        :return: true | false - было ли превышение хоть у одного клиента
        """
        step, step_cust, step_records, sum_overrun = (0, 0, 0, 0.0)
        bar = Progressbar(info='calc results for: {table}'.format(table=self.opts.table_bill), maximum=len(cid_list))

        # 1. Для каждого клиента из списка cid_list
        for cid in cid_list:
            tid = self.get_tid(cid)
            tar = self.get_tar(tid)
            abmin = tar['abmin']
            cost1min = tar['cost1min']

            step += 1
            bar.update_progress(step)

            if cost1min == 0:    # безлимитка
                continue

            # 2. Ищем номера с превышением лимита на местную связь (fm3 - номер 'от кого' для местной связи)
            sql = "SELECT `{field_from}` number, sum(`min`) sum_min FROM {table} " \
                  " WHERE (`cid`={cid} AND `stp`='+' AND `uf`='u')" \
                  " GROUP BY `{field_from}`" \
                  " HAVING (sum(`min`)>{abmin})".\
                format(table=self.opts.table_bill, cid=cid, abmin=abmin, field_from=self.field_from)

            cursor.execute(sql)

            # 3. И сохраняем результат превышения лимита
            if cursor.rowcount > 0:
                step_cust += 1
                for line in cursor:
                    number, sum_min = line
                    sum_overrun += self._save_result(cursor, self.opts.period, cid, number, sum_min, abmin, cost1min)
                    step_records += 1

        bar.go_new_line()
        xlog("saved '{customers}/{records}/{sum_overrun}' (customers/records/summa) in table: {table}".
             format(customers=step_cust, records=step_records, sum_overrun=sum_overrun, table=self.opts.table_numbers))

        return step_records > 0

    def _save_result(self, cursor, period, cid, number, sum_min, abmin, cost1min):
        """
        Сохранение результата местной связи в loc_numbers
        :param cursor: курсор на базу с данными
        :param period: период, 2021_01
        :param cid: код клиента
        :param number: телефонный номер
        :param sum_min: количество минут за period
        :param abmin: абон_минут, входящих в абон_плату
        :param cost1min: стоимость 1-й минуту превышения (руб/мин)
        :return: сумма за превышение
        """

        sum_overrun = round(((int(sum_min) - abmin) * cost1min), 2)

        sql = "INSERT INTO {table} (`cid`, `period`, `number`, `min`, `abmin`, `cost1min`, `sum`) " \
              "VALUES ('{cid}', '{period}', '{number}', '{min}' ,'{abmin}', '{cost1min}', '{sum}')".\
            format(table=self.opts.table_numbers, cid=cid, period=period, number=number, min=sum_min, abmin=abmin,
                   cost1min=cost1min, sum=sum_overrun)

        execute(cursor, sql)

        return sum_overrun

    def _calc_book(self, cursor):
        """
        Из таблицы с данными по номерам (loc_numbers) создаёт итоги в таблице loc_book
        :param cursor: курсор
        :return: общая сумма превышения
        """
        account = self._get_last_account(cursor) + 1
        date = utils.sqldate(datetime.date(datetime.now()))

        sql = "SELECT cid, sum(sum) sum_sum FROM {table} WHERE period='{period}' GROUP BY cid".\
            format(table=self.opts.table_numbers, period=self.opts.period)
        cursor.execute(sql)

        total_summa = 0.0

        for line in cursor:
            cid, sum_sum = line
            total_summa += sum_sum
            sql = "INSERT INTO {table} (`account`, `cid`, `uf`, `period`, `dt`, `sum`) " \
                  "VALUES('{account}', '{cid}', '{uf}', '{period}', '{dt}', '{sum}')".\
                format(table=self.opts.table_book, account=account, cid=cid, uf=self.get_type(cid),
                       period=self.opts.period, dt=date, sum=sum_sum)
            execute(cursor, sql)
            account += 1

        return total_summa

    def _get_last_account(self, cursor, field='account'):
        """
        Возвращает последний номер счёта account
        :param cursor: курсор
        :field: название поля у которого вычисляем последний номер
        :return: последний account
        """
        sql = "SELECT max(`{field}`) FROM {table}".format(field=field, table=self.opts.table_book)
        cursor.execute(sql)
        max_number = cursor.fetchone()[0]
        if not max_number:
            max_number = 0

        return max_number

    def _set_number_local(self, cursor, where=None):
        """
        Устновка номера from (fm3) для местной связи
        :param cursor: курсор на базу с данными
        :param where: дополнительный фильтр
        :return: количество номеров
        """
        re_626 = re.compile(r'626\d{3}')
        re_642 = re.compile(r'642\d{3}')
        re_710 = re.compile(r'710\d{3}')
        re_627 = re.compile(r'627\d{3}')
        re_812 = re.compile(r'812\d{3}')
        re_811 = re.compile(r'811\d{3}')

        sql = "SELECT `id`, `cid`, `fm`, `fm2` FROM {table} WHERE `_stat`='G'".format(table=self.opts.table_bill)

        if where:
            sql += " AND ({where}) ".format(where=where)
        cursor.execute(sql)

        info = 'set from({field_from}) for local billing: {table}'.\
            format(table=self.opts.table_bill, field_from=self.field_from)

        bar = Progressbar(info=info, maximum=cursor.rowcount)
        count, updated, step, step_block, rowcount = (0, 0, 0, 100, cursor.rowcount)

        sql_begin = "UPDATE {table} SET `{field_from}`=CASE".\
            format(table=self.opts.table_bill, field_from=self.field_from)
        sql_end = "ELSE `{field_from}` END".format(field_from=self.field_from)

        values = list()

        for line in cursor:
            num = '-'
            idx, cid, fm, fm2 = line
            if re_626.match(fm2):
                num = fm2
            elif re_642.match(fm2):
                num = fm2
            elif re_710.match(fm2):
                num = fm2
            elif re_627.match(fm2):
                num = fm2
            elif cid == 58 and re_812.match(fm2):
                num = fm2
            elif cid == 58 and re_812.match(fm):
                num = fm
            elif cid == 273 and re_812.match(fm):
                num = fm
            elif (cid == 319 or cid == 53) and re_812.match(fm):  # Терминал-Сити(319), РВЛ-Строй(53)
                num = fm
            elif cid == 282 and re_811.match(fm):
                num = fm
            elif cid == 957 and re_811.match(fm):  # ?
                num = fm

            values.append((idx, num))

            step += 1
            count += 1
            bar.update_progress(count)

            if step == step_block or count >= rowcount:
                id_list = [it[0] for it in values]
                where = "WHERE `id` IN ({list})".format(list=','.join(map(str, id_list)))
                when = ''
                for it in values:
                    when += "WHEN `id`={id} THEN '{number}' ".format(id=it[0], number=it[1])

                sql = "{sql_begin} {when} {sql_end} {where}". \
                    format(sql_begin=sql_begin, when=when, sql_end=sql_end, where=where)
                updated += execute(cursor, sql)
                step = 0
                values = []

        bar.go_new_line()
        xlog("set {all}/{updated} (count/updated) number for local".format(all=count, updated=updated))
        return step

    def bill(self, dsn_bill, dsn_tar, dsn_cust, info=''):
        """
        Главная ф-ия биллинга местной связи
        :param dsn_bill: параметры подключения к базе биллинга
        :param dsn_tar: параметры подключения к базе тарифов
        :param dsn_cust: параметры подключения к базе клиентов
        :param info: текст для логирования
        """
        t1 = time.time()
        xlog('period: {period}'.format(period=self.opts.period))

        db = MySQLdb.Connect(**dsn_bill)
        cursor = db.cursor()

        # определение номера местной связи (fm3)
        self._set_number_local(cursor)

        # сохранение в мапе отношения tid->{abmin, cost1min}
        self._read_local_tar(dsn=dsn_tar, table=self.opts.table_numbers_tar)

        # сохранение в мапе отношения cid->tid для местной связи
        self._read_local_tar_by_customer(dsn=dsn_cust, table=self.opts.table_customers)

        # если за период расчёт уже был, то удаляем записи из loc_numbers и loc_book
        self._delete_if_exist(dsn=dsn_bill)

        # маркируем местные вызовы (stp='+')
        self._marked_local_calls(cursor)

        # клиенты, у которых местная связь считается по потоку, в расчёте по номерам не учавствуют
        stream_cid_list = self._get_stream_customers(dsn=dsn_tar, table=self.opts.table_stream_tar)

        # получаем список потенциальных клиентов для местного биллинга (исключаем тех у которых номера в потоке)
        cid_list = self._get_cust_for_billing(cursor, stream_cid_list)

        # по каждому клиенту - если есть превышение, то записываем в таблицу loc_numbers и итоги в loc_book
        if self._calc_results(cursor, cid_list):
            total = self._calc_book(cursor)
            xlog("total='{total}\u20BD' in table: {table}".format(total=total, table=self.opts.table_book))

        # синхронизация таблиц loc_numbers и loc_book по полю account
        sql = "UPDATE {table_book} b JOIN {loc_numbers} r ON b.cid=r.cid SET r.account=b.account " \
              "WHERE b.period=r.period".format(table_book=self.opts.table_book, loc_numbers=self.opts.table_numbers)
        records = execute(cursor, sql)

        xlog("updated {records} records on the field 'account' in table: {loc_numbers}".
             format(records=records, loc_numbers=self.opts.table_numbers))

        cursor.close()
        db.close()

        if info:
            xlog(info)

        t2 = time.time()
        xlog("work: {0:0.2f} sec".format(t2 - t1))


if __name__ == '__main__':
    p = optparse.OptionParser(description="billing of local calls ",
                              prog="local.py", version="0.1a", usage="local.py --year=year --month=month [--log=file]")

    p.add_option('--year', '-y', action='store', dest='year', help='year, example 2021')
    p.add_option('--month', '-m', action='store', dest='month', help='month in range 1-12')
    p.add_option('--log', '-l', action='store', dest='log', default=flog, help='logfile')

    opt, args = p.parse_args()
    opt.year = ini.year
    opt.month = ini.month

    if not opt.year or not opt.month or not opt.log:
        print(p.print_help())
        exit(1)

    opt.table_bill = 'Y{year:04d}M{month:02d}'.format(year=int(opt.year), month=int(opt.month))
    opt.period = '{year:04d}_{month:02d}'.format(year=int(opt.year), month=int(opt.month))

    opt.table_numbers_tar = 'tarif.loc_numbers_tar'
    opt.table_stream_tar = 'tarif.loc_stream_tar'
    opt.table_numbers = 'bill.loc_numbers'
    opt.table_stream = 'bill.loc_stream'
    opt.table_book = 'bill.loc_book'
    opt.table_customers = 'customers.Cust'

    logging.basicConfig(
        filename=opt.log, level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S", format='%(asctime)s %(message)s', )
    log = logging.getLogger('app')

    try:
        # set_local_tariff_for_customers(cfg.dsn_cust2)     # one time for set customers.Cust.tid_l

        # create local tables if no exists
        create_tables(tables=local_tables)

        # local by numbers
        local = BillingLocal(opt)
        local.bill(dsn_bill=cfg.dsn_bill, dsn_tar=cfg.dsn_tar, dsn_cust=cfg.dsn_cust, info='')

        # local by stream
        stream = Stream(dsn_tar=cfg.dsn_tar, dsn_stream=cfg.dsn_bill, dsn_bill=cfg.dsn_bill,
                        table_stream_tar=opt.table_stream_tar, table_stream=opt.table_stream,
                        table_bill=opt.table_bill, period=opt.period)

        stream.save_data_stream(opt.period)

        xlog('.')

    except MySQLdb.Error as e:
        log.exception(str(e))
        print(e)
    except RuntimeError as e:
        log.exception(str(e))
        print(e)
    except Exception as e:
        log.exception(str(e))
        traceback.print_exc(file=open(opt.log, "at"))
        traceback.print_exc()
