#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Биллинг телефонии (created: 2016_03)
 opt.table = 'Y2016M03'
 opt.log = filename
 opt.filenoexistnumber = filename_no_exist_number
 bill = Billing(opt)
 bill.bill(dsn=cfg.dsn_bill, info=opt.table, save_db=True, where=None)
"""
import os
import sys
import optparse
import logging
from logging import Logger

import MySQLdb
import traceback
import time
#
from modules import cfg              # конфиг
from modules import codedef          # коды СПС
from modules import customers        # клиенты
from modules import link             # текущая разбираемая связь
from modules import result           # итоги по отбираемым записям
from modules import numbers1         # номера
from modules import tarmts           # тарифы МТС
from modules import calltype         # тип звонка (M W S Z V G)
from modules import calendar         # производственный календарь
from modules import calc             # функции для вычислений НДС и пр.
from modules.func import Func        # разные функции
from modules.progressbar import Progressbar     # прогресс-бар
import ini

root = os.path.realpath(os.path.dirname(sys.argv[0]))
flog = "{root}/log/{file}".format(root=root, file='bill.log')


def itog_log(info='-', step=0, update=0, tm1=0.0, tm2=0.0, cost=0.0, min_mg=0):
    """
    Логирование
    :param info: краткий текст
    :param step: количество шагов для достижения цели
    :param update: количество обновлённых записей
    :param tm1: временная метка начала процесса
    :param tm2: временная метка конца процесса
    :param cost: стоимость, если есть
    :param min_mg: общее кол-во минут МГ/МН/ВЗ
    """

    if step == 0 and update == 0 and tm1 == 0 and tm2 == 0 and cost == 0 and min_mg == 0:
        log.info(info)
    else:
        proc = 0 if step == 0 else float(update)/float(step)*100
        log.info('{info}: step/add: {step}/{update} {proc:.1f}% time:{time}s cost:{cost:.2f} min_mg:{min_mg}'.
                 format(info=info, step=step, update=update, proc=proc, time=int(tm2-tm1),
                        cost=cost, min_mg=min_mg))


class Update(object):
    """
    Сохранение(обновление) записи в таблице биллинга типа Y2016M03
    """
    def __init__(self, dsn, table):
        self.dsn = dsn
        self.table = table
        self.db = MySQLdb.Connect(**dsn)
        self.cursor = self.db.cursor()

    def __del__(self):
        self.cursor.close()
        self.db.close()

    def update(self, lnk):
        """
        Обновление записи
        :param lnk: объект link с инфо для сохранения
        """
        a = lnk

        sql = "UPDATE `{base}`.`{table}` SET _f='+', _cid={cid}, _org='{org}', " \
              "_stat='{stat}', _sts='{sts}', _zona={zona}, _code='{code}', " \
              "_desc='{desc}', _name='{name}', _tar={tar}, _tara={tara}, _sum={sum}, _sum2={sum2}, _suma={suma}, " \
              "_dnw='{dnw}', _nid='{nid}', _tid={tid}, _uf='{uf}', _pid={pid}," \
              " fm2='{fm2}', st='{st}', to2='{to2}'" \
              " WHERE id={idx}". \
            format(base=self.dsn['db'], table=self.table, cid=a.cid, org=a.org, stat=a.stat, sts=a.sts,
                   zona=a.zona, code=a.code, desc=a.desc, name=a.name, tar=a.tar, tara=a.tara, sum=a.sum, sum2=a.sum2,
                   suma=a.suma, dnw=a.dnw, idx=a.id, nid=a.nid, tid=a.tid_t, uf=a.uf, pid=a.pid,
                   fm2=a.fm2, st=a.st, to2=a.to2)

        # print(sql)
        self.cursor.execute(sql)

    def update_table(self):
        """
        Обновление полей таблицы (поля без _) данными из нового расчёта (поля с префиксом _)
        Зачем?: на эти поля (без _) пока настроена программа расшифровок на Access
        :return: количество обновлённых записей
        """

        sql = "UPDATE `{base}`.`{table}` SET uf=_uf, cid=_cid, pid=_pid, stat=_stat, sts=_sts, dnw=_dnw, sum=_sum, " \
              "sum2=_sum2, sumKOM=_suma, naprKOM=_name, org=_org, zona=_zona, code=_code, tid=_tid, ok=_f". \
            format(base=self.dsn['db'], table=self.table)

        self.cursor.execute(sql)
        return self.cursor.rowcount


class Billing(object):
    """
    Телефонный биллинг
    """
    # клиенты
    cust = customers.Cust(dsn=cfg.dsn_cust)
    custks = customers.CustKs(dsn=cfg.dsn_cust)
    cust_replace = customers.CustReplace(dsn=cfg.dsn_cust)

    # номера и тарифы ВМ связи ФГУП РСИ
    n811 = numbers1.Number811(dsn=cfg.dsn_tar)

    # городские номера клиентов
    # self.numbers = numbers1.Numbers(dsn=cfg.dsn_tel, table=opts.table, n811=self.n811)

    # коды СПС - связи
    cdef = codedef.Codedef(dsn=cfg.dsn_tar, tabcode='defCode')

    # тип звонка по номеру
    ctype = calltype.Calltype(cdef=cdef)

    # тарифы МТС
    mts = tarmts.Tarmts(dsn=cfg.dsn_tar, tabcode='komstarCode', tabtar='komstarTar', tabtar2='komstarTarRss',
                        tabtarmts='mtsTar', stat=ctype, cdef=cdef, custdefault=1171)

    def __init__(self, opts):
        """
        :param opts: параметры
        """
        self.opts = opts
        # городские номера клиентов
        self.numbers = numbers1.Numbers(dsn=cfg.dsn_tel, table=self.opts.table, n811=self.n811)

    def bill(self, dsn, info, where=None, save_db=True):
        """
        Главная ф-ия биллинга
        :param dsn: параметры подключения к базе
        :param info: текст для логирования
        :param where: доп_условия отбора записей
        :param save_db: True|False:  True-сохранять изменения в базе; False-вывод на экран
        """
        t1 = time.time()
        db = MySQLdb.Connect(**dsn)
        cursor = db.cursor()
        table = self.opts.table

        # для результатов
        res = result.Result()

        # объект для обновления записи
        upd = Update(dsn=dsn, table=table)

        # календарь
        cal = calendar.Calendar(dsn=cfg.dsn_cal, table=table)

        # бизнес правила (могут менять плательщика и в результате сумму)
        # rul = rules.Rules(dsn=cfg.dsn_tel, table='rule_bill')

        # инфо по одной связи
        q = link.Link()
        q.prn_title()

        sql = "select id, dt, fm, fmx, `to`, tox, sec, min, op from `{table}` where sec>0".format(table=table)
        if where:
            sql = "{sql} and {where}".format(sql=sql, where=where)

        cursor.execute(sql)
        bar = Progressbar(info=info, maximum=cursor.rowcount)

        count_noexist_number = 0
        step, step_update, sum_cust = (0, 0, 0.,)
        for line in cursor:
            step += 1
            if save_db:
                bar.update_progress(step)
            q.__init__()
            q.id, q.dt, q.fm, q.fmx, q.to, q.tox, q.sec, q.min, q.op = line
            q.dnw = cal.dnw(q.dt)
            q.to2 = q.to
            q.to = Func.prepare_to(q.to)
            q.fm2 = Func.get_number_fm2(q.fm, q.fmx)
            # print "{dt} {fm} {to} {tox} {sec} {min}".format(dt=q.dt, fm=q.fm, to=q.to, tox=q.tox, sec=q.sec,min=q.min)

            # по номеру узнаем клиента
            q.cid, q.org, q.vpn = self.numbers.get_cidorg(q.fm, q.fmx)

            # у клиента может быть замена
            q.cid = self.cust_replace.get_cid_new(q.cid)

            # если за номер некому платить, запомним номер и продолжим
            if q.cid == '0':
                Func.save_noexist_number(db=dsn['db'], table=table, idx=q.id, fm=q.fm, fmx=q.fmx, to=q.to,
                                         stat=self.ctype.getsts(q.to, q.tox), filename=self.opts.filenoexistnumber)
                count_noexist_number += 1
                continue

            # инфо по клиенту
            cst = self.cust.get_cust(q.cid)
            q.uf, q.cust, q.tid_t = (cst.uf, cst.custalias, cst.tid_t)
            q.pid = 0
            if q.uf == 'f':
                q.pid = self.custks.get_pid_by_number(q.fm)
                q.tid_t = self.custks.get_cust(q.pid).tid

            # стоимость 1 мин и тд.
            # 2020-12-24 number -> cid -> tid -> cost 1 min
            q.sts, q.code, q.zona, q.tar, q.tara, q.nid, q.desc, q.name = \
                self.mts.get_sts_code_zona_tar_tara_nid_desc_name(tid=q.tid_t, org=q.org, to=q.to, tox=q.tox)
            q.stat = self.ctype.get_mwszg(q.sts)        # M, W, S, Z, G, V
            q.st = cfg.stat2st.get(q.stat, '-')         # MG VZ GD

            # в итоге - стоимость разговора
            q.sum = q.tar * q.min           # sum - сумма клиенту (ЮЛ-без НДС; ФЛ-с НДС)
            q.suma = q.tara * q.min         # suma - агенту (без НДС)
            q.sum2 = q.sum                  # sum2 - сумма клиенту без НДС

            # для физлиц НДС включён в тариф
            if q.uf == 'f':
                q.sum2 = calc.sum_without_nds(q.sum)   # sum2 для ФЛ - вычитаем НДС
                q.pid = self.custks.get_pid_by_number(q.fm)

            # бизнес-правила:
            if q.sec <= int(cfg.calc['minsec']):
                q.sum = q.sum2 = q.suma = q.sec = q.min = 0

            # общая сумма без НДС
            sum_cust += float(q.sum2)

            # результат
            if q.sum > 0:
                res.add(q.sts, q.sec, q.min, q.sum2, q.suma)

            # обновление записи
            if save_db:
                upd.update(lnk=q)
                step_update += 1
            else:
                q.prn()

        if count_noexist_number:
            print(("!номеров, отсутствующих в базе:{count}".format(count=count_noexist_number)))

        t2 = time.time()
        itog_log(info=res.result_all())
        itog_log(info, step=step, update=step_update, tm1=t1, tm2=t2, cost=sum_cust, min_mg=res.get_result_mg()['min'])

        # обновление полей таблицы
        records = upd.update_table()
        itog_log(info="updated: {records} records: field = _field ".format(records=records))

        itog_log('.')


if __name__ == '__main__':
    p = optparse.OptionParser(description="billing",
                              prog="bill.py", version="0.1a", usage="bill.py --tab=table [--log=namefile]")

    p.add_option('--tab', '-t', action='store', dest='table', help='table, ex.Y2013M09')
    p.add_option('--log', '-l', action='store', dest='log', default='log/bill.log', help='logfile')

    opt, args = p.parse_args()
    opt.table = ini.table   # Y2021M08

    opt.log = flog
    opt.filenoexistnumber = 'log/nonum.txt'   # сбор номеров необх. для биллинга но их нет в тел_базе

    if not opt.table or not opt.log:
        print(p.print_help)
        exit(1)

    logging.basicConfig(
        filename=opt.log, level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S", format='%(asctime)s %(message)s', )
    log: Logger = logging.getLogger('app')

    try:
        bill = Billing(opt)

        bill.bill(dsn=cfg.dsn_bill, info=opt.table, save_db=True, where="id>0")

        # bill.bill(dsn=cfg.dsn_bill, info=opt.table, save_db=True, where="cid=204")

        # bill.bill(dsn=cfg.dsn_bill, info=opt.table, save_db=True, where="uf='f'")

        # bill.bill(dsn=cfg.dsn_bill, info=opt.table, save_db=True, where="_f='-'")

        # bill.bill(dsn=cfg.dsn_bill, info=opt.table, save_db=True, where="sec <= 3")

        # bill.bill(dsn=cfg.dsn_bill, info=opt.table, save_db=True, where="cid in (9999, 546)")

        # bill.bill(dsn=cfg.dsn_bill, info=opt.table, save_db=True, where="fm like '%6428346' or fm like '%6428347'")

        # bill.bill(dsn=cfg.dsn_bill, info=opt.table, save_db=True, where="fmx like '%6269911'")

        # bill.bill(dsn=cfg.dsn_bill, table='Y2016M02', info='Y2016M02(W)',  where="`stat`='W' limit 1000")
        # bill.bill(dsn=cfg.dsn_bill, table='Y2016M02', where="cid=549  limit 100")

    except MySQLdb.Error as e:
        log.warning(str(e))
        print(e)
    except RuntimeError as e:
        log.warning(str(e))
        print(e)
    except Exception as e:
        log.warning(str(e))
        traceback.print_exc(file=open(opt.log, "at"))
        traceback.print_exc()
