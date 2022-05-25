#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Загрузка данных из SMG в биллинг
"""
import re
import time
import logging
import pymysql
import optparse
import traceback
from cfg import cfg, ini
from modules import codedef
from modules import utils as ut
from modules.progressbar import Progressbar

flog = cfg.paths['logging']['load']    # лог-файл


def date_first_day_month(table):
    return "%s-%s-01" % (table[1:5], table[6:8])  # Y2013M09 => 2013-09-01


def execute(cur, sql, verbose=True):
    """ execute request """
    cur.execute(sql)
    if verbose:
        log.info(sql)


def itog_log(info='-', step=0, update=0, t1=0.0, t2=0.0, cost=0):

    log.info('{info}: step/add: {step}/{update} {proc:.1f}% time:{time}s cost:{cost:.2f}'.
             format(info=info, step=step, update=update, proc=float(update)/(step+1.)*100, time=int(t2-t1), cost=cost))


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
    def select_tar(dnw, tar0820, tar2008, tar_w):
        """ select tariff D|N|W """
        if dnw == 'D':
            return tar0820
        elif dnw == 'N':
            return tar2008
        else:
            return tar_w

    @staticmethod
    def progress(step, records, current):
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


class Calendar(object):
    """ work calendar FGUP RSI & OOO RSS """
    def __init__(self, dsn, table):
        self.year = int(table[1:5])     # table = Y2013M09
        self.month = int(table[6:8])
        db = pymysql.Connect(**dsn)
        cur = db.cursor()
        sql = "select cal from `calendar`.`calendar` where `year`={0:d} and `month`={1:d}".format(self.year, self.month)
        cur.execute(sql)
        if cur.rowcount == 1:
            self.cal = cur._rows[0][0]
        cur.close()
        db.close()

    def dnw(self, dt):
        """
        :return: D|N|W
        :param dt: object mysql datetime (2013-09-01 02:34:41)
        """
        if self.cal[dt.day-1] == '1':
            return 'W'

        if dt.hour in range(8, 20):
            return 'D'
        else:
            return 'N'
# end class Calendar


class Stat(object):
    """ obtain: stat (mg,mn,vz,gd), dnw(D,N,W) """

    def __init__(self, cal, cdef):

        self.cal = cal              # object Calendar
        self.cdef = cdef            # object Codedef (defx9)
        self.re_msk = re.compile('^(49[589])')         # stat=GD (moskva)
        self.re_gd = re.compile('^8(49[589])\d{7,}')   # stat=GD
        self.re_xx = re.compile('^8(80[04])\d{7,}')    # stat=XX (free call)
        self.re_mg = re.compile('^8([2-8]\d{7,})')     # stat=MG
        self.re_vm = re.compile('^811(\d{4})')         # stat=VM
        self.re_mn = re.compile('^810(\d{10,})|^8(7[12]\d{8,})')       # stat=MN, 871|872=KZ
        self.re_tm = re.compile('^(10[01234])')        # stat=GD (time, ...)

        #for line in open(self.fname):
        #    self.__addcodevz__(line)  # 905500-905599,905700-905799

    def __prepare__(self, to, tox=None):
        """ prepare number (cut first 9)
        """
        if self.re_msk.match(to):  # 495* 498* 499*
            return '8' + to
        if to.startswith('98258'):
            return to[4:]
        if to.startswith('9818'):   # 98189263072135 -> 89263072135
            return to[3:]
        if tox:
            if "810" + to == tox:    # to=37167005514 tox=81037167005514
                to = tox
        if to.startswith('9'):
            return to[1:]
        return to

    def getsts(self, to, retprx=False, tox=None):
        """ return sts (mg, mn, vz, vm, gd, xx) call to number 'to'
        :param to:
        :param tox:
        :param retprx: True|False  (return or no prefix)
        return: sts
        return: (sts, prx) if retprx=True
        """

        to = self.__prepare__(to, tox)
        prx = ''
        stat = '-'
        if to.startswith('89') or to.startswith('79'):
            if self.cdef.is_codevz(to[1:7]):
                stat = 'vz'; prx = to[1:7]
            else:
                stat = 'mgs'; prx = to[1:7]  # mgs = mg sps
        if stat == '-':
            m = self.re_gd.match(to)
            if m:
                stat = 'gd'; prx = m.group(1)
        if stat == '-':
            m = self.re_mn.match(to)
            if m:
                stat = 'mn'; prx = m.group(1)

        # if stat == '-':
        #    m = self.re_xx.match(to)
        #    if m: stat = 'xx'; prx = m.group(1)

        if stat == '-':
            m = self.re_mg.match(to)
            if m:
                stat = 'mg'; prx = m.group(1)
        if stat == '-':
            m = self.re_vm.match(to)
            if m:
                stat = 'vm'; prx = m.group(1)

        if stat == '-':
            if to in ('100', '101', '102', '103', '104', '105', '112', '122'):
                stat = 'gd'
                prx = to

        if retprx:
            return stat, prx
        else:
            return stat

    def getstat(self, to):
        """ return stat (mg, mn, vz) call to number 'to'
        """
        if to.startswith('89') or to.startswith('79'):
            prx = to[1:7]
            if self.cdef.is_codevz(prx):
                #if self.iscodevz(prx):
                stat = 'vz'
            else:
                stat = 'mg'
        elif to.startswith('810') or to.startswith('871') or to.startswith('872'):
            stat = 'mn'
        elif self.re_mg.match(to):
            stat = 'mg'
            if self.re_gd.match(to):
                stat = 'gd'
        else:
            stat = '-'
        return stat

    def getMWSZG(self, sts):
        """ return translate sts( mg, mn, mgs, vz, gd ) to (MWSZG)
        """
        q = dict (mg='M', mn='W', mgs='S', vz='Z', gd='G')
        return q.get(sts, '-')

    def getdnw(self, dt):
        """ return D | N | W for date: dt """
        return cal.dnw(dt)
# end class Stat


class Billing(object):
    """ add records to billing table (dsn pointer to db!table)  """

    def __init__(self, dsn, table, tab_sample):
        """
        :param dsn: dsn-param to database
        :rtype : None
        """
        self.db = pymysql.Connect(**dsn)
        self.cur = self.db.cursor()
        self.table = table
        self.__preparetable__(table, tab_sample)

    def __del__(self):
        self.cur.close()
        self.db.close()

    def insert(self, table, dt, fm, to, fmx='-', tox='-', sec=0, p='-', op='-', eq='-', link='-', cause=0,
               l1='-', l2='-', stat='-', sts='-', st='-', code='-', fm2='-', to2='-',
               b='-', ok='-', cid=0, cost=0, dnw='-', org='-', zona=0, eqid=0, p2='-'):

        min_ = Func.sec2min(sec)

        sql = "insert into `%s` (`b`,`ok`,`dt`,`fm`,`to`,`fmX`,`toX`,`fm2`,`to2`,`sec`,`min`," \
            "`l1`,`l2`,`link`,`p`,`eq`,`pr`,`stat`,`sts`,`st`,`code`, `cid`, `sum`, `dnw`, `org`," \
            "`zona`,`eqid`,`op`)" \
            " values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s'," \
              " '%s','%s', '%s', '%s','%s','%s','%s', '%s','%s', '%s', '%s', '%s')" % \
              (table, b, ok, dt, fm, to, fmx, tox, fm2, to2, sec, min_, l1[:7], l2[:7], link[:6], p, eq,
               cause, stat, sts, st, code, cid, cost, dnw, org, zona, eqid, op)
        # print(sql)

        self.cur.execute(sql)

    def __preparetable__(self, table, tab_sample):
        """
        create table if one not exists
        """
        sql= "SELECT 1 FROM `information_schema`.`TABLES` where table_schema = 'bill' and table_name = '{table}'".format(table=table)
        self.cur.execute(sql)

        if self.cur.rowcount == 0:
            f = open(tab_sample)
            sql = f.read().replace('_TABLE_CREATE_', table)
            f.close()
            self.cur.execute(sql)
            log.info("create tab:%s" % table)

    def split626(self, dsn_tel, tab_split, info='-'):
        """
        Разделение 626-х номеров на ФГУП РСИ и ООО РСС. Номера из таблицы tab_split - номера ООО РСС (500 штук)
        Результат разделение: R | G во временном поле org2
        В дальнейшем (в 2016) - это поле org
        :param info: инфо для логирования
        :return: количество изменённых записей
        """
        db = pymysql.Connect(**dsn_tel)
        cur_tel = db.cursor()
        cur_bill = self.cur
        # хэш 626-х номеров Речсвязьсервис
        num = dict()
        sql = "SELECT number FROM {table}".format(table=tab_split)
        cur_tel.execute(sql)
        for line in cur_tel:
            number, = line
            number = "8495" + number
            if num.get(number, 0) == 0:
                num[number] = '+'

        step = 0
        # разделение номеров в таблице с данными
        sql = "SELECT id, `fmx` FROM `{table}` WHERE org2='-' AND `fmx` LIKE '%626____'".format(table=self.table)
        cur_bill.execute(sql)
        bar = Progressbar(info=info, maximum=cur_bill.rowcount)

        for line in cur_bill:
            idd, number = line
            if num.get(number, 0) == 0:
                org = 'G'
            else:
                org = 'R'

            sql = "UPDATE `{table}` SET `org2`='{org}' WHERE id={idd}".format(table=self.table, org=org, idd=idd)
            cur_bill.execute(sql)
            step += 1
            bar.update_progress(step)

        bar.go_new_line()
        return step

# end class Billing


class Number811(object):
    """ numbers 811xxxx """

    rex = re.compile('x', re.IGNORECASE)

    def tr_simple(self, simple):
        return Number811.rex.sub('.{1}', simple)  # 8112[3-9]xx => 8112[3-9].{1}.{1}

    def __init__(self, dsn):
        """
        :param dsn: dsn-dict of db tarif
        saved all numbers 811xxxx
        """
        self.numbers = []
        db = pymysql.Connect(**dsn)
        cur = db.cursor()
        # code + tariff
        sql = "select c.id, c.clid, c.cid, c.org, c.simple, c.name, c.zona, t.tar2008, t.tar0820, t.tarW" \
            " from tarif.vmCode c join tarif.vmTar t on c.zona=t.ZonaID where fbil='+'"

        cur.execute(sql)
        for line in cur:
            id_, clid, cid, org, simple, name, zona, tar2008, tar0820, tarW = line
            spl = self.tr_simple(simple)
            m = re.compile(spl)
            self.numbers.append(dict(id=id_, clid=clid, cid=cid, org=org, simple_raw=spl, simple=m, name=name,
                                       zona=zona, tar2008=tar2008, tar0820=tar0820, tarW=tarW))

        cur.close()
        db.close()

    def getid(self, num):
        """
        :rtype : dictionary (id=id, clid=clid, cid=cid, ...)
        :param num: 9811xxxx or 811xxxx
        """

        for n in self.numbers:
            if n['simple'].search(num):
                return n
        return None

    def getcid(self, num):
        """
        :rtype : int
        :param num: 811xxxx
        :return: cid (CustID) or 0
        """

        for n in self.numbers:
            if n['simple'].search(num):
                return n['cid']
        return 0
# end class Number811


class Numbers(object):
    """
    number -> CustID
    """
    numbers = dict()
    numadd = ('6269000',)
    aon_replace = {
        '15820': {'number': '84956275272', 'cid': '17'},    # Зис(17)
        '15821': {'number': '84956275273', 'cid': '17'},
        '15822': {'number': '84957107371', 'cid': '17'},
        '25360': {'number': '84996428464', 'cid': '58'},    # Северный порт(58)
        '25355': {'number': '84996428481', 'cid': '58'},    # Северный порт(58)
        '25331': {'number': '84996428466', 'cid': '58'},    # Северный порт(58) c 12-07-2017
        '25364': {'number': '84996428464', 'cid': '58'},    # Северный порт(58) c 2019-
        '15824': {'number': '84996275285', 'cid': '188'},   # Авангард Холго(188) с 30-04-2019 по факту
    }
    #re811 = re.compile('^811.{4}$')
    re811 = re.compile('^81[12].{4}$')  # 811xxxx or 812xxxx


    def __init__(self, dsn, table, n811):
        """
        :param dsn: dsn-dict of db telefon
        :param table: ex. Y2013M09
        :param n811: instance Number811
        saved all numbers: xxxxxxx
        """
        self.n811 = n811
        db = pymysql.Connect(**dsn)
        cur = db.cursor()
        # current
        sql = "select number, cid from telefon.tel where number like '_______'"
        cur.execute(sql)
        for line in cur:
            num, cid = line
            n = num[:7]
            if self.numbers.get(n, 0) == 0:
                self.numbers[n] = cid

        # history
        dt = date_first_day_month(table)
        sql = "select number, cid from telefon.history where number like '_______' and d2>='{0:s}'".format(dt)
        cur.execute(sql)
        for line in cur:
            num, cid = line
            n = num[:7]
            if self.numbers.get(n, 0) == 0:
                self.numbers[n] = cid

        cur.close()
        db.close()

        for n in self.numadd:
            if self.numbers.get(n, -1) == -1:
                self.numbers[n] = 0
        
        # for aon, v in self.aon_replace.items():
    	#    print "{aon}->{v}, {cid}".format(aon=aon, v=v, cid=v['cid'])

        for aon, v in list(self.aon_replace.items()):
            if self.numbers.get(aon, -1) == -1:
                self.numbers[aon] = int(v['cid'])

        # debug
        # numbers = self.numbers.keys()
        # numbers.sort()
        # for aon in numbers:
        #    print "{aon} => {cid}".format(aon=aon, cid=self.numbers.get(aon, 0))


    def getcid(self, num):
        """
        :param num: 8495626xxxx or 626xxxx or 811xxxx or 710xxxx, ...
        :return: CustomerID(cid) or 0
        """
        if num.startswith('8495') or num.startswith('8499') or num.startswith('7495') or num.startswith('7499'):
            num = num[4:]
        elif num.startswith('495') or num.startswith('499'):
            num = num[3:]

        if self.re811.search(num):
            cid = self.n811.getcid(num)
        else:
            cid = self.numbers.get(num, 0)

        return cid

    def n811(self):
        return self.n811
# end class Numbers


class Number642Cust58(object):
    """ 642xxxx numbers SevPort(58) """
    numbers = dict()    # {123 => 58, 456 =>58, ...}
    numstr = ''         # 123,456, ...

    def __init__(self, numb):
        """
        :param numb: instance Numbers
        :return:
        """
        cid, nstr = (58, '')
        for x in numb.numbers:
            if x.startswith('642') and numb.numbers[x] == cid:
                self.numbers[x] = cid
                nstr += "{n},".format(n=x)
        if nstr:
            self.numstr = nstr[:-1]

    def getcid(self, num):
        """
        :param num: 8495642xxxx or 642xxxx
        :return: CustomerID(cid) or 0
        """
        if num.startswith('8495') or num.startswith('8499') or num.startswith('7495') or num.startswith('7499'):
            num = num[4:]
        elif num.startswith('499'):
            num = num[3:]

        return self.numbers.get(num, 0)
# end class Number642Cust58


class Number626(object):
    """ numbers 626xxxx for billing """
    numbers = dict()
    numadd = ('6269000',)

    def __init__(self, dsn, table):
        """
        :param dsn: dsn-dict of db telefon
        :param table: ex. Y2013M08
        saved all current+history numbers 626xxxx
        """
        db = pymysql.Connect(**dsn)
        cur = db.cursor()
        # current
        sql = "select number, cid from telefon.tel where number like '626%'"
        cur.execute(sql)
        for line in cur:
            num, cid = line
            n = num[:7]
            if self.numbers.get(n, 0) == 0:
                self.numbers[n] = cid

        # history
        dt = date_first_day_month(table)
        sql = "select number, cid from telefon.history where number like '626%' and d2>='{0:s}'".format(dt)
        cur.execute(sql)
        for line in cur:
            num, cid = line
            n = num[:7]
            if self.numbers.get(n, 0) == 0:
                self.numbers[n] = cid

        cur.close()
        db.close()

        for n in self.numadd:
            if self.numbers.get(n, -1) == -1:
                self.numbers[n] = 0

    def isexist(self, num):
        """
        :param num: 8495626xxxx or 626xxxx
        :return: True | False : num is into num626?
        """
        if num.startswith('8495'):
            num = num[4:]

        return (True, False)[self.numbers.get(num, 0) == 0]

    def getcid(self, num):
        """
        :param num: 8495626xxxx or 626xxxx
        :return: CustomerID(cid) or 0
        """
        if num.startswith('8495') or num.startswith('7495'):
            num = num[4:]

        return self.numbers.get(num, -1)
# end class Number626


class Number710(object):
    """ numbers 710xxxx & 627xxxx for billing """
    numbers = dict()

    def __init__(self, dsn, table):
        """
        :param dsn: dsn-dict of db telefon
        :param table: ex. Y2013M08
        saved all current+history numbers 710xxxx + 627xxxx
        """
        db = pymysql.Connect(**dsn)
        cur = db.cursor()
        # current
        sql = "select number, cid from telefon.tel where number like '710%' or number like '627%' "
        cur.execute(sql)
        for line in cur:
            num, cid = line
            n = num[:7]
            if self.numbers.get(n, 0) == 0:
                self.numbers[n] = cid

        # history
        dt = date_first_day_month(table)
        sql = "select number, cid from telefon.history where (number like '626%' or number like '627%')" \
            " and d2>='{0:s}'".format(dt)
        cur.execute(sql)
        for line in cur:
            num, cid = line
            n = num[:7]
            if self.numbers.get(n, 0) == 0:
                self.numbers[n] = cid

        cur.close()
        db.close()

    def getcid(self, num):
        """
        :param num: 8495710xxxx or 710xxxx
        :return: CustomerID(cid) or 0
        """
        if num.startswith('8495'):
            num = num[4:]

        return self.numbers.get(num, -1)
# end class Number710 (Citylan)


class FindCallRadius(object):
    """
    find call in db radius
    """
    DIFF_SEC_BEGIN = 60     # 30  diff second in begin talk
    DIFF_SEC_TALK = 10      # diff second in talk

    def __init__(self, dsn, table, prefix='A3'):
        """
        :param dsn: dsn-dict for access to db radius
        :param table: ex. Y2013M08
        """
        self.db = pymysql.Connect(**dsn)
        self.cur = self.db.cursor()
        self.table = table  # Y2013M09
        self.table_rad = Radius.table_rad(table, prefix=prefix)

    def __del__(self):
        self.cur.close()
        self.db.close()

    def getid(self, dt, fm, to, sec):
        """
        find call in radius data
        :param dt: date call
        :param fm: number from
        :param to: number to
        :param sec: second
        :return: id record from radius table (A3_2013_09.id)  or  0
        """

        fm_ = fm
        if fm.startswith('8495'):
            fm_ = fm[4:]

        to_ = '9' + to

        sql = "select id from radius.%s" \
              " where `fm`='%s'" \
              " and `to`='%s'" \
              " and ABS(TIMESTAMPDIFF(SECOND, dt, '%s')) < %s" \
              " and ABS(sec-%s) < %s" \
              % (self.table_rad, fm_, to_, dt, self.DIFF_SEC_BEGIN, sec, self.DIFF_SEC_TALK)

        self.cur.execute(sql)
        if self.cur.rowcount > 0:
            return self.cur._rows[0][0]
        else:
            sql = "select id from radius.%s" \
                  " where chx = 'IP ciscorj fm:%s to:%s'" \
                  " and ABS(TIMESTAMPDIFF(SECOND, dt, '%s')) < %s" \
                  " and ABS(sec-%s) < %s" \
                  % (self.table_rad, fm_, to_, dt, self.DIFF_SEC_BEGIN, sec, self.DIFF_SEC_TALK)
            self.cur.execute(sql)
            if self.cur.rowcount > 0:
                return self.cur._rows[0][0]

        return 0

    def getfrom(self, dt, to, sec):
        """
        find talk in radius (A1 or A3)
        :param dt: date call
        :param to: number to
        :param sec: second
        :return: (id, fm) from radius.A1_2014_05 if find or (0, '')
        """

        # select id, fm from A1_2014_05 where `to` like '%89859121232' and ABS(TIMESTAMPDIFF(SECOND, dt, '2014-05-22 00:38:30')) < 30 and ABS(sec-132) <= 10;

        sql= "select id, fm from {table} where `to` like '%{to}' and " \
             " ABS(TIMESTAMPDIFF(SECOND, dt, '{dt}')) < {diffbegin} and" \
             " ABS(sec-{sec}) <= {difftalk}".format(table=self.table_rad, to=to, dt=dt, sec=sec,
                                                    diffbegin=self.DIFF_SEC_BEGIN, difftalk=self.DIFF_SEC_TALK)

        self.cur.execute(sql)
        if self.cur.rowcount > 0:
            idd, fm = self.cur._rows[0]
            return idd, fm

        return 0, '-'

# end class FindCallRadius


class FindCallM200(object):
    """
    find call in db mp12 (ats m200)
    """
    DIFF_SEC_BEGIN = 60  # diff second in begin talk
    DIFF_SEC_TALK = 10   # diff second in talk

    def __init__(self, dsn_rem, dsn_loc, table, local=False, copy=True):
        """
        :param dsn_rem: dsn for access to db radius (remote)
        :param dsn_loc: dsn for access to db localhost:test.m200_Y2013M09 (local)
        :param table: ex. Y2013M09
        :param local: copy recordset from remote to local db
        """

        self.local = local
        try:
            if self.local:
                self.table = "test.m200_" + table   # test.m200_Y2013M09
                if copy:
                    self.db = self.copy_to_local(dsn_rem, dsn_loc, table_rem=table, table_loc=self.table)
                else:
                    self.db = pymysql.Connect(**dsn_loc)
                self.cur = self.db.cursor()

            else:
                self.table = "mp12." + table        # mp12.Y2013M09
                self.db = pymysql.Connect(**dsn_rem)
                self.cur = self.db.cursor()

        except pymysql.Error as e:
            log.warning('FindCallM200: ' + str(e))
            print(e)
            raise RuntimeError('Error in FindCallM200, local MySQL server not load!')

    def __del__(self):
        if hasattr(self, "cur"): self.cur.close()
        if hasattr(self, "db"): self.db.close()

    def copy_to_local(self, dsn_rem, dsn_loc, table_rem, table_loc):
        """
        Copy recordset from remote server to local
        :return: dbl Connect to local database
        """

        # local
        dbl = pymysql.Connect(**dsn_loc)
        curl = dbl.cursor()
        curl.execute("DROP TABLE IF EXISTS {table}".format(table=table_loc))
        curl.execute("CREATE TABLE {table} ("
                     " `id` int(7) unsigned DEFAULT 0, "
                     " `dt` datetime NOT NULL ,"
                     " `fm` char(12) NOT NULL DEFAULT '-' ,"
                     " `fmx` char(16) NOT NULL DEFAULT '-' ,"
                     " `to` char(20) NOT NULL DEFAULT '-' ,"
                     " `sec` int(7) NOT NULL DEFAULT '0' ,"
                     " `eq` char(8) NOT NULL DEFAULT '-' ,"
                     " PRIMARY KEY (`id`)"
                     " ) ENGINE=MyISAM".format(table=table_loc))

        # remote
        dbr = pymysql.Connect(**dsn_rem)
        curr = dbr.cursor()

        sql = "select id,dt,fm,fmx,`to`,sec,eq from mp12.`{table}` where sec>0 and length(`fm`)>=7".format(table=table_rem)


        curr.execute(sql)

        # copy
        for line in curr:
            idd, dt, fm, fmx, to, sec, eq = line
            sql = "insert into {table} (id, dt, fm, fmx, `to`, sec, eq) values " \
                "({id:d}, '{dt}', '{fm}', '{fmx}', '{to}', {sec:d}, '{eq}')".format(table=table_loc, id=idd, dt=dt,
                                                                                    fm=fm, fmx=fmx, to=to, sec=sec, eq=eq)

            curl.execute(sql)

        curr.close()
        dbr.close()
        curl.close()
        return dbl

    def getfrom(self, dt, to, sec):
        """
        find talk in mp12 (ats m200)
        :param dt: date call
        :param to: number to
        :param sec: second
        :return: (id, fm) from mp12.Y2013M09 if find or (0, '')
        """

        to_ = '9' + to
        to9818 = ''
        if to.startswith('9818'):
            to9818 = " or `to` like '%{last10}' ".format(last10=to[-10:])   # %9163336509

        sql = "select id, fm from %s" \
              " where `eq`='mp12'" \
              " and (`to`='%s' or `to`='%s')" \
              " %s " \
              " and ABS(TIMESTAMPDIFF(SECOND, dt, '%s')) < %s" \
              " and ABS(sec-%s) <= %s" \
              % (self.table, to, to_, to9818, dt, self.DIFF_SEC_BEGIN, sec, self.DIFF_SEC_TALK)

        self.cur.execute(sql)
        if self.cur.rowcount > 0:
            idd, fm = self.cur._rows[0]
            return idd, fm

        return 0, '-'

    def getfrom2(self, dt, fm_like, to):
        """
        find talk in mp12 (ats m200) WITHOUT(!) date call and second talk
        :param dt: date call
        :param fm_like: number fm ('81%')
        :param to: number to
        :return: (id, fm) from mp12.Y2013M09 if find or (0, '')
        """

        to_ = '9' + to
        to9818 = ''
        if to.startswith('9818'):
            to9818 = " or `to` like '%{last10}' ".format(last10=to[-10:])   # %9163336509


        sql = "select id, fm from {table}  where `eq`='mp12' and (`to`='{to}' or `to`='{to_}')" \
              " {to9818} " \
              " and (`fm` like '{fm}')".format(table=self.table, to=to, to_=to_, fm=fm_like, to9818=to9818)

        self.cur.execute(sql)
        if self.cur.rowcount > 0:
            idd, fm = self.cur._rows[0]
            return idd, fm

        return 0, '-'

    def getid(self, dt, fm, to, sec):
        """
        find talk in mp12 (ats m200)
        :param dt: date call
        :param to: number to
        :param sec: second
        :return: id from mp12.Y2013M09 if find or 0
        """

        to_ = '9' + to

        sql = "select id, fm from %s" \
              " where (`fm`='%s' or `fmx`='%s')" \
              " and (`to`='%s' or `to`='%s')" \
              " and ABS(TIMESTAMPDIFF(SECOND, dt, '%s')) < %s" \
              " and ABS(sec-%s) <= %s" \
              % (self.table, fm, fm, to, to_, dt, self.DIFF_SEC_BEGIN, sec, self.DIFF_SEC_TALK)

        self.cur.execute(sql)
        if self.cur.rowcount > 0:
            return self.cur._rows[0][0]

        return 0
# end class FindCallM200


class M200(object):
    """ Add records from m200(mp12 db) to billing """

    def __init__(self, dsn, bill, table, numb, stat):
        """
        :param dsn: dsn smg
        :param bill: object Billing
        :param table: table (Y2013M10)
        :param numb: object Numbers
        :param stat: object Stat
        """
        self.db = pymysql.Connect(**dsn)
        self.cur = self.db.cursor()
        self.table = table
        self.bill = bill
        self.stat = stat
        self.numb = numb

    def __del__(self):
        self.cur.close()
        self.db.close()

    def add(self, info='-', eq='-', where=''):
        """
        Add records from m200(mp12) to billing:
        :param info: info for logging
        :param eq: (mp12)
        :param where: add filter, ex. 'sec > 0'
        """
        t1 = time.time()

        sql = "select id, dt, fm, fmX, `to`, toX, sec, eq, l1, l2, link, pr from mp12.`{table}` where f1='-'".format(table=self.table)
        if where: sql += " and " + where

        self.cur.execute(sql)
        step, current, records, update = (0, -1, self.cur.rowcount, 0)
        if self.cur.rowcount > 0:
            for idd, dt, fm, fmx, to, tox, sec, eq, l1, l2, link, cause in self.cur:
                step += 1
                current = Func.progress(step, records, current)
                sts = self.stat.getsts(to)
                # dnw = self.stat.getdnw(dt)
                # cid = self.numb.getcid(fm)

                sql = "update mp12.`{table}` set f1='+' where id={id:d}".format(table=self.table, id=idd)
                self.cur.execute(sql)

                self.bill.insert(self.table, dt=dt, fm=fm, to=to, fmx=fmx, tox=tox, sec=sec, eq=eq, link=link,
                                     cause=cause, l1=l1, l2=l2, stat='-', sts=sts, b='-', ok='-', cid=0, eqid=idd)
                update += 1

        itog_log(info=info, step=step, update=update, t1=t1, t2=time.time())
# end class M200


class Smg(object):
    """ Add records from SMG to billing ( call 626xxxx and more) """

    def __init__(self, dsn, bill, table, numb, stat):
    
        """
        :param dsn: dsn smg
        :param bill: object Billing
        :param table: table (Y2013M09)
        :param numb: object Numbers
        :param stat: object Stat
        """
        self.db = pymysql.Connect(**dsn)
        self.cur = self.db.cursor()
        self.table = table
        self.table_rad = Radius.table_rad(table)
        self.bill = bill
        # self.fradius = fradius
        self.stat = stat
        self.numb = numb

    def __del__(self):
        self.cur.close()
        self.db.close()

    def add(self, src_num, dtr_trank, info='-', eq='-', where='', nocid=False, op='-'):
        """
        Add records from SMG to billing: numbers src_num to direction dtr_trank
        :param src_num: example ('8495626%', '626%') or ('710%','627%')
        :param dtr_trank: example ('mgts_', 'mts') or ('megafon')
        :param info: info for logging
        :param eq: (smg_710, smg_626)
        :param where: add filter, ex. 'id=100'
        :param nocid: True - for load cid=0
        :param op: operator (m=MTS,f=MEGAFON,g=VM GUP,x=mgts,r=RSS)
        """
        t1 = time.time()
        # select * from smg.Y2013M09 where fm like '81_____'  and sec>0 and (dtr like 'mgts%' or dtr like 'megafon' or dtr like 'mts' or dtr like 'isdx%' or dtr like 'rss') ;

        # oper = dict(mts='m', mgts1='m', mgts2='m', mgts3='m', mgts4='m', megafon='f', mtt='a', isdx1='m', isdx2='m', rss='m')
        oper = dict(mts='m', mts2='m', mgts1='x', mgts2='x', mgts3='x', mgts4='x', megafon='f', mtt='a', isdx1='r', isdx2='r', rss='r')
        status_smg = dict(mts='MG', mts2='MG', megafon='MG', mtt='MG', mgts1='GD', mgts2='GD', mgts3='GD', mgts4='GD', isdx1='GD', isdx2='GD', rss='GD')
        status_m200 = dict(mts='M', mts2='M', megafon='M', mtt='M', mgts1='G', mgts2='G', mgts3='G', mgts4='G', isdx1='G', isdx2='G', rss='G')

        # filter src numbers
        snum = "fm like '{0}'".format(src_num[0])
        for n in src_num[1:]:
            snum += " or fm like '{0}'".format(n)

        #filter dtr trank
        dtrank = "dtr like '{0}'".format(dtr_trank[0])
        for n in dtr_trank[1:]:
            dtrank += " or dtr like '{0}'".format(n)

        sql = " select `id`,`dt`,`fm`,`fmx`,`to`,`tox`,`sec`,`str`,`dtr`,`cause`,f1,f2 from smg.`{table}` where " \
              " f1='-' and ({snum}) and ({dtrank}) and sec>0".format(table=self.table, snum=snum, dtrank=dtrank)

        if where: sql += " and " + where

        self.cur.execute(sql)
        step, current, records, update = (0, -1, self.cur.rowcount, 0)
        bar = Progressbar(info=info, maximum=self.cur.rowcount)

        if self.cur.rowcount > 0:
            for idd, dt, fm, fmx, to, tox, sec, str_, dtr, cause, f1, f2 in self.cur:
                step += 1
                # current = Func.progress(step, records, current)
                bar.update_progress(step)
                stat = status_smg.get(dtr, '-')
                sts = self.stat.getsts(to)
                p = oper.get(dtr, '-')
                cid = self.numb.getcid(fm)
                if cid > 0 or nocid:
                    # rid = self.fradius.getid(dt, fm, to, sec)     # find radius.id
                    rid = 0
                    sql = "update smg.`{table}` set f1='+', f2='{stat}', rid={rid:d}, cid={cid:d} " \
                        " where id={id:d}".format(table=self.table, stat=stat, rid=rid, cid=cid, id=idd)
                    self.cur.execute(sql)
                    link = 'smg_' + p
                    self.bill.insert(self.table, dt=dt, fm=fm, to=to, fmx=fmx, tox=tox, sec=sec, p=p, op=op, eq=eq,
                                     eqid=idd, link=link, cause=cause, l1=str_, l2=dtr, stat=status_m200.get(dtr, '-'),
                                     sts=sts, b='+', ok='-')
                    update += 1

        if step > 0:
            Progressbar.go_new_line()

        itog_log(info=info, step=step, update=update, t1=t1, t2=time.time())

    def findbill(self, fm200, info='-', where=''):
        """
        find records in billing and set smg.bid = mp12.id
        :param fm200: object FindCallM200
        """
        t1 = time.time()
        if where:
            where = "where {where}".format(where=where)
        sql = "select id, dt, fm, `to`, sec, stat from {table} {where}".format(table=self.table, where=where)
        self.cur.execute(sql)
        step, current, records, update = (0, -1, self.cur.rowcount, 0)
        for line in self.cur:
            step += 1; current = Func.progress(step, records, current)
            idd, dt, fm, to, sec, stat = line
            bid = fm200.getid(dt, fm, to, sec)
            if bid > 0:
                sql = "update {table} set bid={bid:d} where id={id:d}".format(table=self.table, bid=bid, id=idd)
                self.cur.execute(sql)
                update += 1

        itog_log(info=info, step=step, update=update, t1=t1, t2=time.time())
# end class Smg


class Smg642(object):
    """ Add records from SMG642 to billing ( call 642xxxx ) """

    def __init__(self, dsn, bill, table, numb, stat):
    
        """
        :param dsn: dsn smg
        :param bill: object Billing
        :param table: table (Y2013M09)
        :param numb: object Numbers
        :param stat: object Stat
        """
        self.dsn = dsn
        self.db = pymysql.Connect(**dsn)
        self.cur = self.db.cursor()
        self.table = table
        # self.table_rad = Radius.table_rad(table)
        self.bill = bill
        # self.fradius = fradius
        self.stat = stat
        self.numb = numb

    def __del__(self):
        self.cur.close()
        self.db.close()

    def add(self, src_num, dtr_trank, operator='like', info='-', eq='-', where='', nocid=False, op='-'):
        """
        add('%642____%', 'beeline', info='SMG2_bee', eq='?', op='b')
        Add records from SMG642 to billing: numbers src_num to direction dtr_trank
        2021-12-03: add CTS
        fm rlike '^(7|8)?495221....' or fm rlike '^(7|8)?495236....' or fm rlike '^(7|8)?495730....' or fm rlike '^(7|8)?495739....');
        :param operator: 'like' | 'rlike' for src_num
        :param src_num: example for operator='like'  : ('8495626%', '626%') or ('710%','627%')
        :param src_num: example for operator='rlike' : ('^(7|8)?495221....', '^(7|8)?495236....', '^(7|8)?495730....')
        :param dtr_trank: example ('mgts_', 'mts') or ('megafon')
        :param info: info for logging
        :param eq: (smg_710, smg_626)
        :param where: add filter, ex. 'id=100'
        :param nocid: True - for load cid=0
        :param op: operator (m=MTS,f=MEGAFON,g=VM GUP,x=mgts,r=RSS)
        """

        base = 'smg2'
        t1 = time.time()
        oper = dict(beeline='b', bee_rss='b', mts='m', mgts1='x', mgts2='x', mgts3='x', mgts4='x', megafon='f', mtt='a', isdx1='r', isdx2='r', rss='r')

        # filter src numbers
        # snum = "fm like '{0}'".format(src_num[0])
        # for n in src_num[1:]:
        #     snum += " or fm like '{0}'".format(n)

        # fm rlike '^(7|8)?495221....' or fm rlike '^(7|8)?495236....' or fm rlike '^(7|8)?495730....' or fm rlike '^(7|8)?495739....');

        snum = "fm {0} '{1}'".format(operator, src_num[0])
        for n in src_num[1:]:
            snum += " or fm {0} '{1}'".format(operator, n)

        # filter dtr trank
        dtrank = "dtr like '{0}'".format(dtr_trank[0])
        for n in dtr_trank[1:]:
            dtrank += " or dtr like '{0}'".format(n)

        sql = " select `id`,`dt`,`fm`,`fmx`,`to`,`tox`,`sec`,`str`,`dtr`,`cause`,f1,f2 from {base}.`{table}` where " \
              " f1='-' and sec>0 and ({snum}) and ({dtrank})".format(base=base, table=self.table, snum=snum, dtrank=dtrank)

        if where: sql += " and " + where

        self.cur.execute(sql)
        step, current, records, update = (0, -1, self.cur.rowcount, 0)
        bar = Progressbar(info=info, maximum=self.cur.rowcount)

        conn_update = pymysql.Connect(**self.dsn)
        cur_update = conn_update.cursor()

        if self.cur.rowcount > 0:
            for idd, dt, fm, fmx, to, tox, sec, str_, dtr, cause, f1, f2 in self.cur:
                step += 1
                # current = Func.progress(step, records, current)
                bar.update_progress(step)
                sts = self.stat.getsts(to, tox=tox)
                stat = self.stat.getMWSZG(sts=sts)

                p = oper.get(dtr, '-')
                cid = self.numb.getcid(fm)
                if cid > 0 or nocid:

                    sql = "update {base}.`{table}` set f1='+', f2='{stat}', stat='{sts}', cid={cid:d} " \
                          " where id={idd}".format(base=base, table=self.table, stat=stat, sts=sts, cid=cid, idd=idd)

                    cur_update.execute(sql)

                    link = 'smg2_' + p
                    #if cid == 58:
                    #    fm = fmr    # (internal number sport)

                    self.bill.insert(self.table, dt=dt, fm=fm, to=to, fmx=fmx, tox=tox, sec=sec, p=op, op=op, eq=eq,
                                     eqid=idd, link=link, cause=cause, l1=str_, l2=dtr, stat=stat,
                                     sts=sts, b='+', ok='-')
                    update += 1

        if step > 0:
            Progressbar.go_new_line()

        itog_log(info=info, step=step, update=update, t1=t1, t2=time.time())

    def setstat(self, where=None, info='-'):
        """
        set field `stat`
        установка поля stat для отобранных записей по where
        setstat(where="`f1`='+' AND `dtr` = 'mts' AND `stat`='-'", info='update_stat'
        """
        base = 'smg2'
        t1 = time.time()
        cursor = self.cur

        sql = "SELECT `id`, `to`,`tox` FROM {base}.`{table}`".format(base=base, table=self.table)
        if where:
            sql += "WHERE {where}".format(where=where)

        cursor.execute(sql)
        step, current, records, update = (0, -1, cursor.rowcount, 0)
        if cursor.rowcount > 0:
            for idd, to, tox in cursor:
                step += 1
                current = Func.progress(step, records, current)
                sts = self.stat.getsts(to, tox=tox)
                stat = self.stat.getMWSZG(sts=sts)
                sql = "update {base}.`{table}` set `f2`='{stat}', `stat`='{sts}' where `id`={id:d}".\
                    format(base=base, table=self.table, stat=stat, sts=sts, id=idd)
                cursor.execute(sql)
                update += 1

        itog_log(info=info, step=step, update=update, t1=t1, t2=time.time())

    def add2(self, where='', info='-', eq='-', nocid=False, op='-', p='-', f3='-', p2='+'):
        """
        Add records from SMG642 to billing:
        ! mark field f3

        add2(where="fmx like '%642' and (fm like '%627____' or fm like '%710____') and dtr='mts'", info='smg2_710f', eq='smg2_710f', op='q')
        :param where: ex. fmx like '%642____'
        :param info: info for logging
        :param eq: (smg_710, smg_626, smg2_710f
        :param nocid: True - for load cid=0
        :param op: operator (m=MTS(626),f=MEGAFON,g=VM GUP,x=mgts,r=RSS, q=MTS(642))
        :param p: operator (p=f)- double record for megafon
        :param f3: flag, f3=f - record add to billing
        :param p2: marker in bill:  '#' = double record in billing; '+' = add 710xxxx from smg2 (asterisk->asterisk)
        """
        base = 'smg2'
        t1 = time.time()
        # oper = dict(beeline='b', bee_rss='b', mts='m', mgts1='x', mgts2='x', mgts3='x', mgts4='x', megafon='f', mtt='a', isdx1='r', isdx2='r', rss='r')

        sql = " select `id`,`dt`,`fm`,`fmx`,`to`,`tox`,`sec`,`str`,`dtr`,`cause`,f1,f2 from {base}.`{table}` where ({where})" \
              " and f3='-' and sec>0".format(base=base, table=self.table, where=where)

        self.cur.execute(sql)
        step, current, records, update = (0, -1, self.cur.rowcount, 0)
        bar = Progressbar(info=info, maximum=self.cur.rowcount)

        conn_update = pymysql.Connect(**self.dsn)
        cur_update = conn_update.cursor()

        if self.cur.rowcount > 0:
            for idd, dt, fm, fmx, to, tox, sec, str_, dtr, cause, f1, f2 in self.cur:
                step += 1
                # current = Func.progress(step, records, current)
                bar.update_progress(step)
                sts = self.stat.getsts(to, tox=tox)
                stat = self.stat.getMWSZG(sts=sts)
                # p = oper.get(dtr, '-')
                cid = self.numb.getcid(fm)
                if cid > 0 or nocid:

                    sql = "update {base}.`{table}` set f3='{f3}', f2='{stat}', stat='{sts}', cid={cid:d} " \
                          " where id={id:d}".format(base=base, table=self.table, f3=f3, stat=stat, sts=sts, cid=cid, id=idd)

                    cur_update.execute(sql)

                    link = 'smg2_' + p

                    self.bill.insert(self.table, dt=dt, fm=fm, to=to, fmx=fmx, tox=tox, sec=sec, p=p, op=op, eq=eq,
                                     eqid=idd, link=link, cause=cause, l1=str_, l2=dtr, stat=stat,
                                     sts=sts, b='+', ok='-', p2=p2)    # p2=# - marker double record; p2=+ (asterisk->asterisk for 710*)
                    update += 1

        if step > 0:
            Progressbar.go_new_line()

        itog_log(info=info, step=step, update=update, t1=t1, t2=time.time())

# end class Smg642


class Radius(object):
    """
    Add records to billing from radius.A3
    """

    def __init__(self, dsn, bill, table, numb, stat):
        """
        :param dsn: dsn radius
        :param bill: object Billing
        :param table: table (Y2013M09)
        :param numb: object Numbers
        """
        self.db = pymysql.Connect(**dsn)
        self.cur = self.db.cursor()
        self.table = table
        self.table_rad = Radius.table_rad(table, prefix='A3')
        self.table_rad_a1 = Radius.table_rad(table, prefix='A1')
        self.bill = bill
        self.numb = numb
        self.stat = stat
        self.re_fm = re.compile(r'.*fm:(\d{3,}\s).*')
        self.re_to = re.compile(r'.*to:(\d{3,})\s*.*')

    def __del__(self):
        self.cur.close()
        self.db.close()

    @staticmethod
    def table_rad(table, prefix='A3'):
        return "%s_%s_%s" % (prefix, table[1:5], table[6:8])  # Y2013M09 => A3_2013_09

    def fmxtox(self, txt):
        """
        Obtain fmx and tox
        :return: tuple (fmx, tox)
        """
        fmx, tox = ('-', '-')

        m = self.re_fm.match(txt)
        if m:
            fmx = m.group(1)

        m = self.re_to.match(txt)
        if m:
            tox = m.group(1)

        if fmx.startswith('499'):   # 499642xxxx
            fmx = fmx[3:]

        return fmx, tox

    def add_city(self, info='-', eq='-', op='-'):
        """
        Add records Citylan from radius to billing (mp12.table)
        :param info: info for logging
        :param eq: c3_city (c-Cisco, 3-radius.A3, city-citylan)
        :param op: operator (m=MTS,f=MEGAFON,g=VM GUP,x=mgts,r=RSS)
        :return:
        """
        t1 = time.time()
        sql = "select id, dt, fm, `to`, sec, cause from radius.{0}" \
            " where f99='-' and sec>0 and `chx` like 'IP citylan%' and fm like '_______' ".format(self.table_rad)
        self.cur.execute(sql)
        step, current, update, records = (0, 0, 0, self.cur.rowcount)
        for line in self.cur:
            step += 1
            current = Func.progress(step, records, current)
            idd, dt, fm, to, sec, cause = line
            cid = self.numb.getcid(fm)
            sts = self.stat.getsts(to)
            if cid > 0:
                sql = "update radius.{table} set f99='c' where id={id:d}".format(table=self.table_rad, id=idd)
                self.cur.execute(sql)
                self.bill.insert(self.table, dt=dt, fm=fm, to=to, fmx='-', tox='-', sec=sec, p='c', op=op, eq=eq,
                                 eqid=idd, link='c3_cit', cause=int(cause, 16), l1='radius', l2='citylan',
                                 stat='-', sts=sts, b='+', ok='-')
                update += 1
        itog_log(info=info, step=step, update=update, t1=t1, t2=time.time())

    def add_beelineA3(self, info='-', eq='-', op='-'):
        """
        Add records Beeline/RSS from radius(A3) to billing (mp12.table)
        :param info: info for logging
        :param eq: r3_b (r3-radius.A3, b-beeline)
        :param op: operator (m=MTS,f=MEGAFON,g=VM GUP,x=mgts,r=RSS)
        :return:
        """
        t1 = time.time()
        # not like 626%  -  now 626% add from smg
        # not like '%to:981159%' - autoresponders
        # to:9818 -> beeline
        sql = " select id, dt, fm, `to`, sec, cause, chx from radius.{0}" \
              " where f99='-' and sec>0 and chx like 'IP ciscorj%to:9818%' and chx not like '%to:981159%' and length(fm)=7" \
              " and fm not like '626%' and `to` <>'' order by dt".format(self.table_rad)
        self.cur.execute(sql)
        step, current, update, records = (0, 0, 0, self.cur.rowcount)
        for line in self.cur:
            step += 1
            current = Func.progress(step, records, current)
            idd, dt, fm, to, sec, cause, chx = line
            cid = self.numb.getcid(fm)
            sts = self.stat.getsts(to)
            if cid > 0:
                sql = "update radius.{table} set f99='b' where id={id:d}".format(table=self.table_rad, id=idd)
                self.cur.execute(sql)
                fmx, tox = self.fmxtox(chx)
                self.bill.insert(self.table, dt=dt, fm=fm, to=to, fmx=fmx, tox=tox, sec=sec, p='b', op=op, eq=eq,
                                 eqid=idd, link='r->Mor', cause=int(cause, 16), l1='radius', l2='beeline',
                                 stat='-', sts=sts, b='+', ok='-')
                update += 1
        itog_log(info=info, step=step, update=update, t1=t1, t2=time.time())

    def add_beelineA1(self, info='-', eq='-', op='-'):
        """
        Add records Beeline/RSS from radius(A1) to billing (bill.table)
        :param info: info for logging
        :param eq: r1_b (r1-radius.A1, b-beeline)
        :param op: operator (m=MTS,f=MEGAFON,g=VM GUP,x=mgts,r=RSS)
        :return:
        """
        t1 = time.time()

        # %642____ - all 642
        # 81252__  - sport (internal for 642-x)
        # chx like 'ISDN 0/1/1%'  -> output beeline

        sql = " select id, dt, fm, `to`, sec, cause, chx from radius.{table}" \
              " where (f99='-' and sec>0) and (fm like '%642____' or fm like '81252__')" \
              " and (dt < '20140530')" \
              " order by dt" \
              "".format(table=self.table_rad_a1)

            # " and (dt < '20140530')" \
            # " and chx like 'ISDN 0/1/1%' order by dt" \

        self.cur.execute(sql)
        step, current, update, records = (0, 0, 0, self.cur.rowcount)
        for line in self.cur:
            step += 1
            current = Func.progress(step, records, current)
            idd, dt, fm, to, sec, cause, chx = line
            cid = self.numb.getcid(fm)
            sts = self.stat.getsts(to)
            fmx, tox = ('-','-')
            if cid > 0:
                sql = "update radius.{table} set f99='b' where id={id:d}".format(table=self.table_rad_a1, id=idd)
                self.cur.execute(sql)
                fmx, tox = self.fmxtox(chx)
                if fm.startswith('8499642') and fmx.startswith('642'):     # 8499642xxxx  <--> 642xxxx (fm <-> fmx)
                    fm, fmx = fmx, fm

                self.bill.insert(self.table, dt=dt, fm=fm, to=to, fmx=fmx, tox=tox, sec=sec, p='b', op=op, eq=eq,
                                 eqid=idd, link='r->Mor', cause=int(cause, 16), l1='radius', l2='beeline',
                                stat='-', sts=sts, b='+', ok='-')
                update += 1

            # print "{fm}/{fmx} -> {to}/{tox} ({sts}) ({cid})".format(fm=fm, fmx=fmx, to=to, tox=tox, sts=sts, cid=cid)
        itog_log(info=info, step=step, update=update, t1=t1, t2=time.time())

# end class Radius


class ReplaceNumber(object):
    """
    Replace number in table billing
    example: 6269000 on 8124xx
    example: 642xxxx on 8125xx
    """

    def __init__(self, dsn, table, numb, fm200):
        """
        :param table: table, example Y2013M09
        :param numb: object Numbers
        """
        self.db = pymysql.Connect(**dsn)
        self.cur = self.db.cursor()
        self.table = table
        self.numb = numb
        self.m200 = fm200

    def __del__(self):
        self.cur.close()
        self.db.close()

    def num642_sport(self, info='-'):
        """
        Replace numbers 642xxxx on inter number(8125xx) for SeverPort(58)
        :param info: info for logging
        :return: count replacing numbers
        """
        t1 = time.time()
        n642 = Number642Cust58(numb)
        where = "fm in ({num}) and eq<>'mp12'".format(num=n642.numstr)
        sql = "select id, dt, fm, `to`, sec FROM bill.{table} where {where}".format(table=self.table, where=where)
        self.cur.execute(sql)
        step, current, update, records = (0, 0, 0, self.cur.rowcount)
        for line in self.cur:
            step += 1
            current = Func.progress(step, records, current)
            idd, dt, fm, to, sec = line
            id_m200, fm_m200 = self.m200.getfrom(dt, to, sec)    # get replace number: 8125xx/642xxx
            if fm_m200 != '-':
                sql = "update bill.{table} set fm={fm}, fm2={fm}, fmX={fmx}, pr='164' where id={idd}".format(
                    table=self.table, fmx=fm, fm=fm_m200, idd=idd)
                self.cur.execute(sql)
                update += 1

        itog_log(info=info, step=step, update=update, t1=t1, t2=time.time())

    def num642_sport2(self, info='-', where=''):
        """
        Replace numbers 642xxxx on inter number(8125xx) for SeverPort(58)
        find ONLY fm -> to WITHOUT(!) date
        :param info: info for logging
        :return: count replacing numbers
        """
        t1 = time.time()
        n642 = Number642Cust58(numb)
        wh = "fm in ({num}) and eq<>'mp12'".format(num=n642.numstr)
        if where:
            wh += " and ({wh}) ".format(wh=where)
        sql = "select id, dt, fm, `to`, sec FROM bill.{table} where {wh}".format(table=self.table, wh=wh)
        self.cur.execute(sql)
        step, current, update, records = (0, 0, 0, self.cur.rowcount)
        for line in self.cur:
            step += 1
            current = Func.progress(step, records, current)
            idd, dt, fm, to, sec = line
            id_m200, fm_m200 = self.m200.getfrom2(dt, fm_like='81%', to=to)   # get replace number: 8125xx/642xxx
            if fm_m200 != '-':
                sql = "update bill.{table} set fm={fm}, fm2={fm}, fmX={fmx}, pr='164' where id={idd}".format(
                    table=self.table, fmx=fm, fm=fm_m200, idd=idd)
                self.cur.execute(sql)
                update += 1

        itog_log(info=info, step=step, update=update, t1=t1, t2=time.time())

    def num626(self, fm, info='-'):
        """
        Replace numbers 6269000 on inter number(8124xx) for HMSZ(273)
        Replace numbers 6269082 on inter number(8120[12345]xx) for StroyCity(928)
        :param info: info for logging
        :return: count replacing numbers
        """
        t1 = time.time()
        # where = " fm = '84956269000' and eq<>'mp12' "
        where = " fm = '{fm}' and eq<>'mp12' ".format(fm=fm)
        sql = "select id, dt, fm, `to`, sec FROM bill.{table} where {where}".format(table=self.table, where=where)
        self.cur.execute(sql)
        step, current, update, records = (0, 0, 0, self.cur.rowcount)
        for line in self.cur:
            step += 1
            current = Func.progress(step, records, current)
            idd, dt, fm, to, sec = line
            id_m200, fm_m200 = self.m200.getfrom(dt, to, sec)    # get replace number: 8125xx/642xxx
            if fm_m200 != '-':
                sql = "update bill.{table} set fm={fm}, fm2={fm}, fmX={fmx}, pr='164' where id={idd}".format(
                    table=self.table, fmx=fm, fm=fm_m200, idd=idd)
                self.cur.execute(sql)
                update += 1

        itog_log(info=info, step=step, update=update, t1=t1, t2=time.time())

    def num626_prim(self, info='-', where=''):
        """
        Replace numbers 84956269082  on inter number(8120[12345]xx) for StroyCity(928)
        find ONLY fm -> to WITHOUT(!) date
        :param info: info for logging
        :return: count replacing numbers
        """
        t1 = time.time()
        wh = " eq<>'mp12' and ({wh}) ".format(wh=where)
        sql = "select id, dt, fm, `to`, sec FROM bill.{table} where {wh}".format(table=self.table, wh=wh)
        self.cur.execute(sql)
        step, current, update, records = (0, 0, 0, self.cur.rowcount)
        for line in self.cur:
            step += 1
            current = Func.progress(step, records, current)
            idd, dt, fm, to, sec = line
            id_m200, fm_m200 = self.m200.getfrom2(dt, fm_like='81%', to=to)    # get replace number: 8120xx/626xxx
            if fm_m200 != '-':
                sql = "update bill.{table} set fm={fm}, fm2={fm}, fmX={fmx}, pr='164' where id={idd}".format(
                    table=self.table, fmx=fm, fm=fm_m200, idd=idd)
                self.cur.execute(sql)
                update += 1

        itog_log(info=info, step=step, update=update, t1=t1, t2=time.time())
# end class ReplaceNumber


def setstat_smg(info, dsn, table, stat, all=True):
    """ set stat (vz,mg,mn,gd) in smg.Y2013M10
    """
    db = pymysql.Connect(**dsn)
    cur = db.cursor()
    t1 = time.time()
    sql = "select id, `to` from {table} where dtr='mts'".format(table=table)

    if not all:
        sql += " and stat = '-' "
    cur.execute(sql)
    step, current, records, update = (0, -1, cur.rowcount, 0)
    for line in cur:
        step += 1; current = Func.progress(step, records, current)
        idd, to = line
        sql = "update {table} set stat='{stat}' where id={idd}".format(table=table, stat=stat.getstat(to), idd=idd)
        cur.execute(sql)
        update += 1

    cur.close()
    db.close()
    itog_log(info=info, step=step, update=update, t1=t1, t2=time.time())


def setstat_bill(info, dsn, table, stat, all=True, where=''):
    """ set sts (vz,mg,mn,gd) in bill.Y2013M10
    """
    db = pymysql.Connect(**dsn)
    cur = db.cursor()
    t1 = time.time()
    sql = "select id, `to` from {table} where id>0".format(table=table)
    if not all:
        sql += " and sts = '-' "
    if where:
        sql += " and (" + where + ")"
    cur.execute(sql)
    step, current, records, update = (0, -1, cur.rowcount, 0)
    for line in cur:
        step += 1; current = Func.progress(step, records, current)
        idd, to = line
        sql = "update {table} set sts='{sts}' where id={idd}".format(table=table, sts=stat.getsts(to), idd=idd)
        cur.execute(sql)
        update += 1

    cur.close()
    db.close()
    itog_log(info=info, step=step, update=update, t1=t1, t2=time.time())


class TarMTC(object):
    """
    tariff of MTC
    """
    def __init__(self, dsn, tabcode, tabtar, stat, cdef, custdefault):
        """
         :param dsn: dictionary dsn     (dsn_tar)
         :param tabcode: table of code  (komstarCode)
         :param tabtar: table of tariff (komstarTar)
         :param stat: object Stat
         :param cdef: object Codedef
         #:param custdefault: customer default for tariff (953)
        """
        self.dsn = dsn
        self.tabcode = tabcode  # tarif.komstarCode
        self.tabtar = tabtar    # tarif.komstarTar
        self.stat = stat        # object Stat
        self.cdef = cdef        # object Codedef
        self.names = dict()     # names['797'] = dict(name='N.Novgorod', zona=3, type='MG', tara=2.34)
        self.codes = list()     # sorted list code+nid: ('7831;797') : 7831-tel.code; 797-nid, names['797']['tara']
        self.tariff = dict()    # cust tariff: cost1min = tariff['nid_cid']
        self.tarspsmg = dict()  # cust tariff sps mg: cost1min = tarspsmg['zona_cid']  (ex. tarspsmg['273_6']=7.5)
        self.taraspsmg = dict() # agent tariff sps mg: taraspsmg['zona']=dict(tara=1.1, name='Россия (моб) 4 зона')
        self.custdefault = custdefault  # cust default
        self._read_()           # read: .names, .tariff, .codes

    def _read_(self):
        """ read tariff MTC to: self.names, self.codes, self.tariff
        """
        db = pymysql.Connect(**self.dsn)
        cur = db.cursor()
        # codes
        sql = "select `nid`, `type`, `name`, `zona`, `code1`, `code2`, `tar` from {table}".format(table=self.tabcode)
        cur.execute(sql)
        for line in cur:
            nid, type, name, zona, code1, code2, tar = line
            self.names[str(nid)] = dict(name=name.encode('utf8'), zona=zona, type=type, tara=tar, code1=code1)
            arrcode = Func.arraycode(code1, code2)
            for x in arrcode:
                st = "{code};{nid}".format(code=x, nid=nid)
                self.codes.append(st)

        # tariff
        sql = "select `nid`, `cid`, `tar` from {table}".format(table=self.tabtar)
        cur.execute(sql)
        for line in cur:
            nid, cid, tar = line
            self.tariff[self.join_nidcid(nid, cid)] = tar

        # agent tariff (tara) mg for sps
        sql = "select c.`name`, c.`zona`, c.`tar` tara from `{tabcode}` c  WHERE c.regkom like 'Rossia_mob-%'".format(
            tabcode=self.tabcode)
        cur.execute(sql)
        for line in cur:
            name, zona, tara = line
            self.taraspsmg[str(zona)] = dict(tara=tara, name=name)

        # cust tariff mg for sps
        sql = "select t.cid, c.`name`, c.zona, c.`tar` tara, t.`tar` from `{tabcode}` c , `{tabtar}` t" \
              " WHERE c.nid=t.nid and c.regkom like 'Rossia_mob-%'".format(tabcode=self.tabcode, tabtar=self.tabtar)
        cur.execute(sql)
        for line in cur:
            cid, name, zona, tara, tar  = line
            self.tarspsmg[self.join_zonacid(zona, cid)] = tar


        cur.close()
        db.close()

        self.codes.sort()

        # self.prn_names()
        # self.prn_tarcust()
        # self.prn_codes()
        print("-----------------------------------------------")
        # self.prn_vz()

    def prn_names(self, fname=None):
        """
        print (nid,name,code1,tara)
        if fname: print to fname
        """
        f = None
        if fname: f = open(fname, "wt")
        n = 0
        for k, v in self.names.items():
            q = self.names[k]
            st = "nid:{nid} {{ code1:{code1}, tara:{tara}, name:'{name:s}', type={type}, zona={zona} }}".format(
                nid=k, code1=q['code1'], name=q['name'], tara=q['tara'], type=q['type'], zona=q['zona'])
            print(st)
            if f: f.write(st + "\n")
            n += 1

        st = "itogo:{n} records".format(n=n)
        print(st)
        if f:
            f.write(st + "\n")
            f.close()

    def prn_vz(self):
        self.cdef.print_codevz()

    def prn_codes(self, fname=None):
        """
        print codes('code;nid', 'code2;nid2', ...)
        if fname: print to fname
        """
        f = None
        if fname: f = open(fname, "wt")
        n = 0
        cd = self.codes
        for x in self.codes:
            code, nid = x.split(';')
            st = "code={code}, nid={nid}, name='{name}'({code1}), zona={zona}, tara={tara}".format(
                code=code, nid=nid, name=self.names[nid]['name'], code1=self.names[nid]['code1'],
                zona=self.names[nid]['zona'], tara=self.names[nid]['tara'])
            print(st)
            if f: f.write(st + "\n")
            n += 1

        st = "itogo:{n} records".format(n=n)
        print(st)
        if f:
            f.write(st + "\n")
            f.close()

    def prn_tarcust(self, fname=None):
        """
        print customers tariff
        if fname: print to fname
        """
        n = 0
        f = None
        if fname: f = open(fname, "wt")

        tar = self.tariff
        for k in list(tar.keys()):
            nid, cid = self.split_nidcid(k)
            st = "nid={nid}; cid={cid}; tar={tar}; name='{name:s}'({code1})".format(
                nid=nid, cid=cid, tar=tar[k], name=self.names[nid]['name'], code1=self.names[nid]['code1'])
            print(st)
            if f: f.write(st + "\n")
            n += 1

        st = "itogo:{n} records".format(n=n)
        print(st)
        if f:
            f.write(st + "\n")
            f.close()

        print("tar=", self.gettar(nid=241, cid=549))

    def join_nidcid(self, nid, cid):
        """ nid, cid; return = 'nid_cid'
        """
        return str(nid) + "_" + str(cid)

    def split_nidcid(self, val):
        """ val='nid_cid'; return (nid,cid)
        """
        return val.split('_')   # (nid, cid)

    def join_zonacid(self, zona, cid):
        """ zona, cid; return = 'zona_cid'
        """
        return str(zona) + "_" + str(cid)

    def split_zonacid(self, val):
        """ val='zona_cid'; return (zona,cid)
        """
        return val.split('_')   # (zona, cid)

    def gettar(self, nid, cid):
        """
        return cost 1 min for direction: nid and custid
        """
        k = self.join_nidcid(nid, cid)
        return self.tariff.get(k, 0)

    def gettara(self, nid):
        """
        return agent cost 1 min for direction: nid
        """

        # self.names[nid]['tara']
        x = self.names.get(nid, None)
        if x:
            return x['tara']
        else:
            return 0

    def gettarmgs(self, zona, cid):
        """
        return cost 1 min for sps mg
        """
        k = self.join_zonacid(zona, cid)
        return self.tarspsmg.get(k, 0)

    def gettaramgs(self, zona):
        """
        return agent cost 1 min for sps mg
        """
        x = self.taraspsmg.get(str(zona), None)
        if x:
            return x['tara']
        else:
            return 0

    def get_stat_code_zona_tar_name(self, cid, number):
        """
        cid=273, number=988123263992
        return: (stat, code, zona, tar, name) : ('mg', '8812', 2, 3.31, 'name')
        81012817748622
        """
        num = number
        code, zona, tar, tara, name = ('-', -2, 0, 0, '-')
        sts, prx = self.stat.getsts(number, retprx=True)    # ('vz','916025') or ('mg','8312261')

        if sts == 'mgs':    # mg over sps
            code, zona, stat, name = self.cdef.get_sqlite_code_zona_stat_reg(prx)    # ('903423', 4, 'mg', 'Дагестан')
            tar = self.gettarmgs(zona, cid)
            if tar == 0:
                tar = self.gettarmgs(zona, self.custdefault)
            tara = self.gettaramgs(zona)

        elif sts == 'vz' or sts == 'mg' or sts == 'mn':
            for x in self.codes:
                code, nid = x.split(";")    # '7831;797': code=7831 nid=797
                if code.startswith('7'):
                    code = code[1:]
                tar = self.gettar(nid, cid)
                if tar == 0:
                    tar = self.gettar(nid, self.custdefault)
                tara = self.gettara(nid)
                if prx.startswith(code):
                    a = self.names.get(nid, None)
                    if a:
                        name, zona = (a['name'].decode('utf8'), a['zona'])
                        break
        return (sts, code, zona, tar, tara, name)

# end class TarMTC


def _add_komstar_code():
    """
    add Rossia_mob-1 .. Rossia_mob-6 to komstarCode (6 records)
    """
    db = pymysql.Connect(**cfg.dsn_tar)
    cur = db.cursor()
    table = 'komstarCode'
    tmax = [0, 2.94, 4.20, 5.60, 6.00, 7.00, 7.50]

    for zona in [1, 2, 3, 4, 5, 6]:
        name = "Россия (моб) {zona} зона".format(zona=str(zona))
        regkom = "Rossia_mob-{zona}".format(zona=str(zona)) # Rossia_mob-1 .. Rossia_mob-6
        code2 = "9rm{zona}".format(zona=str(zona))          # 9rm1 .. 9rm6
        tar = 1.10  # agent tariff
        tarmax = tmax[zona]
        sql = "insert into {table} (stat, `type`, `name`, regkom, zona, code1, code2, tar, tarmax) " \
              "values ('{stat}','{type}','{name}','{regkom}',{zona},'{code1}','{code2}',{tar},{tarmax})" \
              "".format(table=table, stat='-',type='MG', name=name, regkom=regkom, zona=int(zona), code1='7', code2=code2, tar=tar, tarmax=tarmax)
        print(sql)
        cur.execute(sql)

    cur.close()
    db.close()


def _add_komstar_tar():
    """
    add 8(cust)*6(zone) = 48 records to tarif.komstarTar
    """
    db = pymysql.Connect(**cfg.dsn_tar)
    cur = db.cursor()
    table = 'komstarTar'
    cust = [84, 273, 549, 760, 787, 952, 953, 957]
    tr = dict()
    tr['84'] =  [0, 1,    1,    1.20, 1.50, 1.70, 1.70]
    tr['273'] = [0, 1.75, 2.88, 4.40, 6,    7,    7.50]
    tr['549'] = [0, 2.36, 4.72, 6.14, 7.08, 7.91, 8.85]
    tr['760'] = [0, 1.03, 2.47, 3.19, 3.48, 3.48, 3.48]
    tr['787'] = [0, 1.02, 1.37, 1.67, 2.13, 2.13, 2.13]
    tr['952'] = [0, 2.01, 3.31, 5.60, 6.00, 7.00, 7.50]
    tr['953'] = [0, 2.60, 4.20, 5.60, 6.00, 7.00, 7.50]
    tr['957'] = [0, 1,    1,    1.20, 1.50, 1.70, 1.70]

    for cid in cust:
        nid = 851
        for zona in [1, 2, 3, 4, 5, 6]:
            nid = int("{nid}".format(nid = nid+1))  # 852 ..
            regrsi = "Rossia_mob-{zona}".format(zona=str(zona)) # Rossia_mob-1 .. Rossia_mob-6
            sql = "insert into {table} (cid, nid, onid, regrsi, tar, tar_old) " \
                  "values ({cid},{nid},{onid},'{regrsi}',{tar},{tar_old})" \
                  "".format(table=table, cid=cid, nid=nid, onid=0, regrsi=regrsi, tar=tr[str(cid)][zona], tar_old=0)
            print(sql)
            cur.execute(sql)

    print(tr['84'][4])
    cur.close()
    db.close()


def _testMTC(otm):
    """
    test class TarMTC:
    calc _codemts, _nidmts, _naprmts, _summts
    otm: object TarMTC
    """

    db = pymysql.Connect(**cfg.dsn_bill)
    cur = db.cursor()
    table = 'Y2013M11'
    sql = "select id, cid, to2 num, sec, `min` from {table} where op='m' limit 100".format(table=table)
    cur.execute(sql)
    for line in cur:
        idd, cid, num, sec, _min = line
        sts, code, zona, tar, tara, name = otm.get_stat_code_zona_tar_name(cid=cid, number=num)
        sumcust = _min * tar
        # sumagent = round((sec * tara/60.),3)
        sumagent = _min * tara
        print("({cid}, num={num}): sts={sts} code={code} zona={zona} name={name} sec={sec} min={min} tar={tar} "
              "tara={tara} sumcust={sumcust} sumagent={sumagent}".format(
                sts=sts, code=code, zona=zona, tar=tar, tara=tara, name=name.encode('utf8'), cid=cid, num=num,
                sumcust=sumcust, sumagent=sumagent, sec=sec, min=_min))
        sql = "update {table} set _codemts='{code}', _naprmts='{name:s}', _summtscust={sumcust}, _summtsagent={sumagent}"\
            " where id={id}".format(table=table, id=idd, code=code, nid=0, name=name[:40].encode('utf8'), sumcust=sumcust,
                                    sumagent=sumagent)
        print(sql)
        cur.execute(sql)
    cur.close()
    db.close()


def set_cust_type(dsn_cust, dsn_bill, table, where):
    """
    Установка кода клиента u|f  в поле `uf` для table
    """
    flist = dict()

    # список клиентов - физлиц
    db = pymysql.Connect(**dsn_cust)
    cur = db.cursor()
    sql = "SELECT `CustID` FROM `customers`.`Cust` WHERE `CustType`='f'"
    cur.execute(sql)
    for line in cur:
        cid = line
        flist[cid] = cid

    # разделение клиентов по ризнаку uf (u|f)
    db = pymysql.Connect(**dsn_bill)
    cur = db.cursor()
    sql = "SELECT id, cid FROM {table} WHERE {where}".format(table=table, where=where)
    cur.execute(sql)
    step = 0
    for line in cur:
        idd, cid = line
        uf = flist.get(cid, 'u')
        sql = "UPDATE `{table}` SET `uf`='{uf}' WHERE id={idd}".format(table=table, uf=uf, idd=idd)
        cur.execute(sql)
        step += 1
    return step


if __name__ == '__main__':
    p = optparse.OptionParser(description="load calls from smg.YxxxxMxx (ex.Y2013M09) for billing",
                              prog="load.py", version="0.1a", usage="load.py --year=year --month=month [--log=namefile]")

    p.add_option('--year', '-y', action='store', dest='year', help='year, example 2021')
    p.add_option('--month', '-m', action='store', dest='month', help='month in range 1-12')
    p.add_option('--log', '-l', action='store', dest='log', default=flog, help='logfile')

    opts, args = p.parse_args()

    # параметры в командной строке - в приоритете
    if not (opts.year and opts.month):
        opts.year = ini.year
        opts.month = ini.month

    opts.table = ut.year_month2period(year=opts.year, month=opts.month)

    if not opts.table or not opts.log:
        print(p.print_help())
        exit(1)

logging.basicConfig(
    filename=opts.log, level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S", format='%(asctime)s %(message)s', )

try:

    t1 = time.time()
    log = logging.getLogger('app')

    n811 = Number811(cfg.dsn_tar)
    numb = Numbers(cfg.dsn_tel, opts.table, n811)
    n626 = Number626(cfg.dsn_tel, opts.table)
    n710 = Number710(cfg.dsn_tel, opts.table)
    cal = Calendar(cfg.dsn_cal, opts.table)
    cdef = codedef.Codedef(dsn=cfg.dsn_tar, tabcode='defCode')
    stat = Stat(cal=cal, cdef=cdef)

    bill = Billing(cfg.dsn_bill, table=opts.table, tab_sample='./sql/table_bill.sql')

    # oper:(m=MTS b=BEELINE f=MEGAFON g=FGUP-RSI c=CITYLAN x=MGTS r=RSS)

    smg2 = Smg642(dsn=cfg.dsn_smg2, bill=bill, table=opts.table, numb=numb, stat=stat)
    smg2.add(src_num=('7499642____%', '%642____%', '81252__', '8117___', '710%', '627%'), dtr_trank=('mts', 'mrp'), info='smg2.642_MTS', eq='smg2_642q', op='q')
    smg2.add(src_num=('7495626%', '8495626%', '626%'), dtr_trank=('mts', 'mrp'), info='smg2.626_MTS', eq='smg2_626q', op='q')
    smg2.add(src_num=('81_____',), dtr_trank=('mts', 'mrp'), info='smg2.811_MTS', eq='smg2_811q', op='q')

    # 2021-12-01 added customers from CTS
    smg2.add(operator='rlike',
             src_num=('^(7|8)?495221....', '^(7|8)?495236....', '^(7|8)?495730....', '^(7|8)?495739....',
                      '^(7|8)?495623....', '^(7|8)?495624....', '^(7|8)?495679....'),
             dtr_trank=('mts', 'mrp'), info='smg2.CTS_MTS', eq='smg2_CTS', op='q')

    # 2022-02-01 added customers from TCU
    smg2.add2(where="(str='tcukom' and dtr='mts')", info='smg2.TCU_MTS', eq='smg2_TCU', op='q', p='q', f3='+')

    t2 = time.time()
    print("work: {0:0.2f} sec".format(t2 - t1, ))
    itog_log(info='end', step=0, update=0, t1=t1, t2=time.time())

    log.warning('.')

except pymysql.Error as e:
    log.warning(str(e))
    print(e)
except RuntimeError as e:
    log.warning(str(e))
    print(e)
except Exception as e:
    log.warning(str(e))
    traceback.print_exc(file=open(opts.log, "at"))
    traceback.print_exc()
