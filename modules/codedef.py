#!/usr/bin/env python
# -*- coding: utf-8 -*-
# from __future__ import unicode_literals
from urllib.request import urlopen
import ssl
import time
import datetime
import logging
import pymysql
import traceback
import re
from io import open
import os.path as path

from cfg import cfg
from modules.func import Func

"""
 codedef - Модуль по работе с кодами сотовой подвижной связи (СПС)

  как использовать:
  import codedef

  # экземпляр
  cdef = Codedef(dsn=cfg.dsn_tar, tabcode='defCode')

  # данными с сайта Россвязи обновляем defCode
  cdef.update(url=url, tabsample='table_defcode.sql', fout=fout, fout_csv=fout_csv, fload=fload)

  # получить инфо по номеру
  num='89010136501'
  code, zona, stat, region = cdef.get_mysql_code_zona_stat_reg(num) # запрос к mysql-серверу
  print code, zona, stat, region                                    # '901013', 4, 'mg', 'Республика Адыгея'

  # Возвращает является ли code номером ВЗ-связи
  vz = cdef.is_codevz(num) # True | False

 - - - - - - - - - - - - - - - - - - - - - - -  - - - - - - - - -  - - - - - - - - - - - -  - - - - - - - - - - - - - -
 особенности реализации:
 defCode - все коды СПС (abc, fm, to, capacity, zona, stat, oper, region, area)
 defRegion - регионы СПС (stat, zona, region, region2, area)

  region2 и region одно и тоже по смыслу, а названия немного отличаются
  region2 может содержать несколько синонимов через ;
  area - пространство включающее в себя регион

  Коды загружаются с сайта Россвязи в базу данных MySql
  Коды распределяются по тарифным зонам России (1-6, 0-Москва) в соотв. с регионами в defRegion
  Переодически Россвязь меняет (?!) названия регионов (добавляет . и прочее), поэтому и есть два поля region и region2.
  В region изначальное название региона, а в region2 обновлённое название.
  и метод add_alias_to_region() заполняет поле region2

  поэтому для нахождения региона в defRegion (stat, zona) нужно просматривать 2 поля: region и region2

"""

# url = 'https://rossvyaz.gov.ru/data/DEF-9xx.csv'
url = 'http://opendata.digital.gov.ru/downloads/DEF-9xx.csv'
# "curl {url} | iconv -f cp1251 | dos2unix > def9x.txt"


def current_ymd():
    """ возвращает текущую дату в формате YYYY-MM-DD """
    return time.strftime("%Y-%m-%d", time.localtime())  # 2017-06-28


def current_ymdhms():
    """ возвращает текущую дату-время в формате YYYY-MM-DD HH:MM:SS"""
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())  # 2017-06-28 16:28:05


root = path.abspath(path.join(__file__, "../.."))    # корень
data = "{root}/data".format(root=root)
flog = "{root}/log/{file}".format(root=root, file='codedef.log')        # лог-файл
fout = "{data}/{file}".format(data=data, file='def9x-' + current_ymd() + '.txt')  # def9x-2013-11-22.txt
fout_csv = "{data}/{file}".format(data=data, file='def9x-' + current_ymd() + '.csv')  # def9x-2013-11-22.csv
fload = "{root}/sql/{file}".format(root=root, file='load_data.sql')     # команда sql для загрузки csv-файла


def itog_log(info='-', step=0, tt=0.0):
    """
    Логирование
    :param info: текст сообщения
    :param step: количество шагов для достижения результата
    :param tt: время на достижение результата
    """
    log.info('{info}: step:{step} time:{time:.3f}s'.format(info=info, step=step, time=tt))


def xprint(msg):
    print(msg)


class Codedef(object):
    """
    Работа с кодами сотовой связи
    """

    def __init__(self, dsn, tabcode):
        """
        :param dsn: параметры подключения к БД
        :param tabcode: таблица с def-кодами tarif.defCode
        """
        self.dsn = dsn                      # dsn подключения к базе
        self.db = pymysql.Connect(**dsn)    # db - активная ссылка на базу

        self.cur = self.db.cursor()         # cur - курсор
        self.tabCode = tabcode              # таблица с кодами (tarif.defCode)
        
        self.regions = dict()               # regions['Липецкая область'] = '30_mg_6'  ('id_stat_zona')
        self.regid = dict()                 # regid['30'] = 'Липецкая область' (resolver regions)
        self.codevz = list()                # (9160000000-9169999999,9210000000-9219999999,...)  коды СПС

        # имена, станут известны при вызове метода self.update
        self.url = None
        self.tabSample = None               # шаблон таблицы (tarif.defCode)
        self.fout = None                    # имя файла для записи кодов СПС с сайта www.rossvyaz.ru
        self.fout_csv = None                # имя файла для загрузки кодов СПС в БД
        self.fload = None                   # имя файла с sql-шаблоном для загрузки csv-файла в БД

        self.read_regions()
        self.read_codevz()

    def __del__(self):
        self.cur.close()
        self.db.close()

    def update(self, url, tabsample, fout=None, fout_csv=None, fload=None, add_alias_region=False):
        """
        Обновление таблицы с кодами tarif.defCode новой инфо с сайта Россвязи
        """
        self.url = url                      # url - указывает на страницу с данными в инете
        self.tabSample = tabsample          # шаблон таблицы (tarif.defCode)
        self.fout = fout                    # имя файла для записи кодов СПС с сайта www.rossvyaz.ru
        self.fout_csv = fout_csv            # имя файла для загрузки кодов СПС в БД
        self.fload = fload                  # имя файла с sql-шаблоном для загрузки csv-файла в БД

        # чтение инфо по кодам СПС с сайта Россвязи
        self.read_url(info='read url')

        # Обновление таблицы tarif.defCode
        self._update_code(info='create tarif.defCode')

        # Добавление синонимов для регионов
        if add_alias_region:
            self.add_alias_to_region(info='add alias to region in tarif.defRegion')

    def read_url(self, info='-'):
        """
        Чтение кодов СПС с сайта rossvyaz.ru
        read cell codes from site rossvyaz.ru
        """
        t = time.time()
        # u = urlopen(self.url)
        # data = u.read()
        # u.close()

        context = ssl._create_unverified_context()
        u = urlopen(self.url, context=context)
        data = u.read()
        u.close()

        f = open(self.fout, "wt", encoding='utf8')
        # f.write(data.decode('cp1251').replace('\r\n', '\n').replace('\t', '').replace('\r', ''))
        f.write(data.decode('utf8'))

        f.close()

        log.info('{info}: bytes:{bytes} time:{time:.3f}s'.format(info=info, bytes=len(data), time=time.time()-t))

    def __preparetable__(self):

        """
        Создание таблицы tarif.defCode если она не существует и стирание данных таблицы в любом случае
        """
        cur = self.cur
        table = self.tabCode

        # проверка на существование таблицы
        sql = "SELECT 1 FROM `information_schema`.`TABLES` WHERE table_schema = '{schema}' AND table_name = '{table}'" \
              "".format(table=table, schema=self.dsn['db'])
        cur.execute(sql)

        if cur.rowcount == 0:   # таблица не существует
            f = open(self.tabSample)
            sql = f.read().replace('_TABLE_CREATE_', table)
            f.close()
            cur.execute(sql)
            log.info("create tab:%s" % table)
        cur.execute("TRUNCATE TABLE `{table}`".format(table=table))
        log.info("truncate tab:`{table}`".format(table=table))

    def read_regions(self):
        """
        Чтение регионов (tarif.defRegion) СПС в словарь : self.regions['name'] = 'id_stat_zona'
        """
        self.cur.execute("select `id`, `stat`, `zona`, `region`, `region2` from `defRegion`")
        for line in self.cur:
            idd, stat, zona, region, region2 = line
            region = region.strip()
            self.regions[region.encode('utf8')] = "{id}_{stat}_{zona}".format(id=idd, stat=stat, zona=zona)

            if region2 != '-':
                reg_array = region2.split(';')  # может быть несколько через ;
                for reg in reg_array:
                    self.regions[reg.encode('utf8')] = "{id}_{stat}_{zona}".format(id=idd, stat=stat, zona=zona)

            self.regid[str(idd)] = region.encode('utf8')  # regid['30'] = 'Липецкая область'

    def print_regions(self):
        """
        Печать регионов СПС
        """
        keys = list(self.regions.keys())
        keys.sort()
        for item in keys:
            xprint("{item}:{val}".format(item=item.decode('utf8'), val=self.regions[item]))

    def get_rid(self, name):
        """ возвращает id из region[name] = (id_stat_zona) """
        return int(self.regions.get(name.encode('utf8'), "-2_xx_-2").split('_')[0])

    def get_rstat(self, name):
        """ возвращает stat из region[name] = (id_stat_zona) """
        return self.regions.get(name.encode('utf8'), "-2_xx_-2").split('_')[1]

    def get_rzona(self, name):
        """ возвращает zona из region[name] = (id_stat_zona) """
        return int(self.regions.get(name.encode('utf8'), "-2_xx_-2").split('_')[2])

    def get_rname(self, regid):
        """ возвращает имя региона по коду региона """
        return self.regid.get(regid, "-")

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

    def write_codevz_file(self, filename=None, filename2=None):
        """
        Сохранение кодов ВЗ-связи в файле в компактном виде
        (для вставки в komstarCode: для строки regkom='Moskow_mob' в поле code2 )
        :param filename: имя файла для сохранения кодов ВЗ-связи
        :param filename2: имя файла для сохранения кодов ВЗ-связи для Инител: (id;code;oper;ts)
        """
        """
        если возможно, то сжиматся для компактности:
         - 9005550000-9005555549 -> 9005550000-9005555549 (нечего сокращать)
         - 9011800000-9011899999 -> 901180-901189  (4 нуля и 4 девятки)
         - 9014700000-9014709999 -> 901470-901470 -> 901470
         - 9411000000-9411004999 -> 9411000-9411004 (3 нуля и 3 девятки)
        """
        vz_debug = list()
        vz = list()
        vz2 = list()
        opers = dict()  # opers[name]=alias

        # список операторов с полным и кратким названием
        sql = "select `name`, `alias` from opers"
        self.cur.execute(sql)
        for line in self.cur:
            name, alias = line
            opers[name] = alias

        # коды ВЗ связи
        sql = "select `abc`, `fm`, `to`, `oper` from `{table}` where `stat`='vz'".format(table=self.tabCode)
        self.cur.execute(sql)
        for line in self.cur:
            # 900   5550000 5555549 5550    : 9005550000 - 9005555549
            # 900   5555551 5559999 4449    : 9005555551 - 9005559999
            # 900   6110000 6199999 90000   : 9006110000 - 9006199999 -> 900611-900619
            # 901   4610000 4619999 10000   : 9014610000 - 9014619999 -> 901461-901461 -> 901461
            abc, fm, to, oper = line
            start = "{abc}{x}".format(abc=abc, x=Func.strz(fm, 7))
            end = "{abc}{x}".format(abc=abc, x=Func.strz(to, 7))
            a = x = "{start}-{end}".format(start=start, end=end)
            if start.endswith('0000') and end.endswith('9999'):
                start = start[:-4]
                end = end[:-4]
                a = "{start}-{end}".format(start=start, end=end)
                x = "{x} : {a}".format(x=x, a=a)

            elif start.endswith('000') and end.endswith('999'):
                start = start[:-3]
                end = end[:-3]
                a = "{start}-{end}".format(start=start, end=end)
                x = "{x} : {a}".format(x=x, a=a)

            if start == end:
                a = "{start}".format(start=start)
                x = "{x} : {a}".format(x=x, a=a)

            vz_debug.append(x)
            vz.append(a)
            vz2.append("{a}:{oper_alias}".format(a=a, oper_alias=opers.get(oper, '.')))

        step = 0
        vz_debug.sort()
        for x in vz_debug:
            xprint("{x}".format(x=x))
            step += 1
        xprint("itogo:{itogo} records".format(itogo=step))

        xprint("--------------------")
        vz.sort()
        for x in vz:
            xprint("{x}".format(x=x))
            step += 1
        xprint("itogo:{itogo} records".format(itogo=step))

        # в строчку
        st = ','.join(vz)
        xprint(st)
        f = open(filename, "w", encoding='utf8')
        f.write(st)
        f.close()

        # в столбик
        f = open(filename+"2", "w", encoding='utf8')
        for x in vz:
            f.write("{x}\n".format(x=x))
            step += 1

        # exit(1)

        # 2016-07-31
        # плоский список кодов ВЗ-связи для биллинга Инител
        # id; code; oper; ts
        xprint("/////////////////////////////////")
        f = open(filename2, "w", encoding='utf8')
        f.write("{id};{code};{oper};{stat};{ts}\n".format(id='id', code='code', oper='oper', stat='stat', ts='ts'))
        step1 = 0
        step2 = 0
        now = datetime.datetime.now()
        ts = "{year:04d}{month:02d}{day:02d}{hh:02d}{mm:02d}{ss:02d}".format(
            year=now.year, month=now.month, day=now.day, hh=now.hour, mm=now.minute, ss=now.second)
        stat = 'Z'  # Z=Внутризоновая
        for xoper in vz2:
            x, oper = xoper.split(':')
            xprint("{x} {oper}".format(x=x, oper=oper))
            step1 += 1
            if x.find('-') > -1:
                start, end = x.split('-')
                start = int(start)
                end = int(end)
                for n in range(start, end+1):
                    xprint("\t{n} {oper}".format(n=n, oper=oper))
                    f.write("{id};{code};{oper};{stat};{ts}\n".format(id=0, code=n, oper=oper, stat=stat, ts=ts))
                    step2 += 1
            else:
                xprint("\t{x} {oper}".format(x=x, oper=oper))
                f.write("{id};{code};{oper};{stat};{ts}\n".format(id=0, code=x, oper=oper, stat=stat, ts=ts))
                step2 += 1

        f.close()
        xprint("vz-compress/vz-plain:{step1}/{step2} records".format(step1=step1, step2=step2))

    def is_codevz(self, code):
        """
        Возвращает является ли code номером ВЗ-связи
        :param code: номер типа 916021xxxx или 8916021xxxx
        :return: True|False
        """
        if len(code) == 11 and code.startswith('89'):    # 8916021xxxx
            code = code[1:]

        for x in self.codevz:
            _fm, _to = x.split('-')     # 9160000000-9169999999
            if _fm <= code <= _to:
                return True

        return False

    def print_codevz(self, fname=None):
        """ Печать всех кодов ВЗ-связи
        :param fname: если есть, то дополнительно печать в файл
        """
        f = None
        if fname:
            f = open(fname, "wt", encoding='utf8',)

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

    def _update_code(self, info='-'):
        """
        Обновление таблицы tarif.defCode новыми данными
        Данные в tarif.defCode стираются и загружаются новые с сайта Россвязи
        :param info: информация для логирования
        """
        t = time.time()
        re_num = re.compile('^\d\d\d')
        re_patch = re.compile('Московская область; Москва\|Московская область; Москва$')
        db = pymysql.Connect(**self.dsn)
        cur = db.cursor()

        # Инициализация таблицы defCode
        self.__preparetable__()

        # коды СПС с сайта Россвязи запишем в csv-файл
        f = open(self.fout)
        f2 = open(self.fout_csv, "wt")
        msg = "{id};{abc};{fm};{to};{capacity};{zona};{stat};{oper};{region};{area};{ts}".format(
            id='id', abc='abc', fm='fm', to='to', capacity='capacity', oper='oper', region='region', area='area',
            zona='zona', stat='stat', ts='ts')
        f2.write(msg + "\n")
        ts = current_ymdhms()

        # АВС/ DEF;От;До;Емкость;Оператор;Регион
        # АВС/ DEF;От;До;Емкость;Оператор;Регион;ИНН
        # 900;0000000;0061999;62000;ООО "Т2 Мобайл";Краснодарский край;7743895280

        # 900;0;99999;100000;Телеком Евразия;Краснодарский край
        # 900;3950000;4049999;100000;ЕКАТЕРИНБУРГ-2000;Ямало - Ненецкий автономный округ |Тюменская область
        # 955;5550000;5559999;10000;ООО "ТРН-телеком";Московская область; Москва|Московская область; Москва
        # 955;8880000;8889999;10000;ООО "МОСТЕЛЕКОМ";Московская область; Москва|Московская область; Москва

        step = 0
        for line in f:
            if not re_num.match(line):
                continue

            line = re_patch.sub('Москва|Московская область', line)

            abc, fm, to, capacity, oper, region, inn = line.strip().split(';')
            regs = region.split('|')
            region, area = (regs[0].strip(), '-')
            if len(regs) > 1:
                area = "|".join(regs[1:])
            stat = self.get_rstat(region)
            zona = self.get_rzona(region)

            msg = "{id};{abc};{fm};{to};{capacity};{zona};{stat};{oper};{region};{area};{inn};{ts}".format(
                    id='0', abc=abc, fm=fm, to=to, capacity=capacity, oper=oper, region=region,
                    area=area, zona=zona, stat=stat, inn=inn, ts=ts)

            f2.write(msg + "\n")
            step += 1

            if stat == 'xx':
                print(("?region: {msg}".format(msg=msg)))

        cur.close()
        db.close()
        f.close()
        f2.close()

        # загрузка csv-файла в бд (defCode)
        f = open(self.fload)
        sql = f.read().replace('_TABLE_', self.tabCode).replace('_FILE_', self.fout_csv)
        self.cur.execute(sql)
        f.close()

        log.info('{info} records: {step} time:{time}'.format(info=info, step=step, time=time.time()-t))
        return step

    def get_mysql_code_zona_stat_reg(self, number):
        """
        Запрос к mysql: ищем инфо по номеру number
        :param number: (8)9034230000
        :return: (code, zona, stat, region): ('903423', 4, 'mg', 'Республика Дагестан')
        !stat in (mg, vz, sp)
        """
        num = number
        if len(num) == 11 and (num.startswith('89') or num.startswith('79')):
            num = number[1:]        # 9034230000
        abc = num[:3]               # 903
        code = num[3:]              # 4230000

        sql = "select `zona`, `stat`, `region` from `defCode` where `abc`='{abc}' " \
              "and {code} >= `fm` and {code} <= `to`".format(abc=abc, code=code)

        self.cur.execute(sql)
        zona, stat, region = (-1, '-', '-')
        if self.cur.rowcount > 0:
            zona, stat, region = self.cur.fetchone()

        return "{abc}{code}".format(abc=abc, code=code), zona, stat, region

    def _find_id_region(self, name):
        """
        Ищет регион в существующим списке по неточному названию name
        :return: id существующего региона или -2
        """
        # новое имя -> существующее имя
        alias = {
            'Кабардино-Балкарская Республика': 'Кабардино - Балкарская Республика',
            'Карачаево-Черкесская Республика': 'Карачаево - Черкесская Республика',
            'Республика Кабардино-Балкарская':  'Кабардино - Балкарская Республика',
            'Республика Карачаево-Черкесская':  'Карачаево - Черкесская Республика',
            'Республика Крым':                  'Севастополь и Республика Крым',
            'Республика Крым и г. Севастополь': 'Севастополь и Республика Крым',
            'г. Севастополь':                   'Севастополь и Республика Крым',
            'Ненецкий АО':                      'Ненецкий автономный округ',
            'Республика Саха /Якутия/':         'Республика Саха (Якутия)',
            'Республика Татарстан':             'Республика Татарстан (Татарстан)',
            'Республика Удмуртская':            'Удмуртская Республика',
            'Республика Чеченская':             'Чеченская Республика',
            'Чувашская Республика - Чувашия':   'Чувашская Республика',
            'Чукотский АО':                     'Чукотский автономный округ',
            'Ямало-Ненецкий АО':                'Ямало - Ненецкий АО',
            'Корякский округ':                  'Корякский автономный округ (Камчатская область)',
            'р-ны Абзелиловский и Белорецкий':  'Республика Башкортостан',
            'г. Санкт-Петербург':               'Санкт - Петербург',
            'г. Санкт-Петербург и Ленинградская область':   'Санкт - Петербург и Ленинградская область',
            'Ханты-Мансийский автономный округ - Югра':     'Ханты - Мансийский - Югра АО',
        }

        idr = self.get_rid(alias.get(name, '-2'))
        if idr == -2:
            if name.endswith('обл.'):
                idr = self.get_rid(name.replace('обл.', 'область'))
            else:
                for prx in ('г.', 'с.', 'п.'):
                    if name.startswith(prx):
                        nam = name.replace(prx, '').strip()
                        idr = self.get_rid(nam)
                        break

        return idr

    def add_alias_to_region(self, info='-'):
        """
        Добавление синонима для названия региона в поле region2 таблицы defRegion
        (поставщик кодов часто меняет названия регионов)
        то есть два поля region и region2 и по они по сути одно и тоже, либо region2='-'
        """
        t = time.time()
        # инициализация region2
        # self.cur.execute("update `defRegion` set `region2`='-'")

        # из свежего defCode выберем те регионы, которых нет в defRegion
        sql = "select `region` as `name` from `defCode` where `zona` < 0 group by `zona`, `region`"
        self.cur.execute(sql)
        step = 0
        for line in self.cur:
            step += 1
            name, = line
            # найдем соответствие названию в существующих названиях регионов
            idr = self._find_id_region(name.encode('utf8'))
            # и добавим синоним (region2) для region
            if idr > 0:
                # region2 может содержать несколько синонимов region через ; (точку с запятой)
                sql = "select `region2` from `defRegion` where `id`={idr}".format(idr=idr)
                self.cur.execute(sql)
                region2 = self.cur.fetchone()[0]
                if region2 == '-':
                    region2 = name.encode('utf8')
                else:
                    region2 += ';' + name
                    region2 = region2.encode('utf8')
                sql = "update `defRegion` set `region2`='{region2}' where `id`={idr}".format(region2=region2, idr=idr)
                self.cur.execute(sql)
                xprint(sql)
        itog_log(info=info, step=step, tt=time.time()-t)


if __name__ == '__main__':

    logging.basicConfig(filename=flog, level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S",
                        format='%(asctime)s %(message)s', )
    log = logging.getLogger('app')
    try:
        # один раз в месяц примерно, для загрузки новых номеров СПС в tarif.defCode c сайта Россвязи
        cdef = Codedef(dsn=cfg.dsn_tar, tabcode='defCode')
        cdef.update(url=url, tabsample='table_defcode.sql', fout=fout, fout_csv=fout_csv, fload=fload)
        exit(1)

        #
        # РАЗЛИЧНЫЕ ТЕСТЫ ---------------------
        cdef = Codedef(dsn=cfg.dsn_tar, tabcode='defCode')

        # cdef.print_regions()

        xprint("# проверка номера на ВЗ-связь:")
        for prefix in ('9160258525', '9425010000', '9160218525', '9030005755'):
            xprint("{prefix} - is VZ: {result}".format(prefix=prefix, result=cdef.is_codevz(prefix)))

        xprint("---")

        xprint("# определение информации по номеру:")
        numbers = ('89010019999', '89010136501', '89160218525', '79161303979', '9825345269', '9164657112', '9265370878',
                   '9296451321', '9175812988', '9258362114',  '79057958148', '79200331414', '79600298005')

        t1 = time.time()
        for num1 in numbers:
            _code, _zona, _stat, _region = cdef.get_mysql_code_zona_stat_reg(num1)
            xprint("{code}  {zona}  {stat}   {region}".format(code=_code, zona=_zona, stat=_stat, region=_region))
        xprint('time:{time:.3f}s'.format(time=time.time()-t1))

        xprint("---")

        # печать кодов ВЗ-связи
        # cdef.print_codevz()

        # запись кодов ВЗ-связи в текстовый файл 
        # cdef.write_codevz_file(filename='./test/def_vz.txt', filename2='./test/def_vz_initel.txt')

    except pymysql.Error as e:
        log.warning(str(e))
        xprint(e)
    except Exception as e:
        log.warning(str(e))
        # traceback.print_exc(file=open(log, "at"))
        traceback.print_exc()
