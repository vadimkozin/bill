# -*- coding: utf-8 -*-

import os
import sys
import datetime
import calendar
import pymysql
import threading
import time
from io import open
import subprocess
from cfg import cfg
from fnmatch import fnmatch
from modules.diskwalk_api import diskwalk

""" Разные функции общего назначения """


def copy_files_new_charset(path1, path2, ext, code1, code2):
    """
    Копирование файлов со сменой кодировки
    :param path1: исходное место
    :param path2: сюда кладём
    :param ext: расширения отбираемых файлов
    :param code1: кодировка "ИЗ"
    :param code2: кодировка "В"
    :return: количество перекодированных файлов
    """
    makedir(path2)

    # удаление всех файлов из path2
    files = diskwalk(path2)
    for fil in files.enumeratePaths():
        os.remove(fil)

    # перекодировка
    files = diskwalk(path1)
    for fil in files.enumeratePaths():
        if fnmatch(fil, "*.{ext}".format(ext=ext)):
            fnew = "{path}/{fname}".format(path=path2, fname=os.path.basename(fil))
            f1 = open(fil, "rt", encoding=code1)
            f2 = open(fnew, "wt", encoding=code2)
            f2.write(f1.read())
            f1.close()
            f2.close()


def dec(val):
    """
    Возвращает десятичное число в формате 12345,10 (разделитель-запятая и 2 знака после запятой)
    :param val: число: 12345.1
    :return: строка: '12345,10'
    """
    return "{val:.02f}".format(val=val).replace('.', ',')


def inn(val):
    """
    возвращает val если он есть, а если None, то возвращает двенадцать 000000000000
    :param val:
    :return:
    """
    return val if val else '000000000000'


def kpp(val):
    """
    возвращает val если он есть, а если None, то возвращает девять  000000000
    :param val:
    :return:
    """
    return val if val else '000000000'


def double_q(val):
    """
    удвоение кавычек в строке + удаление пробелов слева и справа
    :param val: строка
    :return: строка с удвоенными кавычками
    """
    return val.strip().replace('"', '""', 100)


def makedir(path):
    """
    Создаёт директорию, если она не существует
    :param path: полный путь создаваемой директории
    :return: ничего не возвращает
    """
    if not os.access(path, os.W_OK):
        os.makedirs(path)


def get_operator_files(year, month, path, reports, files):
    """
    Формирует словарь названий файлов вида '2015_11_VZ_книга_продаж.csv' для выгрузки оператору
    :param year: год
    :param month: месяц
    :param path: корень для файлов
    :param reports: хеш хешей -  по каким сервисам и какие нужны отчёты
    :param files: хеш с результатом
    :return: files: словарь названий для (MG,MN,ALL), files['MG']['book']='path/2015_11_VZ_книга_продаж.csv'
    """
    xx = reports
    for srv in xx['services']:          # MG, VZ, ALL
        if srv not in files:
            files[srv] = dict()
        for rep in xx['reports']:       # book, detal, amount, total, itog
            if rep not in files[srv]:
                files[srv][rep] = dict()
            files[srv][rep] = "{path}/{year}_{month:02d}_{service}_{report}.{ext}".format(
                path=path, year=int(year), month=int(month), service=srv, report=xx['reports'][rep], ext=xx['ext'])
    return files


def create_table(dsn, filename, table):
    """
    Создание таблицы, в файле создаваемая таблица должна иметь имя '_TABLE_CREATE_'
    :param dsn: dsn
    :param filename: name file with sql-command for create table
    :param table: name creating table
    :return:
    """
    db = pymysql.Connect(**dsn)
    cur = db.cursor()
    f = open(filename, encoding="utf-8")
    sql = f.read().replace('_TABLE_CREATE_', table, 2)
    f.close()
    cur.execute(sql)
    cur.close()
    db.close()


def create_table_if_no_exist(dsn, table, tab_template):
    """
    create table if one not exists
    :param dsn: dsn
    :param table: создаваемая таблица
    :param tab_template: шаблон создания таблицы (sql-запрос в файле)
    """
    db = pymysql.Connect(**dsn)
    cursor = db.cursor()

    created = False

    if not if_exist_table(cursor, table):
        f = open(tab_template)
        sql = f.read().replace('_TABLE_CREATE_', table)
        f.close()
        cursor.execute(sql)
        if if_exist_table(cursor, table):
            created = True

    cursor.close()
    db.close()
    return ('', table)[created]


def if_exist_table(cursor, table):
    sql = "SHOW TABLES LIKE '{table}'".format(table=table)
    cursor.execute(sql)

    return (False, True)[cursor.rowcount];


def getlastid(cursor, table, year, month, field='id'):
    """
    Возвращает последний (максимальный) номер в поле field
    :param cursor: курсор
    :param table: таблица
    :param year: год
    :param month: месяц
    :param field: название поля ( по умолчанию id )
    :return:
    """
    sql = "SELECT max(`{field}`) FROM `{table}` WHERE `year`={year} AND `month`='{month}'".format(
        table=table, year=year, month=month, field=field)
    cursor.execute(sql)
    v = cursor.fetchone()[0]
    return v if v else 0


def get_last_account(cursor, table, year, field='account'):
    """
    Возвращает последний (максимальный) номер в поле field
    :param cursor: курсор
    :param table: таблица
    :param year: год
    :param field: название поля ( по умолчанию account )
    :return:
    """
    sql = "SELECT max(`{field}`) FROM `{table}` WHERE `year`='{year}' ".format(
        table=table, year=year, field=field)
    cursor.execute(sql)
    v = cursor.fetchone()[0]
    return v if v else 0


def period2year_month(period):
    """
    Возвращает кортеж (год, месяц) из периода по шаблону: Y2015M11
    :param period: период, например, Y2015M11
    :return: (2015,11)
    """
    year, month = period[1:].split('M')
    return int(year), int(month)


def year_month2period(year, month, month_char='M'):
    """
    Возвращает период типа Y2015M11 или Y2015_11
    :param year: год, 2015
    :param month: месяц, 11
    :param month_char: символ месяца: M or _
    :return:  Y2015M11 or Y2015_11
    """
    return "Y{year:04d}{month_char}{month:02d}".format(year=int(year), month=int(month), month_char=month_char)


def formatdate(date, separator='-'):
    """
    форматирование даты
    :param date: питоновская дата (date)
    :param separator: разделитель полей в дате: 29-11-2015 or 29.11.2015
    :return: отформатрованную дату в виде строки '29-11-2015'
    """

    if date:
        return "{day:02d}{sep}{month:02d}{sep}{year:04d}".format(day=date.day, month=date.month, year=date.year,
                                                                 sep=separator)
    else:
        return "{day:02d}{sep}{month:02d}{sep}{year:04d}".format(day=11, month=11, year=1111, sep=separator)


def formatdate_org(date, separator='-'):
    """
    форматирование даты
    :param date: питоновская дата (date)
    :param separator: разделитель полей в дате: 29-11-2015 or 29.11.2015
    :return: отформатрованную дату в виде строки '29-11-2015'
    """
    return "{day:02d}{sep}{month:02d}{sep}{year:04d}".format(day=date.day, month=date.month, year=date.year,
                                                             sep=separator)


def dateperiod(year, month):
    """
    Возвращает кортеж из 3-х дат: -1 день от 1-го числа месяца, 1-е число месяца и последнее число месяца
    (2015,3) -> (28-02-2015 01-03-2015 31-03-2015)
    :param year: год
    :param month: месяц
    :return: (date1, date2, date3)
    """
    date2 = datetime.date(int(year), int(month), 1)
    date1 = date2 - datetime.timedelta(days=1)  # число за 1 день до date2
    date3 = datetime.date(int(year), int(month), calendar.monthrange(int(year), int(month),)[1])
    return date1, date2, date3


def sqldate(date):
    """
    Преобразует питоновскую date в sql-date: YYYYMMDD
    :param date: 2015-11-30
    :return: '20151130'
    """
    return date.strftime("%Y%m%d")


def uniq(alist):
    """
    Возвращает список уникальных элементов массива
    :param alist: список
    :return: список уникальных элементов
    """
    abc = {}
    return [abc.setdefault(e, e) for e in alist if e not in abc]


def sign(val):
    """
    Возвращает знак числа:
    sign(x) = 1,   если x > 0,
    sign(x) = -1,  если x < 0,
    sign(x) = 0,   если x = 0.
    :param val: число
    :return: -1 0 or 1
    """
    x = 1
    if val == 0:
        x = 0
    elif val < 0:
        x = -1
    return x


def rnd(val, ndigits=2):
    """
    Округляет по правилам школьной арифметики
    1.112 -> 1.11,  1.145 -> 1.15,  -99.125 -> -99.13
    :param val: число
    :param ndigits: количество знаков после запятой
    :return: округлённое число
    """
    xpw = pow(10, ndigits)
    sgn = sign(val)  # знак числа
    tmp = abs(val) * xpw + 0.5
    x = sgn * int(tmp)/float(xpw)
    return x


def nds(val, ndigits=2):
    """
    Возвращает НДС для числа (само число val без НДС)
    :param val: число без ндс
    :param ndigits: количество знаков для округления
    :return: НДС
    """
    return rnd(val * cfg.ndskoff, ndigits)


def nds2(val, ndigits=2):
    """
    Возвращает НДС из числа (само число val содержит НДС)
    :param val: число включает ндс
    :param ndigits: количество знаков для округления
    :return: НДС
    """
    # nds_from_number = number * 20/120;  cfg.ndskoff=0.20
    return rnd(val * cfg.ndskoff / (1 + cfg.ndskoff), ndigits)


def get_file_rows(filename):
    """ return count rows of file """
    cmd = "wc -l {file}".format(file=filename)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=open('/dev/null'))
    out = p.stdout.read()
    return int(out.strip().split()[0])


def get_full_path(year, month, path, directory, ext):
    """
    Формирует полный путь к файлу по шаблону: {path}/{period}/{directory}/{filename}_{suffix}.{ext}
    где period = {year}_{month}, напр. 2021_01
    suffix = directory, напр. book
    :param year: год
    :param month: месяц
    :param path: путь до периода
    :param directory: директория после периода
    :param ext: расширение файла
    :return: (путь без файла, полный путь):  (/tmp/abc, /tmp/abc/filename.txt)
    """
    suffix = directory

    period = '{year:04d}_{month:02d}'.format(year=int(year), month=int(month))  # 2021_01
    filename = "{period}_{suffix}.{ext}".format(period=period, suffix=suffix, ext=ext)  # 2021_01_book.xls
    path = "{path}/{period}/{directory}".format(path=path, period=period, directory=directory)
    result = "{path}/{filename}".format(path=path, filename=filename)  # path/2021_01/book/2021_01_book.xlsx

    return path, result


class ProgressTime(object):
    """
    Прогресс времени
    time_start: точка отсчёта
    pt = ProgressTime(time.time())
    diff = pt.timer(time.time())
    """
    def __init__(self, time_start):
        self.time_start = time_start

    """ возвращает сколько прошло времени с точки отсчёта """
    def timer(self):
        return time.time() - self.time_start


class ProgressChar(object):
    """ Прогресс в виде меняющегося символа """
    """
        Запуск символьного прогресса с интервалом 0.25сек и отменой через 9 секунд: 
        p = ProgressChar(0.25)
        p.go()
        threading.Timer(9.0, p.cancel).start()
    """
    def __init__(self, interval):
        self.interval = interval
        self.current = 0
        self.timer = None

    def go(self):
        chars = ('-', '|', '/', '\\', ':)', '|', '/')
        sys.stdout.write('\r' + chars[self.current])
        sys.stdout.flush()
        self.current += 1
        if self.current >= len(chars):
            self.current = 0

        self.cancel()
        self.timer = threading.Timer(self.interval, self.go)
        self.timer.start()

    def cancel(self):
        if self.timer:
            self.timer.cancel()


def period_is_billing(year, month):
    """
    Возвращает True|False - соответсвует ли заданный период расчётному периоду
    ps. 'расчётный период' - это всегда предыдущий месяц от текущего астрономического
    :param year: год
    :param month: месяц
    :return: True | False
    """

    # текущий расчётный год и месяц
    year_current = datetime.datetime.now().year
    month_curent = datetime.datetime.now().month
    month_curent -= 1
    if month_curent == 0:
        month_curent = 12
        year_current -= 1

    print('year:', year, 'month:', month)
    print('year_current:', year_current, 'month_curent:', month_curent)


    return year == year_current and month == month_curent
