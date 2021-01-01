# -*- coding: utf-8 -*-

import os
import datetime
import calendar
import MySQLdb
from io import open
from modules import cfg
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
                path=path, year=year, month=month, service=srv, report=xx['reports'][rep], ext=xx['ext'])
    return files


def create_table(dsn, filename, table):
    """
    Создание таблицы, в файле создаваемая таблица должна иметь имя '_TABLE_CREATE_'
    :param dsn: dsn
    :param filename: name file with sql-command for create table
    :param table: name creating table
    :return:
    """
    db = MySQLdb.Connect(**dsn)
    cur = db.cursor()
    f = open(filename, encoding="utf-8")
    sql = f.read().replace('_TABLE_CREATE_', table, 2)
    f.close()
    cur.execute(sql)
    cur.close()
    db.close()


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
    return "Y{year:04d}{month_char}{month:02d}".format(year=year, month=month, month_char=month_char)


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
    date2 = datetime.date(year, month, 1)
    date1 = date2 - datetime.timedelta(days=1)  # число за 1 день до date2
    date3 = datetime.date(year, month, calendar.monthrange(year, month,)[1])
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
    return rnd(val*cfg.ndskoff, ndigits)


def nds2(val, ndigits=2):
    """
    Возвращает НДС из числа (само число val содержит НДС)
    :param val: число включает ндс
    :param ndigits: количество знаков для округления
    :return: НДС
    """
    # nds_from_number = number * 20/120;  cfg.ndskoff=0.20
    return rnd(val*cfg.ndskoff/(1+cfg.ndskoff), ndigits)
