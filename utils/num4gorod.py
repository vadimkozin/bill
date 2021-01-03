#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
num4gorod - определяет номер, для выставления повремённой платы и сохр. в поле YxxxxMxx.fm3
в повремёнке нужны номера: 626xxxx 642xxxx 710xxxx 627xxxx 81xxxxx
"""
import os
import re
import MySQLdb
import datetime
from modules import progressbar
from modules import cfg

# root = os.path.realpath(os.path.dirname(sys.argv[0]))
owd = os.getcwd()
os.chdir('..')
root = os.getcwd()
logfile = "{root}/log/{file}".format(root=root, file='num4gorod.log')
os.chdir(owd)


def log(msg):
    """
    логирование
    """
    global logfile
    f = open(logfile, "a")
    date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    f.write("{date} {msg}\n".format(date=date, msg=msg))
    f.close()


def main(table, where=None):
    """
    :param table: таблица YxxxxMxx
    :param where: дополнительный фильтр
    """
    re_626 = re.compile(r'626\d{3}')
    re_642 = re.compile(r'642\d{3}')
    re_710 = re.compile(r'710\d{3}')
    re_627 = re.compile(r'627\d{3}')
    re_812 = re.compile(r'812\d{3}')
    re_811 = re.compile(r'811\d{3}')
    # re_6261901 = re.compile(84956261901)

    db = MySQLdb.connect(**cfg.dsn_bill)
    cursor = db.cursor()
    sql = "SELECT id, op, org, cid, fm, fmx, fm2 FROM {table} WHERE _stat='G'".format(table=table)
    if where:
        sql += " AND ({where}) ".format(where=where)
    cursor.execute(sql)
    bar = progressbar.Progressbar(info=table, maximum=cursor.rowcount)
    step = 0
    for line in cursor:
        num = '-'
        idx, op, org, cid, fm, fmx, fm2 = line
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
        elif (cid == 319 or cid == 53) and re_812.match(fm):    # Терминал-Сити(319), РВЛ-Строй(53)
            num = fm
        elif cid == 84 and re_811.match(fm):
            num = fm
        elif cid == 282 and re_811.match(fm):
            num = fm
        elif cid == 957 and re_811.match(fm):   # ?
            num = fm

        elif cid == 787 and re_811.match(fm2) and fmx == '84956261901':
            num = '6261901'

        sql = "UPDATE {table} SET fm3='{num}' WHERE id={idx}".format(table=table, num=num, idx=idx)
        cursor.execute(sql)
        step += 1
        bar.update_progress(step)

    return step


if __name__ == '__main__':

    records = main(table='Y2020M12', where=None)

    msg = "update {records} records".format(records=records)
    print("\n" + msg)
    log(msg)
