#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
2021-04-07: set-fcalcint.py
установка флажка customers.Cust.fCalcInt=Y для тех клиентов у кого есть ip в billing.tabIP:

1. сброс. установим для всех fCalcInt = 'N'
UPDATE customers.Cust SET fCalcInt = 'N';

2. установим fCalcInt = 'Y' для тех что имеют Ip в billing.tabIP
UPDATE customers.Cust SET fCalcInt = 'Y' WHERE CustID IN (SELECT CustID FROM billing.tabIP GROUP BY CustID);

3. проверка - сколько клиентов имеют ip (то есть услугу интернет)
SELECT count(*) countIp from customers.Cust WHERE fCalcInt = 'Y'; // клиенты с IP
SELECT count(*) countIp FROM (SELECT CustID FROM billing.tabIP group by CustID) a; // клиенты с IP
"""
import os
import sys
import MySQLdb
import datetime
from cfg import cfg


def connect(host, user, passwd, db, use_unicode=True, charset='utf8', connect_timeout=5, local_infile=0):
    return dict(host=host, user=user, passwd=passwd, db=db, use_unicode=use_unicode, charset=charset,
                connect_timeout=connect_timeout, local_infile=local_infile)


root = os.path.realpath(os.path.dirname(sys.argv[0]))
logfile = "{root}/log/{file}".format(root=root, file='set-fcalcint.log')
dsn_fcalcint = cfg.dsn_fcalcint


def log(msg):
    """
    логирование
    """
    global logfile
    f = open(logfile, "a")
    date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    f.write("{date} {msg}\n".format(date=date, msg=msg))
    f.close()


def main(dsn):
    db = MySQLdb.Connect(**dsn)
    cursor = db.cursor()
    requests_update = [
        dict(about="set customers.Cust.fCalcInt = 'N' for all customers", sql="UPDATE customers.Cust SET fCalcInt = 'N'"),
        dict(about="set customers.Cust.fCalcInt = 'Y' for customers with ip", sql="UPDATE customers.Cust SET fCalcInt = 'Y' WHERE CustID IN (SELECT CustID FROM billing.tabIP GROUP BY CustID)"),
    ]
    requests_check = [
        dict(about="get number of customers with flag fCalcInt = 'Y'", sql="SELECT count(*) countIp from customers.Cust WHERE fCalcInt = 'Y'"),
        dict(about="get number of customers with IP", sql="SELECT count(*) countIp FROM(SELECT CustID FROM billing.tabIP group by CustID) a"),
    ]

    for req in requests_update:
        cursor.execute(req['sql'])
        updated = cursor.rowcount
        msg = "({updated} records): {about}".format(updated=updated, about=req['about'])
        print(msg)
        log(msg)

    for req in requests_check:
        cursor.execute(req['sql'])
        count = cursor.fetchone()[0]
        msg = "({count} records): {about}".format(count=count, about=req['about'])
        print(msg)
        log(msg)

    cursor.close()
    db.close()
    log('.')


if __name__ == '__main__':
    main(dsn=dsn_fcalcint)

