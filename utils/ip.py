#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ip.py - утилита для работы с webrss
2021-03-11 добавил 15 записей в webrss.tabIP. Стало 1024, 192.166.232.0/22
https://www.calculator.net/ip-subnet-calculator.html?cclass=b&csubnet=22&cip=192.166.232.0&ctype=ipv4&printit=0&x=61&y=19

0. запросами к tabIP выяснилось что не хватает записей из сетки 192.166.232.*
1. create_ip_net() - создаёт таблицу tmp232 с диапазоном  192.166.232.0-192.166.232.255
   и помечает ip-адреса (f='+') которых нет в tabIP
2. insert_new_record_tab_ip() - дополняет tabIP записями из tmp232 у которых f='+'

"""
import MySQLdb
from modules import cfg

table_tmp = 'tmp232'
drop_table_tmp232 = "DROP TABLE IF EXISTS `{table}`;".format(table=table_tmp)
create_table_tmp232 = "CREATE TABLE `{table}` (`id` int(7) NOT NULL AUTO_INCREMENT COMMENT 'код записи', " \
               "`ip` char(15) NOT NULL DEFAULT '0' COMMENT 'IP-address', " \
               "`f` char(1) NOT NULL DEFAULT '-', " \
               "PRIMARY KEY (`id`), " \
               "UNIQUE KEY `ip` (`ip`)) " \
               "ENGINE=MyISAM  DEFAULT CHARSET=cp1251;".format(table=table_tmp)


def create_ip_net(dsn=cfg.dsn_webrss, table=table_tmp, begin='192.166.232', start=0, stop=256):
    db = MySQLdb.Connect(**dsn)
    cursor = db.cursor()

    # cursor.execute(drop_table_tmp232)
    # cursor.execute(create_table_tmp232)

    for step in range(start, stop):
        ip = '{begin}.{step}'. format(begin=begin, step=step)
        sql = "INSERT INTO `{table}` (`ip`) VALUES ('{ip}')".format(table=table, ip=ip)
        print(sql)
        cursor.execute(sql)

    cursor.execute("UPDATE `{table}` SET f='-'".format(table=table))
    cursor.execute("UPDATE `{table}` a LEFT JOIN `tabIP` b ON a.ip=b.IP SET `f`='+' WHERE b.IP Is NULL".format(table=table))

    cursor.close()
    db.close()


def insert_new_record_tab_ip(dsn=cfg.dsn_webrss, table=table_tmp):
    db = MySQLdb.Connect(**dsn)
    cursor = db.cursor()

    sql = "SELECT ip FROM {table} WHERE f='+'".format(table=table)
    cursor.execute(sql)

    for line in cursor:
        ip = line[0]

        user = "null-{six_numbers}".format(six_numbers=''.join(ip.split('.'))[-6:])      # null-235103
        sql = "INSERT INTO `tabIP` (`IP`, `f_ip`, `f_p25`, `CustID`, `Prim`, `user`) " \
              "VALUES ('{ip}','{f_ip}', '{f_p25}', '{CustID}', '{Prim}', '{user}')".\
            format(ip=ip, f_ip='-', f_p25='-', CustID=546, Prim='add', user=user)
        print(sql)
        cursor.execute(sql)

    cursor.close()
    db.close()


if __name__ == '__main__':

    # create_ip_net()
    insert_new_record_tab_ip()

