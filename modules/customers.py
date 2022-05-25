#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Клиенты
"""
import pymysql
from cfg import cfg


class CustReplace(object):
    """
    Замена кодов клиента для выставления счёта
    """
    def __init__(self, dsn):
        self.dsn = dsn
        self._customers = dict()
        self.create_custmap()

    def create_custmap(self):
        """
        Создание мапы замещающих клиентов
        """
        db = pymysql.Connect(**self.dsn)

        cursor = db.cursor()
        sql = "SELECT `cid`, `cid_new` FROM `cust_replace`"

        cursor.execute(sql)
        for line in cursor:
            cid, cid_new = line
            self._customers[cid] = cid_new

        cursor.close()
        db.close()

    def get_cid_new(self, cid):
        """
        Возвращает код клиента для выставления счёта
        :param cid: код клиента из таблицы номеров (cid=telefon.tel.cid)
        :return: код клиента для выставления счёта cid_new
        """
        return self._customers.get(int(cid), cid)

    def print_cust(self):
        """
        Печать информации по всем клиентам замещения
        """
        for cid in self._customers.keys():
            print("{cid} -> {cid_new}".format(cid=cid, cid_new=self._customers[cid]))


class CustItemKs(object):
    """ Данные по клиентам квартирного сектора, клиенты без договоров (626-е номера) """
    def __init__(self, pid, cid, tid, custname, tel, address):
        self.pid = pid  # код ФЛ для cid=549
        self.cid = cid  # код cid (customers.CustKS.cid = customers.CustID for CustType='f')
        self.tid = tid  # код тарифного плана (->tariff.tariff_tel.tid)
        self.custalias = custname
        self.custname = custname
        self.tel = tel  # номер телефона
        self.address = address


class CustKs(object):
    """
    Список клиентов квартирного сектора, элементы списка: объекты CustItemKs
    """
    def __init__(self, dsn):
        self.dsn = dsn
        self._customers = dict()
        self.pid_by_cid = dict()
        self.pid_by_number = dict()
        self.create_custlist()

    @property
    def customers(self):
        return self._customers

    def create_custlist(self):
        db = pymysql.Connect(**self.dsn)
        cursor = db.cursor()
        sql = "SELECT `pid`, `cid`, `tid`, `name` custname, `tel`, `xnumber`, `address` FROM `customers`.`CustKS`" \
              " WHERE LENGTH(`tel`) >3"
        cursor.execute(sql)
        for line in cursor:
            pid, cid, tid, custname, tel, xnumber, address = line
            self._customers[pid] = CustItemKs(pid, cid, tid, custname, tel, address)
            self.pid_by_cid[cid] = pid
            self.pid_by_number[xnumber[1:]] = pid    # 74996428318 -> 4996428318

        cursor.close()
        db.close()

    def get_cust(self, pid):
        """
        Возвращает всю информацию по клиенту квартирного сектора
        :param pid: код клиента
        :return: объект с информацией по клиенту
        """
        return self._customers.get(pid, '-')

    def get_pid_by_cid(self, cid):
        """
        Возвращает pid Физлица по его cid
        :param cid: код клиента в таблице customers.Cust
        :return: pid физ-лица
        """
        return self.pid_by_cid.get(cid, 0)

    def get_pid_by_number(self, number):
        """
        Возвращает pid Физлица по его телефонному номеру
        :param number: телефонный номер физ-лица
        :return: pid физ-лица
        """

        if len(number) == 11 and (number.startswith('8') or number.startswith('7')):
            number = number[1:]  # (8|7)4956261538 -> 4956261538
        return self.pid_by_number.get(number, 0)

    def print_cust(self):
        """
        Печать информации по всем клиентам квартирного сектора
        """
        for pid, p in self._customers.items():
            print("{pid} {name} {tel} {address}".format(pid=pid, name=p.custname, tel=p.tel, address=p.address))


class CustItem(object):
    """ Данные по клиенту """
    def __init__(self, cid, custalias, custname, uf, inn, kpp, dog_rss, dog_date_rss, dog_rsi, dog_date_rsi, tid_t):
        self.cid = cid                      # код клиента
        self.custalias = custalias          # краткое название клиента
        self.custname = custname            # полное название клиента
        self.uf = uf                        # uf = u|f - юр_лицо|физ_лицо
        self.inn = inn                      # ИНН
        self.kpp = kpp                      # КПП
        self.dog_rss = dog_rss              # договор клиента с РСС
        self.dog_date_rss = dog_date_rss    # дата договора клиента с РСС
        self.dog_rsi = dog_rsi              # договор клиента с РСИ
        self.dog_date_rsi = dog_date_rsi    # дата договора клиента с РСИ
        self.tid_t = tid_t                  # код тарифного плана телефонии, -> tariff.tariff_tel.tid


class Cust(object):
    """
    Список клиентов, элементы списка: объекты CustItem
    cst = Cust(dsn=dsn_cust)
    info = cst.getcust(273)  # info - объект CustItem
    print info.custalias, info.uf, ..
    """
    def __init__(self, dsn):
        self.dsn = dsn
        self._customers = dict()
        self.create_custlist()

    @property
    def customers(self):
        return self._customers

    def create_custlist(self):
        """
        Создание словаря клиентов
        """
        db = pymysql.Connect(**self.dsn)
        cursor = db.cursor()
        # sql = "SELECT CustID cid, CustAlias custalias, CustName custname, CustType uf, INN inn, KPP kpp, " \
        #      "`NumDTelRssMtc` dog_rss, `DateDTelRssMtc` dog_date_rss, `NumDTelRsiMtc` dog_rsi," \
        #      "`DateDTelRsiMtc` dog_date_rsi  FROM customers.Cust"
        sql = "SELECT CustID cid, CustAlias custalias, CustName custname, CustType uf, INN inn, KPP kpp, " \
              "`NumDTelRssMtc` dog_rss, `DateDTelRssMtc` dog_date_rss, 'dog_rsi' dog_rsi," \
              "'dog_date_rsi' dog_date_rsi, `tid_t`  FROM customers.Cust"

        cursor.execute(sql)
        for line in cursor:
            cid, custalias, custname, uf, inn, kpp, dog_rss, dog_date_rss, dog_rsi, dog_date_rsi, tid_t = line
            self._customers[cid] = CustItem(cid, custalias, custname, uf, inn, kpp, dog_rss, dog_date_rss, dog_rsi,
                                            dog_date_rsi, tid_t)
        cursor.close()
        db.close()

    def get_cust(self, cid):
        """
        Возвращает всю информацию по клиенту
        :param cid: код клиента
        :return: объект CustItem с информацией по клиенту
        """
        return self._customers.get(int(cid), '-')

    def get_tid_t(self, cid):
        """
        Возвращает код телефонного тарифного плана по клиенту
        :param cid: код клиента
        :return: tid_t - tariff_id for telephone
        """
        return self._customers.get(int(cid), '-').tid_t

    def print_cust(self):
        """
        Печать информации по всем клиентам
        """
        st = "{cid} {uf} {name} ({inn} {kpp}) ({dog_rss} {dog_date_rss}) ({dog_rsi} {dog_date_rsi}) (tid_t: {tid_t})"
        print(st)
        for cid, p in self._customers.items():
            print(st.format(cid=cid, uf=p.uf, name=p.custname, inn=p.inn, kpp=p.kpp, dog_rss=p.dog_rss,
                            dog_date_rss=p.dog_date_rss, dog_rsi=p.dog_rsi, dog_date_rsi=p.dog_date_rsi, tid_t=p.tid_t))


if __name__ == '__main__':

    print("\n# Клиенты:")
    cust = Cust(dsn=cfg.dsn_cust)
    # cust.print_cust()

    print("# инфо по клиентам:")
    for custid in (273, 999, 957, 319, 549):
        p = cust.get_cust(custid)
        print("{cid}  {uf}  {tid_t}  {name:20} {kpp}"
              .format(cid=p.cid, uf=p.uf, name=p.custalias, kpp=p.kpp, tid_t=p.tid_t))

    print("\n# Квартирный сектор:")
    custks = CustKs(dsn=cfg.dsn_cust)
    # custks.print_cust()
    for _pid in (490, 900, 901):
        p = custks.get_cust(_pid)
        print("{pid} {tid} {name:22} {address}".format(pid=p.pid, tid=p.tid, name=p.custalias, address=p.address))

    # pid by cid
    print("\n#Квартирный сектор - pid by cid")
    _cid = 793   # Иванов МП
    _pid = custks.get_pid_by_cid(_cid)
    p = custks.get_cust(_pid)
    print("cid: {cid} -> pid:{pid}  {name:22} {address}".format(cid=_cid, pid=p.pid, name=p.custalias, address=p.address))

    # pid by number
    print("\n#Квартирный сектор - pid by number")
    _number = '84956261538'
    _pid = custks.get_pid_by_number(_number)
    p = custks.get_cust(_pid)
    print("number: {number} -> pid:{pid}  {name:22} {address}".format(number=_number, pid=p.pid, name=p.custalias, address=p.address))

    print("\n#Клиенты со специальным тарифом")
    special = CustReplace(dsn=cfg.dsn_cust)
    special.print_cust()

    for _cid in (546, 900, 282):
        _cid_new = special.get_cid_new(_cid)
        print("cid={cid} -> cid_new={cid_new}".format(cid=_cid, cid_new=_cid_new))
