#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
a2.py - читает, дополняет и сохраняет в БД номера и клиентов А2  (tel.a2_numbers, tel.a2_customers)

исходные данные:
a2-numbers.csv - файл с номерами А2
a2-customers.csv - файл с клиентами А2

результат:
tel.a2_numbers - номера А2
tel.a2_customers - клиенты А2
"""
import os
import MySQLdb
import datetime
from modules import cfg
from modules import utils
from modules import progressbar

# root = os.path.realpath(os.path.dirname(sys.argv[0]))
owd = os.getcwd()
os.chdir('..')
root = os.getcwd()
os.chdir(owd)

logfile = "{root}/log/{file}".format(root=root, file='a2-numbers.log')
a2_numbers_file = "{root}/a2/{file}".format(root=root, file='a2-numbers.csv')
a2_customers_file = "{root}/a2/{file}".format(root=root, file='a2-customers.csv')
a2_template_tab_numbers = "{root}/sql/a2/{file}".format(root=root, file='table_a2_numbers.sql')
a2_template_tab_customers = "{root}/sql/a2/{file}".format(root=root, file='table_a2_customers.sql')

customer_ks_cid = 549  # код квартирного сектора

# клиенты из данных А2 которых невозможно найти автоматически в клиентской базе РСС
a2_cust2cid = {
    'Анштальт': 1259,
    'Арутюнова Е.В.': 1264,
    'Беликов С.Г 626-92-61': 738,
    'Белтадзе Ю.Т.': 918,
    'Битюков В.М. 626-15-38': 991,
    'Бускин В.Д.': 830,
    'Бускин В.Д. 630=   626-94-45': 830,
    'Гаврилова Е.В. 626-15-03': 865,
    'Голованова Н.Ю. 626-19-06(Солдатов)': 539,
    'Горанский Николай Борисович': 1258,
    'Горохов А.В. 535=': 1263,
    'Иванов М.П. (499)': 793,
    'Карташов В.Р.  626  (630=)': 1082,
    'Кондитерский ДОМ (626)': 297,
    'Красвеника  (626)':  574,
    'Краюхин А.И. /535р/  626-14-92': 997,
    'МЕГАФОН': 0,
    'МРП ООО': 41,  # ?
    'МТС  ПАО  с 03 июля 2015Г': 0,
    'Нестерова А.А': 818,
    'Олейников В.Ф.  626-96-86': 1059,
    'Олимп Коннект  ТО': 1255,
    'Перепелкина И.И. /535р./ 626-14-38': 985,
    'Разживин В.М. 626-14-19': 984,
    'Русаков В.А.(499) (500р.)': 775,
    'Русская Лапландия ООО (499)': 1148,
    'Сафонова Т.Я.(499)/500р./': 1149,
    'Северный порт АО  ИНТ с 01.01.2020 г.': 58,
    'Северный порт АО (499) с 01.01.2020г.': 58,
    'Северный порт АО (Пр. провода) с 01.01.2020': 58,
    'Соболев Д.А. (ИП)': 1246,
    'Тебякин А.В.': 758,
    'ТРАНЗИТЕК ООО': 0,
    'ЦТС': 118,
    '"""Шелдонфарма"" ООО"': 1260,
    'Шумихин А.Е.': 849,
    'Шумихин А.Е. 626-94-87': 849,
    'Якунин Валерий Николаевич (размещ. оборудов., б/договора)': 806,

    'Альконт ООО(Долгопрудный)': 722,
    'Бабичев С.В.': 1087,
    'ВодоходЪ': 648,
    'ВодоходЪ(499)': 648,
    'Волчков П.Б.': 281,
    'ДМ Текстиль ТПК': 892,
    'ИП Любимая Ю.В.': 1240,
    'КАТЕР-СЕРВИС (ХМСЗ)  626': 964,
    'РВЛ-СТРОЙ (626)': 53,
    'РВЛ-строй': 53,
    'Речной регистр': 37,
    'РЕЧНОЙ РЕГИСТР (626)': 37,
    'Речной регистр(499)': 37,
    'ХМСЗ (626)': 273,
    'ХМСЗ (Интернет)': 273,
    'Чайка интернет 10944,00 не начислять до 01.01.2021г.': 70,

}


def log(msg):
    """
    логирование
    """
    global logfile
    f = open(logfile, "a")
    date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    f.write("{date} {msg}\n".format(date=date, msg=msg))
    f.close()


def is_digit(string):
    if string.isdigit():
        return True
    else:
        try:
            float(string)
            return True
        except ValueError:
            return False


class A2(object):
    """ Номера и клиеты А2 """
    def __init__(self, dsn_customers, dsn_numbers, numbers_file, customers_file, tab_a2_numbers, tab_a2_customers,
                 template_tab_a2_numbers, template_tab_a2_customers):
        """
        :param dsn_customers: dsn клиентов
        :param dsn_numbers: dsn телефонных номеров
        :param numbers_file: имя csv-файла с номерами
        :param customers_file: имя csv-файла с клиентами
        :param tab_a2_numbers: таблица с номерами a2
        :param tab_a2_customers: таблица с клиентами a2
        :param template_tab_a2_numbers: шаблон для создания таблицы tab_a2_numbers
        :param template_tab_a2_customers: шаблон для создания таблицы tab_a2_customers

        """
        self.dsn_customers = dsn_customers
        self.dsn_numbers = dsn_numbers
        self.numbers_file = numbers_file
        self.customers_file = customers_file
        self.tab_a2_numbers = tab_a2_numbers
        self.tab_a2_customers = tab_a2_customers
        self.template_tab_a2_numbers = template_tab_a2_numbers
        self.template_tab_a2_customers = template_tab_a2_customers
        self.xnumber2cid = dict()       # мапа xnumber -> cid
        self.xnumber2number = dict()    # мапа xnumber -> number
        self.cid2name_rss = dict()      # мапа cid -> name customer RSS
        self.cid2name_a2 = dict()       # мапа cid -> name customer A2
        self.cid2inn = dict()           # мапа cid -> inn
        self.inn2cid = dict()           # мапа inn -> cid
        self.dog_inet2cid = dict()       # мапа dog_internet -> cid (NumDInetRssOOO)
        self.ks_xnumbers = dict()       # мапа ks_xnumber -> инфо-словарь для КС: xnumber -> {pid, cid, name, number}
        self._create_maps_from_customers()
        self._create_xnumber2cid()
        self._create_ks_info()

    def _create_ks_info(self):
        """
        Создание словаря с инфо по номерам КС (квартирного сектора)
        :return: кол-во элементов словаря
        """
        db = MySQLdb.Connect(**self.dsn_customers)
        cursor = db.cursor()
        sql = "SELECT pid, cid, name, xnumber, tel number FROM {table}".format(table='customers.CustKS')

        cursor.execute(sql)
        for line in cursor:
            pid, cid, name, xnumber, number = line
            self.ks_xnumbers[xnumber] = dict(pid=pid, cid=cid, name=name, number=number)

        cursor.close()
        db.close()
        return len(self.ks_xnumbers)

    def _create_xnumber2cid(self):
        """
        Создание мапы(словаря) number -> cid
        :return: количество элементов в словаре xnumber2cid
        """
        db = MySQLdb.Connect(**self.dsn_numbers)
        cursor = db.cursor()
        sql = "SELECT cid, xnumber, number FROM {table}".format(table='telefon.tel')

        cursor.execute(sql)
        for line in cursor:
            cid, xnumber, number = line
            self.xnumber2cid[xnumber] = cid
            self.xnumber2number[xnumber] = number

        cursor.close()
        db.close()
        return len(self.xnumber2cid)

    def _create_maps_from_customers(self):
        """
        Создание мап(словарей): cid2name_rss, cid2inn, inn2cid, dog_inet2cid
        :return: количество элементов в словаре cid2name_rss
        """
        db = MySQLdb.Connect(**self.dsn_customers)
        cursor = db.cursor()
        sql = "SELECT CustID cid, CustAlias, INN, NumDInetRssOOO dog_inet FROM {table}".format(table='customers.Cust')

        cursor.execute(sql)
        for line in cursor:
            cid, cust_alias, inn, dog_inet = line
            self.cid2name_rss[cid] = cust_alias
            self.cid2inn[cid] = inn
            if inn:
                self.inn2cid[inn] = cid
            if dog_inet:
                self.dog_inet2cid[dog_inet] = cid

        cursor.close()
        db.close()
        return len(self.cid2name_rss)

    def get_customers_a2(self):
        return self.cid2name_a2

    def save_a2_number_base(self, records, table='a2_numbers'):
        """
        Сохранение номеров а2 в базе
        :param records: массив по номерам в виде словаря
        :param table: таблица для номеров
        :return: количество добавленных записей
        """
        db = MySQLdb.Connect(**self.dsn_numbers)
        cursor = db.cursor()

        # sql = "TRUNCATE TABLE `{table}`".format(table=table)
        sql = "DELETE FROM `{table}`".format(table=table)

        cursor.execute(sql)

        step = 0

        for d in records:
            sql = "INSERT INTO {table} (`number`, `xnumber`, `cid`, `pid`, `prim`) " \
                  "VALUES ('{number}', '{xnumber}', '{cid}', '{pid}', '{prim}')".\
                format(table=table, number=d['number'], xnumber=d['xnumber'], cid=d['cid'], pid=d['pid'],
                       prim=d['prim'])
            cursor.execute(sql)
            step += 1

        cursor.close()
        db.close()
        return step

    def update_numbers_base(self, numbers):
        db = MySQLdb.Connect(**self.dsn_numbers)
        cursor = db.cursor()

        sql = "UPDATE `tel` SET `a2`='-'"
        cursor.execute(sql)
        updated1 = cursor.rowcount

        sql = "UPDATE `tel` SET `a2`='+' WHERE `xnumber` IN ({numbers})".format(numbers=','.join(numbers))
        cursor.execute(sql)
        updated2 = cursor.rowcount

        print("updated numbers: {updated1}/{updated2} (a2='-'/a2='+')".format(updated1=updated1, updated2=updated2))

        cursor.close()
        db.close()

    def read_and_save_numbers(self):

        requests, numbers = self.read_numbers()
        self.save_numbers_in_base(requests)
        self.update_numbers_base(numbers)

    def read_numbers(self):
        """
        Чтение и дополнение необходимой информацией клиентов a2
        :return: кортеж (список команд INSERT для встаки, список номеров)
        """

        # fields = ('number', 'xnumber', 'cid', 'pid', 'prim')
        numbers_insert = []     # список запросов INSERT для всех строк из файла
        numbers_a2 = []         # список номеров а2

        d = dict()
        step, rows = (0, utils.get_file_rows(self.numbers_file))
        for line in open(self.numbers_file):
            if line.startswith('#'):
                continue
            step += 1

            line = ''.join([' ' if ord(i) == 160 else i for i in line])

            d['xnumber'], cust_a2, undef = line.strip().split(';')
            cid = self.xnumber2cid.get(d['xnumber'], 0)
            cust_name_rss = self.cid2name_rss.get(cid, '')
            # inn = self.cid2inn.get(cid, '')
            self.cid2name_a2[cid] = cust_a2

            numbers_a2.append(d['xnumber'])

            pid = 0
            if cid == customer_ks_cid:
                ks = self.ks_xnumbers.get(d['xnumber'], {})
                if ks:
                    pid = ks['pid']
                    cust_name_rss = ks['name']

            d['number'] = self.xnumber2number.get(d['xnumber'], '-')
            d['cid'] = cid
            d['pid'] = pid
            d['prim'] = cust_name_rss

            numbers_insert.append(self._get_insert(table=self.tab_a2_numbers, dict_kv=d))
        return numbers_insert, numbers_a2

    def read_and_save_customers(self):

        requests = self.read_customers()
        self.save_customers_in_base(requests)

    def read_customers(self):
        """
        Чтение и дополнение необходимой информацией клиентов a2
        :return: список команд INSERT для встаки
        """
        # fields = (
        #     'nn', 'сust_a2', 'cust_rss', 'cid', 'inn', 'serv_space', 'bank_account', 'address_fact', 'address_u',
        #     'phone', 'email',
        #     'inet_dog', 'inet_speed', 'inet_sum_nds',
        #     'tel626_dog', 'tel626_sum_nds',
        #     'tel642_dog', 'tel642_sum_nds',
        #     'mg_dog', 'mg_sum_nds',
        #     'to_dog', 'to_sum_nds',
        #     'pp_dog', 'pp_sum_nds',
        #     'all_sum_nds'
        # )
        fields_number = ('inet_sum_nds', 'tel626_sum_nds', 'tel642_sum_nds', 'mg_sum_nds', 'to_sum_nds', 'pp_sum_nds',
                         'all_sum_nds')
        customers_insert = []     # список запросов INSERT для всех строк из файла

        d = dict()
        step, rows = (0, utils.get_file_rows(self.customers_file))
        for line in open(self.customers_file):
            if line.startswith('#'):
                continue
            step += 1

            line = ''.join([' ' if ord(i) == 160 else i for i in line])

            # nn, cust_a2, inn, serv_space, bank_account, address_fact, address_u, phone, email, inet_dog, inet_speed,
            # inet_sum_nds, tel626_dog, tel626_sum_nds, tel642_dog, tel642_sum_nds, mg_dog, mg_sum_nds, to_dog,
            # to_sum_nds, pp_dog, pp_sum_nds, all_sum_nds = line.strip().split(';')
            d['nn'], d['cust_a2'], d['inn'], d['serv_space'], d['bank_account'], d['address_fact'], d['address_u'],\
                d['phone'], d['email'], d['inet_dog'], d['inet_speed'], d['inet_sum_nds'], d['tel626_dog'], \
                d['tel626_sum_nds'], d['tel642_dog'], d['tel642_sum_nds'], d['mg_dog'], d['mg_sum_nds'], d['to_dog'], \
                d['to_sum_nds'], d['pp_dog'], d['pp_sum_nds'], d['all_sum_nds'] = line.strip().split(';')

            # для полей с претензией на числовое значение
            for f in fields_number:
                d[f] = self._str2number(d[f])
                if d[f] == '':
                    d[f] = 0

            if d['inet_speed'] == '':
                d['inet_speed'] = 0

            inet_dog = self._get_inet_dog(d['inet_dog'])

            # cid по прямому соответствию из таблицы
            cid = a2_cust2cid.get(d['cust_a2'].strip(), 0)

            if cid == 0:
                # определяем cid по ИНН
                cid = self.inn2cid.get(d['inn'], 0)
                if cid == 0:
                    cid = self.dog_inet2cid.get(inet_dog, 0)  # либо по дооговору по интернет

            d['cid'] = cid
            d['cust_rss'] = self.cid2name_rss.get(cid, '')

            customers_insert.append(self._get_insert(table=self.tab_a2_customers, dict_kv=d))
        return customers_insert

    def save_customers_in_base(self, requests):
        """
        Сохраняет клиентов А2 в базе по списку запросов
        :param requests: список запросов
        :return: кол-во сохранённых записей
        """
        db = MySQLdb.Connect(**self.dsn_numbers)
        cursor = db.cursor()

        utils.create_table_if_no_exist(dsn=self.dsn_numbers, table=self.tab_a2_customers,
                                       tab_template=self.template_tab_a2_customers)

        cursor.execute("DELETE FROM {table}".format(table=self.tab_a2_customers))

        step, rows = (0, len(requests))
        bar = progressbar.Progressbar(info='a2_customers', maximum=rows)

        for request in requests:
            cursor.execute(request)
            step += 1
            bar.update_progress(step)

        cursor.close()
        db.close()
        bar.go_new_line()

        return step

    def save_numbers_in_base(self, requests):
        """
        Сохраняет номера А2 в базе по списку запросов
        :param requests: список запросов
        :return: кол-во сохранённых записей
        """
        db = MySQLdb.Connect(**self.dsn_numbers)
        cursor = db.cursor()

        utils.create_table_if_no_exist(dsn=self.dsn_numbers, table=self.tab_a2_numbers,
                                       tab_template=self.template_tab_a2_numbers)

        cursor.execute("DELETE FROM {table}".format(table=self.tab_a2_numbers))

        step, rows = (0, len(requests))
        bar = progressbar.Progressbar(info='a2_numbers', maximum=rows)

        for request in requests:
            cursor.execute(request)
            step += 1
            bar.update_progress(step)

        cursor.close()
        db.close()
        bar.go_new_line()

        return step

    def _get_inet_dog(self, inet_dog_date):
        """
        Возвращает номер договора из строки
        :param inet_dog_date: номер договора и дата
        :return: номер договора
        """
        inet_dog = ''
        a = inet_dog_date.split()   # 411И/2013 от 13.12.2013г.
        if a:
            inet_dog = a[0].strip()
        return inet_dog

    def _get_insert(self, table, dict_kv):
        """
        Возвращает строку INSERT INTO
        :param table: таблица для вставки
        :param dict_kv: словарь пар key=value
        :return: строка INSERT INTO ...
        """

        # "INSERT INTO {table} (`a`,`b`,`c`) VALUES ('{a}','{b}','{c}')"

        fields = list(dict_kv.keys())
        fields.sort()

        fields_in_backticks = ["`{f}`".format(f=f) for f in fields]
        values = ["'{v}'".format(v=dict_kv[f]) for f in fields]

        string = "INSERT INTO {table} ({fields}) VALUES ({values})".\
            format(table=table, fields=','.join(fields_in_backticks), values=','.join(values))

        return string

    def _str2number(self, string):
        """
        Преобразует строку с псевдо-числом в число
        :param string: '55 164,75'
        :return: 55164.75
        """

        string = string.replace(' ', '').replace(',', '.')
        if not is_digit(string):
            string = 0
        return string


if __name__ == '__main__':

    a2 = A2(dsn_customers=cfg.dsn_cust, dsn_numbers=cfg.dsn_tel, numbers_file=a2_numbers_file,
            customers_file=a2_customers_file, tab_a2_numbers='a2_numbers', tab_a2_customers='a2_customers',
            template_tab_a2_numbers=a2_template_tab_numbers, template_tab_a2_customers=a2_template_tab_customers)

    a2.read_and_save_numbers()
    a2.read_and_save_customers()
