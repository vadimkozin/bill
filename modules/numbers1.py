#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
numbers1 - модуль работы с номерами
состоит из:
 Numbers811 - ведомственный МГ ФГУП РСИ (811xxxx и 812xxxx)
 Numbers - городские номера (626xxxx, 642xxxx, 710xxxx, 627xxxx) из telefon.tel
"""
import re
import pymysql
from io import open
from cfg import cfg
from modules.func import Func


class Number811(object):
    """
    Номера ведомственного МГ ФГУП РСИ (811xxxx, 812xxxx) из tarif.vmCode и tarif.vmTar
    """
    rex = re.compile('x', re.IGNORECASE)

    @staticmethod
    def tr_simple(simple):
        return Number811.rex.sub('.{1}', simple)  # 8112[3-9]xx => 8112[3-9].{1}.{1}

    def __init__(self, dsn):
        """
        Читает все номера 811xxxx 812xxxx в словарь self.numbers
        :param dsn: dsn базы
        self.numbers[] = dict(id, clid, cid, org, simple_raw, simple, name, zona, tar2008, tar0820, tarW)
        """
        self.numbers = []
        db = pymysql.Connect(**dsn)

        cur = db.cursor()
        # code + tariff
        sql = "select c.id, c.clid, c.cid, c.org, c.simple, c.name, c.zona, t.tar2008, t.tar0820, t.tarW" \
            " from tarif.vmCode c join tarif.vmTar t on c.zona=t.ZonaID where fbil='+'"

        cur.execute(sql)
        for line in cur:
            id_, clid, cid, org, simple, name, zona, tar2008, tar0820, tarw = line
            spl = Number811.tr_simple(simple)
            m = re.compile(spl)
            self.numbers.append(dict(id=id_, clid=clid, cid=cid, org=org, simple_raw=spl, simple=m, name=name,
                                     zona=zona, tar2008=tar2008, tar0820=tar0820, tarW=tarw))
        cur.close()
        db.close()

    def getob(self, num):
        """
        Возвращает всю информацию в виде словаря для номера num
        ex.  ob = getob('8113300');  print ob['cid'], ob['zona'], ...
        :param num: 811xxxx or 812xxxx
        :return: dict(id, clid, cid, org, simple_raw, simple, zona, tar2008, tar0820, tarW )
        """
        for n in self.numbers:
            if n['simple'].search(num):
                return n
        return None

    def getcid(self, num):
        """
        Возвращает код клиента для номера num
        :param num: 811xxxx or 812xxxx
        :return: код клиента (cid) или  0
        """
        for n in self.numbers:
            if n['simple'].search(num):
                return n['cid']
        return 0

    def get_cidorg(self, num):
        """
        Возвращает строку: код клиента и код организации для номера num
        :param num: 811xxxx or 812xxxx
        :return: 'cid,org', ex. '273;R'
        """
        for n in self.numbers:
            if n['simple'].search(num):
                return Numbers.join_cidorg(n['cid'], n['org'])
        return Numbers.join_cidorg(0, '-')

    def count_numbers(self):
        """
        Возвращает количество городских номеров
        """
        return len(self.numbers)

# end class Number811


class Numbers(object):
    """
    Городские номера из telefon.tel
    """
    numbers = dict()                    # ex. numbers['6261626']='282;R'
    numadd = ('6269000',)
    re811 = re.compile('^81[12].{4}$')  # 811xxxx or 812xxxx
    numbers_vpn = dict()                # номера ВПН : ex. numbers_vpn[6261099]='84;G'
    aon_replace = {
        '15820': {'number': '84956275272', 'cid': '17', 'org': 'G'},    # Зис(17)
        '15821': {'number': '84956275273', 'cid': '17', 'org': 'G'},
        '15822': {'number': '84957107371', 'cid': '17', 'org': 'G'},
        '25360': {'number': '84996428464', 'cid': '58', 'org': 'R'},    # Северный порт(58)
        '25355': {'number': '84996428481', 'cid': '58', 'org': 'R'},    # Северный порт(58)
        '25331': {'number': '84996428466', 'cid': '58', 'org': 'R'},    # Северный порт(58) c 12-07-2017
        '25364': {'number': '84996428464', 'cid': '58', 'org': 'R'},  # Северный порт(58) c 2019-
        '15824': {'number': '84996275285', 'cid': '188', 'org': 'R'},  # Авангард Холго(188) с 30-04-2019 по факту


    }


    def __init__(self, dsn, table, n811):
        """
        Все городские номера (xxxxxxx) из telefon.tel
        :param dsn: dsn - инфо подключения к telefon.tel
        :param table: период, ex. Y2016M02 - нужен для инфо по номерам из истории за период
        :param n811: instance Number811
        """
        self.n811 = n811
        db = pymysql.Connect(**dsn)
        cur = db.cursor()

        # текущие номера: (поле fSplit - маркирует номера ВПН : + or -)
        sql = "select `number`, `cid`, `fOrg` `org`, `fSplit` `vpn` FROM `telefon`.`tel` where `number` like '_______'"
        cur.execute(sql)
        for line in cur:
            num, cid, org, vpn = line
            n = num[:7]
            if self.numbers.get(n, 0) == 0:
                self.numbers[n] = Numbers.join_cidorg(cid, org)  # 'cid;org'
            if vpn == '+':
                self.numbers_vpn[n] = Numbers.join_cidorg(cid, org)  # 'cid;org'

        # номера из истории за расчётный месяц
        dt = Func.date_first_day_month(table)
        sql = "select `number`, `cid`, `org` from `telefon`.`history` where `number` like '_______' and d2>='{dt}'".\
            format(dt=dt)
        cur.execute(sql)
        for line in cur:
            num, cid, org = line
            n = num[:7]
            if self.numbers.get(n, 0) == 0:
                self.numbers[n] = Numbers.join_cidorg(cid, org)  # 'cid;org'

        cur.close()
        db.close()

        # добавочные номера
        for n in self.numadd:
            if self.numbers.get(n, -1) == -1:
                self.numbers[n] = "0;-"    # '0;-'

        # дополнительно
        for aon, v in list(self.aon_replace.items()):
            if self.numbers.get(aon, -1) == -1:
                self.numbers[aon] = Numbers.join_cidorg(int(v['cid']), v['org'])


    @staticmethod
    def join_cidorg(cid, org):
        """
        Склейка кода клиента и организации
        :param cid: код клиента (84, 273, ..)
        :param org: код организации (R,G,I)
        :return: 'cid;org'
        """
        return str(cid) + ";" + str(org)

    @staticmethod
    def split_cidorg(val):
        """
        Разделение кода клиента от организации
        :param val: 'cid;org' (ex. '84;G')
        :return: (cid,org)  ex. (84, 'G')
        """
        return val.split(';')   # (cid, org)

    def count_numbers(self):
        """
        Возвращает количество городских номеров
        """
        return len(self.numbers)

    def _get_cidorg(self, num):
        """
        По номеру возвращает строку "cid;org" (код клиента и принадлежность к организации)
        :param num: номер: 8495626xxxx or 626xxxx or 811xxxx or 710xxxx, ...
        :return: строку 'cid;org' или 0
        """
        num = Func.get_number_7digits(num)

        # сначала ищем в telefon.tel, а затем в таблице tarif.vmCode
        cidorg = self.numbers.get(num, None)
        if not cidorg:
            cidorg = self.n811.get_cidorg(num)

        return cidorg

    def get_cidorg(self, num, num2):
        """
        По 2-м номерам fm/fmx определяет (cid, org, flag_vpn) код клиента, организация (R,G), флаг_впн
        :param num: первый номер из пары fm/fmx : 8117251/84956261545, 6275272/84996428495, 84996428441/84996428441
        :param num2: второй номер из пары fm/fmx : 8117251/84956261545
        :return: (cid, org, flag_vpn) или (0, '-', False)
        """

        # если номер ВПН, то определяем cid+org только по городскому номеру
        vpn = False
        if self.is_vpn(num2):
            cidorg = self._get_cidorg(num2)
            vpn = True
        else:
            # 1-я попытка по fm (должен быть номер клиента городской 642/626 или внутренний 811)
            cidorg = self._get_cidorg(num)
            if cidorg[0] == '0':
                # 2-я потытка по fmx (должен быть городской 8495626xxxxx/8499642xxxxx, иногда внутренний номер 81xxxxx)
                cidorg = self._get_cidorg(num2)

        cidorgvpn = list(Numbers.split_cidorg(cidorg))
        cidorgvpn.append(vpn)
        return cidorgvpn

    def get_cidorg_2(self, num, num2):
        """
        По 2-м номерам fm/fmx определяет (cid, org) код клиента и принадлежность к организации (R,G)
        :param num: первый номер из пары fm/fmx : 8117251/84956261545, 6275272/84996428495, 84996428441/84996428441
        :param num2: второй номер из пары fm/fmx : 8117251/84956261545
        :return: (cid, org) или (0, '-')
        """
        # 1-я попытка по fm (должен быть номер клиента городской 642/626 или внутренний 811)
        cidorg = self._get_cidorg(num)
        if cidorg[0] == '0':
            # 2-я потытка по fmx (должен быть городской 8495626xxxxx/8499642xxxxx, иногда внутренний номер 81xxxxx)
            cidorg = self._get_cidorg(num2)


        # если номер ВПН, то определяем cid+org только по городскому номеру
        vpn = False
        if self.is_vpn(num2):
            cidorg = self._get_cidorg(num2)
            vpn = True

        return Numbers.split_cidorg(cidorg)

    def get_cidorg_1(self, num, num2):
        """
        По 2-м номерам fm/fmx определяет (cid, org) код клиента и принадлежность к организации (R,G)
        :param num: первый номер из пары fm/fmx : 8117251/84956261545, 6275272/84996428495, 84996428441/84996428441
        :param num2: второй номер из пары fm/fmx : 8117251/84956261545
        :return: (cid, org) или (0, '-')
        """
        # 1-я попытка по fmx (должен быть наш городской номер: 8499642xxxx | 8495626xxxx)
        cidorg = self._get_cidorg(num2)
        if cidorg[0] == '0':
            # 2-я потытка по fm (может быть как городской, так и внутренний номер 81xxxxx)
            cidorg = self._get_cidorg(num)

        return Numbers.split_cidorg(cidorg)

    def get_vpn_cidorg(self, num):
        """
        Проверка номера на ВПН и если номер ВПН, то :
        возвращает кортеж (cid,org) (код клиента и принадлежность к организации)
        :param num: номер: 8495626xxxx or 626xxxx  ...
        :return: кортеж (cid,org) или (0,'-') если не номер не ВПН
        """
        num = Func.get_number_7digits(num)
        cidorg = self.numbers_vpn.get(num, "0;-")
        return Numbers.split_cidorg(cidorg)

    def is_vpn(self, num):
        """
        возвращает True/False - номер ВПН или нет
        """
        n = Func.get_number_7digits(num)
        #if n in self.numbers_vpn:
        #    return True
        #else:
        #    return False
        return n in self.numbers_vpn

    def n811(self):
        return self.n811
# end class Numbers


class Numreplace(object):
    """
    Номера замены : городской номер заменяет внутренние
    84956261626 => '811720[13468]|8117287|8117252|811721[89]'
    84956261901 => '81171XX'
    """
    def __init__(self, dsn, table, fname_rss, fname_inf, numbers, load=False):

        self.dsn = dsn               # параметры подсоединения к базе
        self.table = table           # таблица куда загружаем
        self.fname_rss = fname_rss   # имя файла откуда берём (РСС)
        self.fname_inf = fname_inf   # имя файла откуда берём (РСИ)
        self.numbers = numbers       # объект Numbers с инфо по номерам
        self.numrep = dict()         # словарь с номерами замены ( '84956261901'=>'81171XX', .. )
        self.numrep_cidorg = dict()  # словарь с инфо по cid и org для номеров замены ('84956261901'=>'787_R', ..)

        if load:
            self._load_number_replace()

        db = pymysql.Connect(**self.dsn)
        cur = db.cursor()
        cur.execute("SELECT `number`, `inter`, `cid`, `org` FROM `telefon`.`{table}`".format(table=self.table))
        for line in cur:
            number, inter, cid, org = line
            if number in self.numrep:
                self.numrep[number] += "|{inter}".format(inter=inter)
            else:
                self.numrep[number] = inter
            self.numrep_cidorg[number] = self.numbers.join_cidorg(cid, org)

        cur.close()
        db.close()

    def _load_number_replace(self):
        # загрузка номеров замены
        db = pymysql.Connect(**self.dsn)
        cur = db.cursor()
        # cur.execute("truncate table `telefon`.`{table}`".format(table=self.table))
        cur.execute("delete from `telefon`.`{table}`".format(table=self.table))

        # РСС (org=R)
        # org = 'R'
        f = open(self.fname_rss, encoding='utf8')
        step = 0
        for line in f:
            number, inter = line.split()
            cid, org = self.numbers.get_cidorg(number, number)
            sql = "insert into `{table}` (`cid`, `number`, `inter`, `org`) values ('{cid}','{number}', '{inter}', " \
                  "'{org}')".format(table=self.table, cid=cid, number=number, inter=inter, org=org)
            cur.execute(sql)
            step += 1
        f.close()

        # ФГУП РСИ (org=I)
        # org = 'G'
        f = open(self.fname_inf, encoding='utf8')
        for line in f:
            number, inter = line.split()
            cid, org = self.numbers.get_cidorg(number, number)
            sql = "insert into `{table}` (`cid`, `number`, `inter`, `org`) values ('{cid}','{number}', '{inter}', " \
                  "'{org}')".format(table=self.table, cid=cid, number=number, inter=inter, org=org)

            cur.execute(sql)
            step += 1
        f.close()

        cur.close()
        db.close()
        return step

    def print_numbers(self):
        """
        Вывод на печать всей таблицы замены: городской->что замещает
        """
        keys = list(self.numrep.keys())
        keys.sort()
        for k in keys:
            cid, org = self.numbers.split_cidorg(self.numrep_cidorg[k])
            cid_org = "{cid}/{org}".format(cid=cid, org=org)
            print("{cid_org:7}  {number:12}  {replace}".format(cid_org=cid_org, number=k, replace=self.numrep[k]))
        print("всего городских номеров для замены: {size}".format(size=self.count_numbers()))

    def print_inter(self):
        """
        Вывод на печать всей таблицы замены: замещаемый->городской
        """
        step = 0
        x = dict()
        for number, inters in self.numrep.items():
            alist = inters.split('|')   # 811720[13468]|8117287|8117252|811721[89]
            for q in alist:
                x[q] = number

        repl_numbers = list(x.keys())
        repl_numbers.sort()

        for q in repl_numbers:
            number = x[q]
            cid, org = self.numbers.split_cidorg(self.numrep_cidorg[number])
            cid_org = "{cid}/{org}".format(cid=cid, org=org)
            step += 1
            print("{cid_org:7} {inter:16} {number}".format(cid_org=cid_org, inter=q, number=number))
        print("всего замещаемых номеров(шаблонов): {size}".format(size=step))

    def count_numbers(self):
        """
        Возвращает количество городских номеров для подмены
        """
        return len(self.numrep)

    def number2inter(self, number):
        """
        По городскому номеру возвращаются номера замены
        """
        return self.numrep.get(number, '-')

    def inter2number(self, num_replace):
        """
        По номеру замены возвращается городской номер
        """
        step = 0
        for number, inters in self.numrep.items():
            step += 1
            # 811720[13468]|8117287|8117252|811721[89]
            # 8120XXX
            alist = inters.split('|')
            for q in alist:
                q = q.replace('X', '.')
                if re.match(q, num_replace):
                    return number
        return '-'


if __name__ == '__main__':

    # номера
    num811 = Number811(dsn=cfg.dsn_tar)
    nm = Numbers(dsn=cfg.dsn_tel, table='Y2016M01', n811=num811)

    # загрузка номеров замены в таблицу telefon.num_replace
    nr = Numreplace(dsn=cfg.dsn_tel, table='num_replace', fname_rss='./sql/num_replace_rss.txt',
                    fname_inf='./sql/num_replace_inf.txt', numbers=nm, load=False)
    print("\n# городские номера для замены")
    nr.print_numbers()

    print("\n# замещаемые номера")
    nr.print_inter()

    print("\n# по городскому найдем что он замещает:")
    for _num in ('84996428469', '84996428482', '84996428495', '4956269000'):
        _cid, _org, _vpn = nm.get_cidorg(_num, _num)
        _cid_org = "{cid}/{org}".format(cid=_cid, org=_org)
        print("{number:11}  {cid_org:8} {replace}".format(number=_num, replace=nr.number2inter(_num), cid_org=_cid_org))

    print("\n# по замещаемому найдем городской:")
    for _inter in ('8117201', '8125252', '8125258', '6271111', '7100000', '8117200', '1234567',):
        _number = nr.inter2number(_inter)
        _cid, _org, _vpn = nm.get_cidorg(_number, _number)
        _cid_org = "{cid}/{org}".format(cid=_cid, org=_org)
        print("{inter}  {replace}  {cid_org:6}".format(inter=_inter, replace=_number, cid_org=_cid_org))

    # exit(1)

    # номера ВМ ФГУП РСИ
    print("\n# 811xxxx 812xxxx")
    num811 = Number811(dsn=cfg.dsn_tar)
    for _num in ('8113300', '8117100', '8117500'):
        _cid = num811.getcid(_num)
        ob = num811.getob(_num)
        if ob:
            print("num:{num} cid:{cid:<7} org:{org}  zona:{zona}  tar0820={tar0820}  name:{name}".\
                format(num=_num, cid=_cid, tar0820=ob['tar0820'], zona=ob['zona'], name=ob['name'], org=ob['org']))
        else:
            print("num:{num} cid:{cid:<7} org:{org}  zona:{zona}  tar0820={tar0820}  name:{name}".\
                format(num=_num, cid=_cid, tar0820=None, zona=None, name=None, org=None))

    print("всего номеров Вед. связи: {count_numbers}".format(count_numbers=num811.count_numbers()))

    # номера из telefon.tel
    print("\n# telefon.tel")
    nm = Numbers(dsn=cfg.dsn_tel, table='Y2016M01', n811=num811)
    for _num in ('84996428280', '6261626', '6261545', '6428200', '7108046', '6275273', '8117500'):
        _cid, _org, _vpn = nm.get_cidorg(_num, _num)
        print("num:{num} cid:{cid:7} org:{org}".format(num=_num, cid=_cid, org=_org))

    print("всего городских номеров: {count_numbers}".format(count_numbers=nm.count_numbers()))

    print("\n# по паре fm/fmx определяем (cid;org)")
    for fm_fmx in ('6275272;84996428495', '81261xx;81261xx','8117254;8117254', '8117219;84956261626',
                   '8117217;84956261545', '6261626;6261626'):
        _fm, _fmx = fm_fmx.split(';')
        _cid, _org, _vpn = nm.get_cidorg(_fm, _fmx)
        print("{fm}/{fmx:15} cid:{cid:7} org:{org}".format(fm=_fm, fmx=_fmx, cid=_cid, org=_org))

    print("\n# проверка номера на ВПН")
    for _num in ('6275272', '6261099', '6261545', '6261750', '6269045', '6269526', '8117172'):
        print("{num} : {vpn}".format(num=_num, vpn=nm.is_vpn(_num)))
