#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
tarmts - модуль тарификации (МТС)
"""
import MySQLdb
from io import open
from modules import cfg
from modules import codedef
from modules.func import Func
from modules import calltype
from modules import customers


class Tarmts(object):
    """
    тарифы МТС
    """
    def __init__(self, dsn, tabcode, tabtar, tabtar2, tabtarmts, stat, cdef, custdefault, tiddefault=1):
        """
        :param dsn: dictionary dsn     (dsn_tar)
        :param tabcode: table of code  (komstarCode)
        :param tabtar: table of tariff (komstarTar)
        :param tabtar2: table of tariff (komstarTarRss)
        :param tabtarmts: table of tariff (mtsTar)
        :param stat: object Calltype
        :param cdef: object codedef.Codedef
        :param custdefault: customer default for tariff (953)
        :param tiddefault: tariff id default (1..5)
        """
        self.dsn = dsn
        self.tabcode = tabcode   # таблица tarif.komstarCode
        self.tabtar = tabtar     # таблица tarif.komstarTar
        self.tabtar2 = tabtar2   # таблица tarif.komstarTarRss
        self.tabtarmts = tabtarmts  # таблица tarif.mtsTar (таблица с клиентскими тарифами с 2020-12)
        self.stat = stat         # объект Calltype (тип звонка: MWSZVG)
        self.cdef = cdef         # объект Codedef (СПС)
        self.names = dict()      # names['797'] = dict(name='N.Novgorod', zona=3, type='MG', tara=2.34)
        self.codes = list()      # сорт.список: code+nid: ('7831_797_MN') : 7831-tel.code; 797-nid; MN-мн
        self.tariff = dict()     # клиент.тариф МГ/МН/ВЗ: tariff['nid_cid'] = стоим_1_мин
        self.tariff2 = dict()    # клиент.тариф МГ/МН/ВЗ: tariff2['nid_tid'] = стоим_1_мин
        self.tarspsmg = dict()   # клиент.тариф СПС МГ по зонам: tarspsmg['zona_cid'] = ст_1_мин (tarspsmg['6_273']=7.5)
        self.tarspsmg2 = dict()  # клиент.тариф СПС МГ по зонам: tarspsmg2['zona_tid'] = ст_1_мин (tarspsmg2['6_4']=7.5)
        self.taraspsmg = dict()  # агент.тариф  СПС МГ по зонам: taraspsmg['zona']=dict(tara=1.1, name='Россия (моб)..')
        self.custdefault = custdefault  # код клиента по умолчанию для выбора тарифа
        self.tiddefault = tiddefault  # код тарифа по умолчанию, (1..5)

        self._read_()            # чтение: .names, .tariff, .codes, .tarspsmg, .taraspsmg
        # self._read_old()            # чтение: .names, .tariff, .codes, .tarspsmg, .taraspsmg

    def size_tariff(self):
        """
        Размер структуры с тарифами
        """
        return len(self.tariff)

    def _read_(self):
        """
        Чтение тарифов МТС в : names, codes, tariff, tariff2, tarspsmg, taraspsmg
        """
        db = MySQLdb.Connect(**self.dsn)
        cur = db.cursor()

        # направления: names['797'] = dict(name='Нижегородская', zona=2, type='MG', tara=0.81, code1='831')
        # список тел. кодов с привязкой к направлению: codes['code_nid_stat'], напр. codes['7831_797_MN']
        sql = "select `nid`, `type`, `name`, `zona`, `code1`, `code2`, `tar` from {table} where `type`<>'VZ'".\
            format(table=self.tabcode)
        cur.execute(sql)
        for line in cur:
            nid, _type, name, zona, code1, code2, tar = line
            self.names[str(nid)] = dict(name=name.encode('utf8'), zona=zona, type=_type, tara=tar, code1=code1)
            arrcode = Func.arraycode(code1, code2)
            for x in arrcode:
                self.codes.append(self.join_codenidstat(x, nid, _type))  # code_nid_stat

        self.codes.sort()

        # (пока) клиентский тариф с привязкой кода клиента к направлению: tariff['nid_cid'] = стоит_1_мин
        # клиентский тариф с привязкой кода тарифного плана к направлению : tariff2['nid_tid'] = стоит_1_мин
        sql = "select `tid`, `nid`, `cid`, `tar` from {table}".format(table=self.tabtarmts)
        cur.execute(sql)
        for line in cur:
            tid, nid, cid, tar = line
            self.tariff[self.join_nidcid(nid, cid)] = tar
            self.tariff2[self.join_nidtid(nid, tid)] = tar

        # агентский тариф СПС МГ с привязкой к зоне: taraspsmg['zona']=dict(tara=1.1, name='Россия (моб) 4 зона')
        sql = "select `name`, `zona`, `tar` tara from `{tabcode}`  WHERE `regkom` like 'Rossia_mob-%'".format(
            tabcode=self.tabcode)
        cur.execute(sql)
        for line in cur:
            name, zona, tara = line
            self.taraspsmg[str(zona)] = dict(tara=tara, name=name)

        # (mtsTar) клиент_тариф СПС МГ+ВЗ с привязкой к зоне: tarspsmg['zona_cid'] (ex. tarspsmg['6_273']=7.5)
        sql = "select t.`cid`, t.`tid`, k.`name`, k.`zona`, k.`tar` tara, t.`tar` from `{tabcode}` k JOIN `{tabtar}`" \
              " t ON k.`nid`=t.`nid` WHERE k.`regkom` like 'Rossia_mob-%' or k.`type` = 'VZ'".\
            format(tabcode=self.tabcode, tabtar=self.tabtarmts)
        cur.execute(sql)
        for line in cur:
            cid, tid, name, zona, tara, tar = line
            if zona == -1:  # zona=-1 СПС в komstarCode == zona=0 СПС в defCode
                zona = 0
            self.tarspsmg[self.join_zonacid(zona, cid)] = tar
            self.tarspsmg2[self.join_zonatid(zona, tid)] = tar

        cur.close()
        db.close()

        # self.prn_names()
        # self.prn_tarcust()
        # self.prn_codes()
        # self.prn_vz()

    def _read_old__(self):
        """
        Чтение тарифов МТС в : names, codes, tariff, tarspsmg, taraspsmg
        """
        db = MySQLdb.Connect(**self.dsn)
        cur = db.cursor()

        # направления: names['797'] = dict(name='Нижегородская', zona=2, type='MG', tara=0.81)
        # список тел. кодов с привязкой к направлению: codes['code;nid'], напр. codes['7831;797']=2.50
        sql = "select `nid`, `type`, `name`, `zona`, `code1`, `code2`, `tar` from {table} where `type`<>'VZ'".\
            format(table=self.tabcode)
        cur.execute(sql)
        for line in cur:
            nid, _type, name, zona, code1, code2, tar = line
            self.names[str(nid)] = dict(name=name.encode('utf8'), zona=zona, type=_type, tara=tar, code1=code1)
            arrcode = Func.arraycode(code1, code2)
            for x in arrcode:
                self.codes.append(self.join_codenidstat(x, nid, _type))

        self.codes.sort()

        # клиентский тариф с привязкой к направлению: tariff['nid_cid'] = стоит_1_мин
        sql = "select `nid`, `cid`, `tar` from {table}".format(table=self.tabtar)
        cur.execute(sql)
        for line in cur:
            nid, cid, tar = line
            self.tariff[self.join_nidcid(nid, cid)] = tar

        # клиентский тариф2 с привязкой к направлению: tariff['nid_cid'] = стоит_1_мин
        sql = "select `nid`, `cid`, `tar` from {table}".format(table=self.tabtar2)
        cur.execute(sql)
        for line in cur:
            nid, cid, tar = line
            self.tariff[self.join_nidcid(nid, cid)] = tar

        # агентский тариф СПС МГ с привязкой к зоне: taraspsmg['zona']=dict(tara=1.1, name='Россия (моб) 4 зона')
        sql = "select `name`, `zona`, `tar` tara from `{tabcode}`  WHERE `regkom` like 'Rossia_mob-%'".format(
            tabcode=self.tabcode)
        cur.execute(sql)
        for line in cur:
            name, zona, tara = line
            self.taraspsmg[str(zona)] = dict(tara=tara, name=name)

        # (komstarTar) клиент_тариф СПС МГ+ВЗ с привязкой к зоне: tarspsmg['zona_cid'] (ex. tarspsmg['273_6']=7.5)
        sql = "select t.`cid`, k.`name`, k.`zona`, k.`tar` tara, t.`tar` from `{tabcode}` k JOIN `{tabtar}` t " \
              "ON k.`nid`=t.`nid` WHERE k.`regkom` like 'Rossia_mob-%' or k.`type` = 'VZ'".\
            format(tabcode=self.tabcode, tabtar=self.tabtar)
        cur.execute(sql)
        for line in cur:
            cid, name, zona, tara, tar = line
            if zona == -1:  # zona=-1 СПС в komstarCode == zona=0 СПС в defCode
                zona = 0
            self.tarspsmg[self.join_zonacid(zona, cid)] = tar

        # (komstarTarRss) клиент_тариф СПС МГ+ВЗ с привязкой к зоне: tarspsmg['zona_cid'] (ex. tarspsmg['273_6']=7.5)
        sql = "select t.`cid`, k.`name`, k.`zona`, k.`tar` tara, t.`tar` from `{tabcode}` k JOIN `{tabtar}` t " \
              "ON k.`nid`=t.`nid` WHERE k.`regkom` like 'Rossia_mob-%' or k.`type` = 'VZ'".\
            format(tabcode=self.tabcode, tabtar=self.tabtar2)
        cur.execute(sql)
        for line in cur:
            cid, name, zona, tara, tar = line
            if zona == -1:  # zona=-1 СПС в komstarCode == zona=0 СПС в defCode
                zona = 0
            self.tarspsmg[self.join_zonacid(zona, cid)] = tar

        cur.close()
        db.close()

        # self.prn_names()
        # self.prn_tarcust()
        # self.prn_codes()
        # self.prn_vz()

    def prn_names(self, fname=None):
        """
        Печать направлений (nid,name,code1,tara)
        :param fname: если есть, то дополнительно печать в файл
        """
        f = None
        if fname:
            f = open(fname, "wt", encoding='utf8')
        n = 0
        for k, v in self.names.items():
            q = self.names[k]
            st = "nid:{nid} {{ code1:{code1}, tara:{tara}, name:'{name:s}', type={type}, zona={zona} }}".format(
                nid=k, code1=q['code1'], name=q['name'].decode('utf8'), tara=q['tara'], type=q['type'], zona=q['zona'])
            print(st)
            if f:
                f.write(st + "\n")
            n += 1

        st = "itogo:{n} records".format(n=n)
        print(st)
        if f:
            f.write(st + "\n")
            f.close()

    def prn_vz(self, fname=None):
        """
        Обёртка для печати кодов ВЗ-связи
        :param fname: если есть, то дополнительно печать в файл
        """
        self.cdef.print_codevz(fname)

    def prn_codes(self, fname=None):
        """
        Печать кодов: codes('code;nid', 'code2;nid2', ...)
        :param fname: если есть, то дополнительно печать в файл
        """
        f = None
        if fname:
            f = open(fname, "wt", encoding='utf8')
        n = 0
        for x in self.codes:
            code, nid, stat = self.split_codenidstat(x)
            st = "{x:15} : code={code}, nid={nid}, stat={stat} name='{name}'({code1}), zona={zona}, tara={tara}".format(
                code=code, nid=nid, stat=stat, name=self.names[nid]['name'].decode('utf8'),
                code1=self.names[nid]['code1'], zona=self.names[nid]['zona'], tara=self.names[nid]['tara'], x=x)
            print(st)
            st = "{x}".format(x=x)
            if f:
                f.write(st + "\n")
            n += 1

        st = "itogo:{n} records".format(n=n)
        print(st)
        if f:
            f.write(st + "\n")
            f.close()

    def prn_tarcust(self, fname=None):
        """
        Печать клиентских тарифов
        :param fname: если есть, то дополнительно печать в файл
        """
        n = 0
        f = None
        if fname:
            f = open(fname, "wt", encoding='utf8')

        tar = self.tariff
        for k in tar.keys():
            nid, cid = self.split_nidcid(k)
            st = "nid={nid}; cid={cid}; tar={tar}; name='{name:s}'({code1})".format(
                nid=nid, cid=cid, tar=tar[k], name=self.names[nid]['name'].decode('utf8'),
                code1=self.names[nid]['code1'])
            print(st)
            if f:
                f.write(st + "\n")
            n += 1

        st = "itogo:{n} records".format(n=n)
        print(st)
        if f:
            f.write(st + "\n")
            f.close()

    @staticmethod
    def join_nidcid(nid, cid):
        """ nid, cid; return = 'nid_cid'
        """
        return str(nid) + "_" + str(cid)

    @staticmethod
    def split_nidcid(val):
        """ val='nid_cid'; return (nid,cid)
        """
        return val.split('_')   # (nid, cid)

    @staticmethod
    def join_nidtid(nid, tid):
        """ nid, tid; return = 'nid_tid'
        """
        return str(nid) + "_" + str(tid)

    @staticmethod
    def split_nidtid(val):
        """ val='nid_tid'; return (nid, tid)
        """
        return val.split('_')   # (nid, tid)

    @staticmethod
    def join_zonacid(zona, cid):
        """ zona, cid; return = 'zona_cid'
        """
        return str(zona) + "_" + str(cid)

    @staticmethod
    def split_zonacid(val):
        """ val='zona_cid'; return (zona,cid)
        """
        return val.split('_')   # (zona, cid)

    @staticmethod
    def join_zonatid(zona, tid):
        """ zona, tid; return = 'zona_tid'
        """
        return str(zona) + "_" + str(tid)

    @staticmethod
    def split_zonatid(val):
        """ val='zona_tid'; return (zona,tid)
        """
        return val.split('_')   # (zona, tid)

    @staticmethod
    def join_codenidstat(code, nid, stat):
        """ code, nid, stat; return = 'code_nid_stat'
        """
        return str(code) + "_" + str(nid) + "_" + str(stat)

    @staticmethod
    def split_codenidstat(val):
        """ val='code_nid_stat'; return (code,nid,stat)
        """
        return val.split('_')   # (code, nid, stat)

    def gettar(self, nid, cid):
        """
        возвращает клиент. стоимость 1 мин для направления по коду nid для клиента cid
        """
        k = self.join_nidcid(nid, cid)
        return self.tariff.get(k, 0)

    def gettar2(self, nid, tid):
        """
        возвращает клиент. стоимость 1 мин для направления nid для тар_плана tid
        """
        k = self.join_nidtid(nid, tid)
        return self.tariff2.get(k, 0)

    def gettara(self, nid):
        """
        возвращает агент. стоимость 1 мин для напрааления по коду nid
        """
        x = self.names.get(nid, None)
        if x:
            return x['tara']
        else:
            return 0

    def gettarmgs(self, zona, cid):
        """
        возвращает клиент. стоимость 1 мин СПС МГ по зоне и cid
        """
        k = self.join_zonacid(zona, cid)
        return self.tarspsmg.get(k, 0)

    def gettarmgs2(self, zona, tid):
        """
        возвращает клиент. стоимость 1 мин СПС МГ по зоне и tid
        """
        k = self.join_zonatid(zona, tid)
        return self.tarspsmg2.get(k, 0)

    def gettaramgs(self, zona):
        """
        возвращает агентскую стоимость 1 мин СПС МГ
        """
        x = self.taraspsmg.get(str(zona), None)
        if x:
            return x['tara']
        else:
            return 0

    def get_name_zona_tar_tara(self, nid, cid):
        """
        По коду направления и коду клиента возвращает: name, zona, tar, tara
        :param cid: код клиента
        :param nid: код направления
        :return : кортеж: name, zona, tar, tara
        """

        a = self.names.get(nid, None)                   # инфо по коду направления nid
        if a:
            name, zona = (a['name'].decode('utf8'), a['zona'])
            tar = self.gettar(nid, cid)
            if tar == 0:
                tar = self.gettar(nid, self.custdefault)
            tara = self.gettara(nid)
            ok = True
        else:
            name, zona, tar, tara, ok = ('-', -2, 0, 0, None)

        return name, zona, tar, tara, ok

    def get_name_zona_tar_tara2(self, nid, tid):
        """
        По коду направления и коду тарифа возвращает: name, zona, tar, tara
        :param nid: код направления
        :param tid: код тарифа
        :return : кортеж: name, zona, tar, tara
        """

        a = self.names.get(nid, None)                   # инфо по коду направления nid
        if a:
            name, zona = (a['name'].decode('utf8'), a['zona'])
            tar = self.gettar2(nid, tid)
            if tar == 0:
                tar = self.gettar2(nid, self.tiddefault)
            tara = self.gettara(nid)
            ok = True
        else:
            name, zona, tar, tara, ok = ('-', -2, 0, 0, None)

        return name, zona, tar, tara, ok

    def old__get_sts_code_zona_tar_tara_name_nid__old(self, cid, org, to, tox=None):
        """
        Информация по звонку на номер number клиентом cid
        :param cid: код клиента, например, 273
        :param org: R|G - договор по номеру с организацией РСС(R) или РСИ(G)
        :param to: вызываемый номер to, например, 988123263992
        :param tox: вызываемый номер tox, например, 988123263992
        :return: (sts, code, zona, tar, tara, name, nid) : ('mg', '8812', 2, 3.31, 0.18, 'name', 123)
        """
        code, zona, tar, tara, name, nid = ('-', -2, 0, 0, '-', 0)
        sts, prx = self.stat.getsts(to=to, tox=tox)    # ('vz','916025') or ('mg','8312261')

        if sts in ('mgs', 'vz'):    # СПС: mgs=S (сотовая по России) vz=Z (ВЗ)
            code, zona, stat, name = self.cdef.get_mysql_code_zona_stat_reg(prx)  # '903423123', 4, 'mg', 'Дагестан'
            tar = self.gettarmgs(zona, cid)
            if tar == 0:
                tar = self.gettarmgs(zona, self.custdefault)
            tara = self.gettaramgs(zona)
            # агентский для ВЗ прописан в cfg потому что с кодами одна таблица komstarCode
            if sts == 'vz':
                tara = (cfg.atar_vz['rsi'], cfg.atar_vz['rss'])[org == 'R']

        elif sts == 'mg':
            for x in self.codes:
                code, nid, stat = self.split_codenidstat(x)     # '7831_797_MG': code=7831 nid=797 stat=MG
                if stat == 'MG' and prx.startswith(code[1:]):
                    name, zona, tar, tara, ok = self.get_name_zona_tar_tara(nid, cid)
                    break

        elif sts in ('kz', 'ab'):
            for x in self.codes:
                code, nid, stat = self.split_codenidstat(x)     # '7831_797_MG': code=7831 nid=797 stat=MG
                if stat == 'MN' and code.startswith('7') and prx.startswith(code[1:]):
                    name, zona, tar, tara, ok = self.get_name_zona_tar_tara(nid, cid)
                    break

        elif sts == 'mn':
            for x in self.codes:
                code, nid, stat = self.split_codenidstat(x)         # '1204_265_MN: code=1204 nid=265 stat=MN
                if stat == 'MN' and prx.startswith(code):
                    name, zona, tar, tara, ok = self.get_name_zona_tar_tara(nid, cid)
                    break

        elif sts == 'gd':
            name, code, zona = ('Москва', '7' + prx, 0)
        elif sts == 'vm':
            name, code = ('МГ ФГУП РСИ', prx)

        return sts, code, zona, tar, tara, name, nid

    def get_sts_code_zona_tar_tara_name_nid(self, tid, org, to, tox=None):
        """
        Информация по звонку на номер to c тарифным планом tid
        :param tid: код тарифа, например, 1, их всего сейчас 5 штук
        :param org: R|G - договор по номеру с организацией РСС(R) или РСИ(G)
        :param to: вызываемый номер to, например, 988123263992
        :param tox: вызываемый номер tox, например, 988123263992
        :return: (sts, code, zona, tar, tara, name, nid) : ('mg', '8812', 2, 3.31, 0.18, 'name', 123)
        """
        code, zona, tar, tara, name, nid = ('-', -2, 0, 0, '-', 0)
        sts, prx = self.stat.getsts(to=to, tox=tox)    # ('vz','916025') or ('mg','8312261')

        if sts in ('mgs', 'vz'):    # СПС: mgs=S (сотовая по России) vz=Z (ВЗ)
            code, zona, stat, name = self.cdef.get_mysql_code_zona_stat_reg(prx)  # '903423123', 4, 'mg', 'Дагестан'
            tar = self.gettarmgs2(zona, tid)
            if tar == 0:
                tar = self.gettarmgs2(zona, self.tiddefault)
            tara = self.gettaramgs(zona)
            # агентский для ВЗ прописан в cfg потому что с кодами одна таблица komstarCode
            if sts == 'vz':
                tara = (cfg.atar_vz['rsi'], cfg.atar_vz['rss'])[org == 'R']

        elif sts == 'mg':
            for x in self.codes:
                code, nid, stat = self.split_codenidstat(x)     # '7831_797_MG': code=7831 nid=797 stat=MG
                if stat == 'MG' and prx.startswith(code[1:]):
                    name, zona, tar, tara, ok = self.get_name_zona_tar_tara2(nid, tid)
                    break

        elif sts in ('kz', 'ab'):
            for x in self.codes:
                code, nid, stat = self.split_codenidstat(x)     # '7831_797_MG': code=7831 nid=797 stat=MG
                if stat == 'MN' and code.startswith('7') and prx.startswith(code[1:]):
                    name, zona, tar, tara, ok = self.get_name_zona_tar_tara2(nid, tid)
                    break

        elif sts == 'mn':
            for x in self.codes:
                code, nid, stat = self.split_codenidstat(x)         # '1204_265_MN: code=1204 nid=265 stat=MN
                if stat == 'MN' and prx.startswith(code):
                    name, zona, tar, tara, ok = self.get_name_zona_tar_tara2(nid, tid)
                    break

        elif sts == 'gd':
            name, code, zona = ('Москва', '7' + prx, 0)
        elif sts == 'vm':
            name, code = ('МГ ФГУП РСИ', prx)

        return sts, code, zona, tar, tara, name, nid


if __name__ == '__main__':
    # коды СПС
    codedef = codedef.Codedef(dsn=cfg.dsn_tar, tabcode='defCode')

    # клиенты
    cust = customers.Cust(dsn=cfg.dsn_cust)

    # тип звонка по номеру
    ct = calltype.Calltype(cdef=codedef)

    # тарифы МТС
    mts = Tarmts(dsn=cfg.dsn_tar, tabcode='komstarCode', tabtar='komstarTar', tabtar2='komstarTarRss',
                 stat=ct, tabtarmts='mtsTar', cdef=codedef, custdefault=1171, tiddefault=1)
    # проверка - печать всех списков
    # mts.prn_names(fname='./test/names.txt')
    # mts.prn_codes(fname='./test/codes.txt')
    # mts.prn_tarcust(fname='./test/tarcust.txt')
    # mts.prn_vz(fname='./test/vz.txt')
    #

    # 1.тариф узнаём по коду клиента
    # print("\n тариф МТС (by customers.id):")
    # for _cid in (953, 549, 957, 1171):
    #     for _org in ('R', 'G'):
    #         print("cid:{cid}/{org}".format(cid=_cid, org=_org))
    #         for _num in ('89283522010', '8108613818709812', '88312261234', '101', '89160218525', '89991234567',
    #                      '81038012345678', '84956261626', '88001234567', '8119999'):
    #             _stat, _code, _zona, _tar, _tara, _name, _nid = \
    #                 mts.old__get_sts_code_zona_tar_tara_name_nid__old(cid=_cid, org=_org, to=_num)
    #             print(" cid:{cid} num:{num:20s} : {stat}/{zona:4s} \t: {tar:5s}/{tara:10s}: " \
    #                   "{code}/'{name}'/({nid})".\
    #                 format(cid=_cid, num=_num, stat=_stat, code=_code, zona=str(_zona), tar=str(_tar),
    #                        tara=str(_tara), name=_name, space='', nid=_nid))
    #
    # print("size_tariff : {size}".format(size=mts.size_tariff()))

    # 2. тариф узнаём по коду тарифа
    print("\n тариф МТС (by tariff.tariff_tel.tid):")
    for _cid in (953, 549, 957, 1171):
        _tid = cust.get_tid_t(_cid)
        for _org in ('R', 'G'):
            print("cid:{cid}/{org}/{tid}".format(cid=_cid, org=_org, tid=_tid))
            for _num in ('89283522010', '8108613818709812', '88312261234', '101', '89160218525', '89991234567',
                         '81038012345678', '84956261626', '88001234567', '8119999'):

                _stat, _code, _zona, _tar, _tara, _name, _nid = \
                    mts.old__get_sts_code_zona_tar_tara_name_nid__old(cid=_cid, org=_org, to=_num)
                print(" cid:{cid} num:{num:20s} : {stat}/{zona:4s} \t: {tar:5s}/{tara:10s}: "
                      "{code}/'{name}'/({nid})".
                      format(cid=_cid, num=_num, stat=_stat, code=_code, zona=str(_zona), tar=str(_tar),
                             tara=str(_tara), name=_name, space='', nid=_nid))

                _stat, _code, _zona, _tar, _tara, _name, _nid = \
                    mts.get_sts_code_zona_tar_tara_name_nid(tid=_tid, org=_org, to=_num)
                print(" cid:{cid} num:{num:20s} : {stat}/{zona:4s} \t: {tar:5s}/{tara:10s}: "
                      "{code}/'{name}'/({nid})".
                      format(cid=_cid, num=_num, stat=_stat, code=_code, zona=str(_zona), tar=str(_tar),
                             tara=str(_tara), name=_name, space='', nid=_nid))

    print("size_tariff : {size}".format(size=mts.size_tariff()))
