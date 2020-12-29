#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
calltype - модуль определения типа звонка :
состоит из:
Calltype - определяет по номеру тип звонка: mg(M) mn(W) vz(Z) gd(G) mgs(M) vm(V)
"""
import re
from modules import cfg
from modules import codedef


class Calltype(object):
    """
    Определение типа звонка (mg,mn,vz,gd,mgs,vm)
    # mg=M mn=W vz=S gd=G mgs=Z vm=V
    # M=МГ, W=МН, S=СПС Россия, Z=ВЗ (СПС Москва), G=Город, V=Ведомственная МГ ФГУП РСИ
    """
    sts2stat = dict(mg='M', mn='W', mgs='S', vz='Z', gd='G', vm='V', kz='W', ab='W')    # sts -> stat
    stat2sts = dict(M='mg', W='mn', S='mgs', Z='vz', G='gd', V='vm')            # stat -> sts

    re_msk = re.compile('^(49[589])')                      # G (gd) (moskva)
    re_gd = re.compile('^8(49[589])\d{7,}')                # G (gd)
    re_xx = re.compile('^8(80[04])\d{7,}')                 # X (xx) (бесплатный вызов 8800)
    re_mg = re.compile('^8([2-8]\d{7,})')                  # M (mn)
    re_vm = re.compile('^(811\d{4})')                      # V (vm)
    re_mn = re.compile('^810(\d{10,})')                    # W (mn)
    re_kz = re.compile('^8(7[01267]\d{8,})')               # W (mn), Казахстан
    re_ab = re.compile('^8([89]40\d{7,})')                 # W (mn), Абхазия 7840 & 7940
    re_tm = re.compile('^(10[01234])')                     # G (gd) (запрос времени и прочее)

    def __init__(self, cdef):
        """
        :param cdef: объект Codedef (defx9)
        """
        self.cdef = cdef    # коды ВЗ

        # for line in open(self.fname):
        #    self.__addcodevz__(line)  # 905500-905599,905700-905799

    def __prepare__(self, to, tox=None):
        """
        Приведение номера к стандартному формату МГ/МН/ВЗ - связи
        :return: возвращается номер в стандартном формате : 8xxxZZZZZZZ или 810xxxZZZZZZZ..
        """
        if self.re_msk.match(to):       # 495* 498* 499*
            return '8' + to
        if to.startswith('98258'):
            return to[4:]
        if to.startswith('9818'):       # 98189263072135 -> 89263072135
            return to[3:]
        if tox:
            if "810" + to == tox:   # to=37167005514 tox=81037167005514
                return tox
            elif "10" + to == tox:  # to=420776774884 tox=10420776774884
                return "8" + tox

        if to.startswith('9'):          # 983121234567 => 83121234567
            return to[1:]
        return to

    def getsts(self, to, tox=None):
        """
        Определяет тип звонка : (mg, mn, vz, vm, gd, xx)
        :param to: вызываемый номер на входе в SMG
        :param tox: вызываемый номер на выходе в SMG (иногда отличается от to)
        return: sts -  один из (mg, mn, vz, vm, gd, xx)
        return: (sts, prefix)
        """

        to = self.__prepare__(to, tox)
        prx, stat = ('', '-')

        if to.startswith('89'):
            if self.cdef.is_codevz(to[1:]):    # to[1:7]
                stat, prx = ('vz', to[1:])     # to[1:7]
            else:
                m = self.re_ab.match(to)
                if m:
                    stat, prx = ('ab', m.group(1))  # 7940 - Абхазия
                else:
                    stat, prx = ('mgs', to[1:])    # mgs = mg sps, to[1:7]
        if stat == '-':
            m = self.re_gd.match(to)
            if m:
                stat, prx = ('gd', m.group(1))
        if stat == '-':
            m = self.re_mn.match(to)
            if m:
                stat, prx = ('mn', m.group(1))
        if stat == '-':
            m = self.re_kz.match(to)
            if m:
                stat, prx = ('kz', m.group(1))
        if stat == '-':
            m = self.re_ab.match(to)    # 7840 - Абхазия
            if m:
                stat, prx = ('ab', m.group(1))

        # if stat == '-':
        #    m = self.re_xx.match(to)
        #    if m: stat, prx = ('xx', m.group(1))

        if stat == '-':
            m = self.re_mg.match(to)
            if m:
                stat, prx = ('mg', m.group(1))
        if stat == '-':
            m = self.re_vm.match(to)
            if m:
                stat, prx = ('vm', m.group(1))

        if stat == '-':
            m = self.re_tm.match(to)
            if m:
                stat, prx = ('gd', m.group(1))

        return stat, prx

    def get_mwszg(self, sts):
        """
        Преобразует sts( mg, mn, mgs, vz, gd, vm ) в (M,W,S,Z,G,V)
        """
        return self.sts2stat.get(sts, '-')

    def get_mgmn(self, stat):
        """
        Преобразует stat(M,W,S,Z,G,V) в sts( mg, mn, mgs, vz, gd )
        """
        return self.stat2sts.get(stat, '-')

if __name__ == '__main__':

    # Codedef - коды СПС - связи
    codedef = codedef.Codedef(dsn=cfg.dsn_tar, tabcode='defCode', tabsample='table_defcode.sql', update=False)

    #
    ct = Calltype(cdef=codedef)

    print("\n# тип звонка по номеру:")
    # определение типа звонка: (МГ МН ВЗ)
    for num in ('84965697920', '101', '989160218525', '89991234567', '88312261234', '81038012345678', '84956261626', '88001234567',
                '8119999', '81077292701720', '81077051400999', '89265407411'):
        _sts, _prx = ct.getsts(to=num, tox=num)        # vz, mn, mg, mgs, gd, vm
        _stat = ct.get_mwszg(_sts)     # M, W, S, Z, G, V
        print("{num:16s} sts:{sts}({stat}) prx:{prx}".format(num=num, sts=_sts, stat=_stat, prx=_prx))

