#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
mobil.py - модуль по работе с мобильными кодами (пока нужно только для Морспасслужбы(1308))
  как использовать:
  import mobile

  # экземпляр
  mob = Morspas(dsn=cfg.dsn_tar, tab_code='defCode', tab_tar='tar1308', rossia_mob_nid=847)

  # получить инфо по номеру
  num='9893619999'
  info = mob.get_info(num) # получить инфо по сотовому номеру
  info.nid      - код направления (13801)
  info.name     - название направления (Санкт-Петербург (сот, МТС))
  info.regkom   - регион (St-Peterburg_mob_mts)
  info.tar      - тариф (2.02)
  info.type     - тип связи (MG)
  --
  если не найдено, то, считаем что это 'Россия мобильная',(nid:847)
  то есть вернётся информация в info по коду направления 847
  а если же номер не мобильный, то вернёт False

"""
import re
import time
import logging
import pymysql
import traceback
import os.path as path
from cfg import cfg

root = path.abspath(path.join(__file__, "../.."))    # корень
flog = "{root}/log/{file}".format(root=root, file='mobile.log')        # лог-файл


class Morspas(object):
    def __init__(self, dsn, tab_code, tab_tar, rossia_mob_nid=847):
        """
        :param dsn: параметры подключения к БД
        :param tab_code: таблица с def-кодами (tarif.defCode)
        :param tab_tar: таблица с тарифами (tarif.tar1308)
        :param rossia_mob_nid: код (nid) России мобильной
        """
        self.dsn = dsn  # dsn подключения к базе
        self.db = pymysql.Connect(**dsn)  # db - активная ссылка на базу

        self.cur = self.db.cursor()  # cur - курсор
        self.tab_code = tab_code    # таблица с кодами (tarif.defCode)
        self.tab_tar = tab_tar    # таблица с тарифами (tarif.tar1308)

        self.regions = ('Rossia_mob_mts', 'St-Peterburg_mob', 'St-Peterburg_mob_mts', 'Rossia_mob', 'Moskow_mob')
        self.rossia_mob = rossia_mob_nid   # Rossia_mob, Россия моб. (кроме Москвы и Московской области)
        self.info_rossia_mob = None

        self.codes = list()  # [{fm:9110000000, to:9110399999, nid:13801}, {}, {}, ..]
        self.info = dict()
        self.__read_codes()

    def __del__(self):
        self.cur.close()
        self.db.close()

    def __read_codes(self):
        regions = ''
        for region in self.regions:
            regions += "'" + region + "',"
        regions = regions[:-1]    # delete last ,

        sql = "SELECT `nid`, `name`, `regkom`, `code1`, `code2`, `tar`, `type` FROM {table} WHERE " \
              "`type` IN ('MG', 'VZ') AND `regkom` IN ({regions})".\
            format(table=self.tab_tar, regions=regions)

        self.cur.execute(sql)
        for line in self.cur:
            nid, name, regkom, _code1, _code2, tar, _type = line
            info = dict()
            info['nid'] = nid
            info['name'] = name
            info['regkom'] = regkom
            info['tar'] = tar
            info['type'] = _type
            self.info[nid] = info
            if nid == self.rossia_mob:
                self.info_rossia_mob = info

            codes = _code2.split(',')    # codes = ['9110000000-9110399999', '9110800000-9112999999', ..]
            for code in codes:      # code = '9110000000-9110399999'
                if '-' in code:
                    code1, code2 = code.split('-')
                    item = dict()
                    item['fm'] = code1
                    item['to'] = code2
                    item['nid'] = nid
                    self.codes.append(item)

    def getinfo(self, number):
        """
        Получение инфо по номеру
        :param number: номер (9893619999 or 79600298005 or 9991234567)
        :return: объект с информацией по номеру или False если номер не мобильный
        :examples:
        79600298005: {'nid': 847, 'name': 'Россия моб. (кроме Москвы и Московской области)', 'regkom': 'Rossia_mob', 'tar': 3.28, 'type': 'MG'}
        79160218525: {'nid': 849, 'name': 'Россия моб. Москва и Московская область', 'regkom': 'Moskow_mob', 'tar': 1.55, 'type': 'VZ'}
        9893619999: {'nid': 13801, 'name': 'Санкт-Петербург (сот, МТС)', 'regkom': 'St-Peterburg_mob_mts', 'tar': 2.02, 'type': 'MG'}
        9014780000: {'nid': 13803, 'name': 'РФ мобильная (МТС)', 'regkom': 'Rossia_mob_mts', 'tar': 2.22, 'type': 'MG'}
        9014780000: {'nid': 13803, 'name': 'РФ мобильная (МТС)', 'regkom': 'Rossia_mob_mts', 'tar': 2.22, 'type': 'MG'}
        74951234567: False
        12345: False
        """
        re_mob = re.compile('^(7|8)?(9\d{9})$')
        num = number

        if not re_mob.match(number):
            return False

        if len(number) == 11:
            num = number[1:]

        for item in self.codes:
            if item['fm'] <= num <= item['to']:
                return self.info.get(item['nid'])

        # тогда это Россия мобильная
        return self.info_rossia_mob


def xprint(msg):
    print(msg)


if __name__ == '__main__':

    logging.basicConfig(filename=flog, level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S",
                        format='%(asctime)s %(message)s', )
    log = logging.getLogger('app')
    try:

        # РАЗЛИЧНЫЕ ТЕСТЫ ---------------------
        mob = Morspas(dsn=cfg.dsn_tar, tab_code='defCode', tab_tar='tar1308', rossia_mob_nid=847)

        xprint("# определение информации по номеру:")
        numbers = ('89010019999', '89010136501', '89160218525', '79161303979', '9265370878',
                   '9296451321', '9175812988', '9258362114', '79057958148', '79200331414', '79600298005',
                   '89160218525', '79160218525', '9160218525', '9014780000', '9322620000', '9893619999',
                   '74951234567', '12345', '79991234567')

        t1 = time.time()
        for number in numbers:
            _info = mob.getinfo(number)
            xprint("{number}: {info}".format(number=number, info=_info))

        xprint('time:{time:.3f}s'.format(time=time.time()-t1))

        xprint("---")

    except pymysql.Error as e:
        log.warning(str(e))
        xprint(e)
    except Exception as e:
        log.warning(str(e))
        traceback.print_exc()
