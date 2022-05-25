#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
rules - дополнительные бизнес-правила. Правила в таблице telefon.rule_bill
правило меняет плательщика и в итоге сумму за связь
использование:
    rul = Rules(dsn=cfg.dsn_tel, table='rule_bill')
    q = link.Link()
    ...
    q заполнен информацией
    cidx, orgx, idx = rul.get_cidorg(q)

"""
import re
import pymysql
from cfg import cfg
from modules import link


class ItemRule(object):
    """
    одно бизнес-правило
    """
    def __init__(self, id, cid, op, fm, stat, cidn, orgn, nds):
        """
        правила храним в откомпиллированном виде регулярного выражения
        """
        # в правиле на опцию может стоять звёздочка(*). Это означает - что угодно
        self.id = id
        self.re_cid = re.compile(r'{cid}'.format(cid='.*' if cid == '*' else cid))
        self.re_op = re.compile(r'{op}'.format(op='.*' if op == '*' else op))
        self.re_fm = re.compile(r'{fm}'.format(fm='.*' if fm == '*' else fm))
        self.re_stat = re.compile(r'{stat}'.format(stat='.*' if stat == '*' else stat))
        self.cidn = cidn
        self.orgn = orgn
        self.nds = nds  # one from: [*, +, -]


class Rules(object):
    def __init__(self, dsn, table):
        self.dsn = dsn
        self.table = table
        self.db = pymysql.connect(**self.dsn)
        self.cur = self.db.cursor()
        self.rules = []
        self._read_rules()

    def _read_rules(self):
        """
        Чтение бизнес-правил в структуру
        """
        sql = "SELECT id,cid,op,fm,stat,cidn,orgn,nds FROM {table} WHERE period='Y9999M99' and ok='+'".format(table=self.table)
        self.cur.execute(sql)
        for line in self.cur:
            id, cid, op, fm, stat, cidn, orgn, nds = line
            self.rules.append(ItemRule(id, cid, op, fm, stat, cidn, orgn, nds))

    def get_cidorg(self, lnk):
        """
        Применяет бизнес-правила из таблицы rule_bill
        Возвращает новые (cid,org) если одно из правил сработало или старые (cid, org)
        """
        cidn, orgn, idn, nds = (lnk.cid, lnk.org, 0, '*')
        for rule in self.rules:
            if rule.re_cid.match(str(lnk.cid)) and rule.re_fm.match(lnk.fm) and rule.re_stat.match(lnk.stat)\
                    and rule.re_op.match(lnk.op):
                cidn, orgn, idn, nds = (rule.cidn, rule.orgn, rule.id, rule.nds)
                break
        return cidn, orgn, idn, nds


if __name__ == '__main__':
    rul = Rules(dsn=cfg.dsn_tel, table='rule_bill')
    q = link.Link()
    for q.cid, q.org, q.op, q.fm, q.stat in ((84, 'X', 'm', '8117999', 'W'),
                                             (84, 'X', 'q', '6269526', 'Q'),
                                             (1148, 'X', 'q', '6428485', 'Z'),
                                             (1148, 'X', 'q', '6428200', 'S'),
                                             (1156, 'X', 'q', '84956275272', 'M'),
                                             (1174, 'X', 'm', 'xxx', '?'),
                                             (549, 'X', 'q', '6261113', 'M')
                                             ):
        cidx, orgx, idx, ndsx = rul.get_cidorg(q)
        print("{cid}/{fm}/{stat} => {cidx}/{orgx}({idx})/(nds:{ndsx})"\
            .format(cid=q.cid, fm=q.fm, stat=q.stat, cidx=cidx, orgx=orgx, idx=idx, ndsx=ndsx))
