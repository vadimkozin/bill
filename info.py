#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Разная информация по данным:
1) Статистика связей (проверка наличия данных по дням)
info.py 2022 5

"""
import optparse
import traceback
import pymysql
import logging
from cfg import cfg, ini

flog = cfg.paths['logging']['info']     # лог-файл


def xlog(msg, out_console=True):
    """
    Пишет в лог и на консоль если out_console=True
    :param msg: сообщение для логирования
    :param out_console: True|False флажок
    :return:
    """
    log.info(msg)
    if out_console:
        print(msg)


class Stat(object):
    def __init__(self, opts, dsn_smg):
        """
        Биллинг местной связи
        :param opts: параметры
        :param dsn_smg: параметры подключения к базе
        """
        self.opts = opts
        self.dsn_smg = dsn_smg

    def run(self):
        days_total = 0
        calls_total = 0
        min_total = 0

        db = pymysql.Connect(**self.dsn_smg)
        cursor = db.cursor()
        sql = cfg.sqls['stat_days'].format(table=self.opts.table)
        cursor.execute(sql)

        print('smg2.{table}'.format(table=self.opts.table))
        delimiter = '-------------------'
        print(delimiter)
        print('day  calls  sum_min')
        print(delimiter)

        for line in cursor:
            day, calls, sum_min = line
            print('{:>3} {:>6} {:>7}'.format(day, calls, sum_min))
            days_total += 1
            calls_total += calls
            min_total += sum_min

        print(delimiter)
        cursor.close()
        db.close()
        xlog('{table}: days/calls/minutes = {days}/{calls}/{min}'.format(table=self.opts.table, days=days_total,
                                                                         calls=calls_total, min=min_total))


if __name__ == '__main__':
    p = optparse.OptionParser(description="Info ",
                              prog="info.py", version="0.1a", usage="stat.py --year=year --month=month [--log=file]")

    p.add_option('--year', '-y', action='store', dest='year', help='year, example 2022')
    p.add_option('--month', '-m', action='store', dest='month', help='month in range 1-12')
    p.add_option('--log', '-l', action='store', dest='log', default=flog, help='logfile')
    p.add_option("--reset", "-r",
                 action="store_true", dest="reset", default=False,
                 help="option only for compatibility with bill.py")

    opt, args = p.parse_args()

    # параметры в командной строке - в приоритете
    if not (opt.year and opt.month):
        opt.year = ini.year
        opt.month = ini.month

    if not opt.year or not opt.month or not opt.log:
        print(p.print_help())
        exit(1)

    opt.table = 'Y{year:04d}M{month:02d}'.format(year=int(opt.year), month=int(opt.month))  # Y2022M05
    opt.period = '{year:04d}_{month:02d}'.format(year=int(opt.year), month=int(opt.month))  # 2022_05

    logging.basicConfig(
        filename=opt.log, level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S", format='%(asctime)s %(message)s', )
    log = logging.getLogger('app')

    try:
        stat = Stat(opts=opt, dsn_smg=cfg.dsn_smg2)
        stat.run()

        xlog('.')

    except pymysql.Error as e:
        log.exception(str(e))
        print(e)
    except RuntimeError as e:
        log.exception(str(e))
        print(e)
    except Exception as e:
        log.exception(str(e))
        traceback.print_exc(file=open(opt.log, "at"))
        traceback.print_exc()
