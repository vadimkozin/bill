# -*- coding: utf-8 -*-

import os
import MySQLdb
import xlsxwriter
from modules import cfg

owd = os.getcwd()
os.chdir('..')
root = os.getcwd()
os.chdir(owd)
path_results = "{root}/results".format(root=root)   # файлы с результатом по МГ/ВЗ (utf-8) для выст_счетов

a2 = dict(name="OOO 'A2-Телеком'", suffix_file='_rss')
month_names = ['январь', 'февраль', 'март', 'апрель', 'май', 'июнь', 'июль', 'август', 'сентябрь', 'октябрь',
               'ноябрь', 'декабрь']


class _ObjectFormat(object):
    pass


class CustItem(object):
    """
    Клиент
    """
    def __init__(self, cid, customer):
        self.cid = cid  # код клиента
        self.customer = customer  # название клиента


class BookItem(object):
    """
    Книга продаж - общие итоги по юр-лицам и физ-лицам
    """
    title = 'Книга продаж ПАО МТС/А2. Год {year}, месяц {month}'

    def __init__(self, cid, customer, uf, contract, account, date, summa, nds=0, total=0):
        self.cid = cid              # код клиента
        self.customer = customer    # название клиента
        self.uf = uf                # u-юр_лицо f-физ_лицо
        self.contract = contract    # номер договора
        self.account = account      # номер счёта/сф
        self.date = date            # дата выставления счёта
        self.summa = float(summa)   # сумма без НДС
        self.nds = float(nds)       # НДС
        self.total = float(total)   # всего


class ServiceItem(object):
    """
    Книга услуг - детализация итогов по услугам (МГ/ВЗ)
    """
    title = 'Детализация по услугам для юр-лиц. Год {year}, месяц {month}'

    def __init__(self, cid, customer, uf, contract, account, date, serv, summa, nds, total):
        self.cid = cid              # код клиента
        self.customer = customer    # название клиента
        self.uf = uf                # u-юр_лицо f-физ_лицо
        self.contract = contract    # номер договора
        self.account = account      # номер счёта/сф
        self.date = date            # дата выставления счёта
        self.serv = serv            # услуга: (MG | VZ)
        self.summa = float(summa)   # сумма без НДС
        self.nds = float(nds)       # НДС
        self.total = float(total)   # всего


class BillReportXls(object):
    """
    Результат по услугам в виде xls-файла
    """
    def __init__(self, dsn, year, month, path):
        self.dsn = dsn
        self.year = year
        self.month = month
        self.period = '{year:04d}_{month:02d}'.format(year=int(year), month=int(month))  # 2021_01
        self.table = 'Y{year:04d}M{month:02d}'.format(year=int(year), month=int(month))  # Y2021M01
        self.outfile = "{path}/{period}{suffix}.{ext}".format(path=path, period=self.period, suffix=a2['suffix_file'],
                                                              ext='xlsx')  # 2021_01_rss.xls

    @staticmethod
    def create_formats(workbook):
        """
        Создаёт форматы для таблицы ексель
        :param workbook: экземпляр xlsxwriter.Workbook
        :return: ссылка на объект с форматами
        """
        # форматы:
        font_size = 13
        font_size2 = 14
        fmt = workbook.add_format
        frm = _ObjectFormat()
        frm.default = fmt({'font_size': font_size})
        frm.default_blue = fmt({'font_size': font_size, 'color': 'blue'})
        frm.prefix_name = fmt({'font_size': 11})
        frm.default_center = fmt({'font_size': font_size, 'align': 'center'})
        frm.default_right = fmt({'font_size': font_size, 'align': 'right'})
        frm.default_left = fmt({'font_size': font_size, 'align': 'left'})
        frm.bold = fmt({'bold': True, 'font_size': font_size})
        frm.bold2 = fmt({'bold': True, 'font_size': font_size2, 'color': 'blue'})
        frm.bold_center = fmt({'bold': True, 'align': 'center', 'font_size': font_size})
        frm.bold_right = fmt({'bold': True, 'align': 'right', 'font_size': font_size})
        frm.bold_left = fmt({'bold': True, 'align': 'left', 'font_size': font_size})
        frm.header = fmt({'bold': True, 'font_size': font_size, 'bg_color': '#C0C0C0'})
        frm.header_center = fmt({'bold': True, 'align': 'center', 'font_size': font_size, 'bg_color': '#C0C0C0'})
        frm.header_right = fmt({'bold': True, 'align': 'right', 'font_size': font_size, 'bg_color': '#C0C0C0'})
        frm.money = fmt({'num_format': '#,##0"₽"', 'font_size': font_size})
        frm.money_bold = fmt({'bold': True, 'num_format': '#,##"₽"', 'font_size': font_size})
        frm.date = fmt({'num_format': 'dd-mm-yyyy', 'font_size': font_size})
        frm.date_full = fmt({'num_format': 'dd-mm-yyyy hh:mm', 'font_size': font_size})
        frm.date_full_center = fmt({'num_format': 'dd-mm-yyyy hh:mm', 'font_size': font_size, 'align': 'center'})
        frm.date_full_left = fmt({'num_format': 'dd-mm-yyyy hh:mm', 'font_size': font_size, 'align': 'left'})

        return frm

    def create_file(self):
        # создание книги
        workbook = xlsxwriter.Workbook(self.outfile)
        frm = BillReportXls.create_formats(workbook)

        # списки
        book_list = self._get_book_data()
        bookf_list = self._get_bookf_data()
        service_list = self._get_service_data()
        customers_u = self._get_customers_u()

        # лист с итогами
        self._create_main_sheet(workbook, frm, book_list, bookf_list, service_list)

        # лист с актом
        self._create_akt(workbook, frm)

        # листы с детализацией
        self._create_detailed_sheets(workbook, frm, customers_u)

        workbook.close()

    def _create_main_sheet(self, workbook, frm, book_list, bookf_list, service_list):
        """
        Создаёт лист в файле xlsx для книги продаж
        :param workbook: ссылка на книгу эксель
        :param frm: ссылка на объект с форматами
        :param book_list: список элементов BookItem (юр_лица)
        :param bookf_list: список элементов BookItem (физ_лица)
        :param service_list: список элементов ServiceItem
        :return:
        """

        period = self.period

        # заголовки страницы
        ws = workbook.add_worksheet(period)
        ws.fit_to_pages(1, 100)  # Fit to 1x100 pages.
        ws.write('A1', 'Name', frm.bold)
        ws.write('B1', a2['name'], frm.bold)
        ws.write('A2', 'Period', frm.bold)
        ws.write('B2', period, frm.bold)
        ws.write('A3', 'Data', frm.bold)
        ws.write('B3', 'Телефонная станция A2', frm.bold)

        # ширина колонок
        ws.set_column(0, 0, 10)
        ws.set_column(1, 1, 65)
        ws.set_column(2, 2, 4)
        ws.set_column(3, 3, 20)
        ws.set_column(4, 8, 12)

        book_title = BookItem.title.format(year=self.year, month=self.month)
        row = self._create_book(title=book_title, book_list=book_list, worksheet=ws, row_start=4, frm=frm)

        service_title = ServiceItem.title.format(year=self.year, month=self.month)
        row = self._create_service(title=service_title, service_list=service_list, worksheet=ws, row_start=row+1,
                                   frm=frm)

        bookf_fitle = "Физические лица (НДС включён в стоимость)"
        self._create_bookf(title=bookf_fitle, book_list=bookf_list, worksheet=ws, row_start=row+1, frm=frm)

        # workbook.close()

    def _create_detailed_sheets(self, workbook, frm, customers_u):
        """
        Создание отдельных листов : каждый лист - детализация по клиенту
        :param workbook: ссылка на книгу эксель
        :param frm: ссылка на объект с форматами
        :param customers_u: список клиентов CustItem
        :return:
        """
        for x in customers_u:
            # print(x.cid, x.customer)
            self._create_detailed_cust(workbook, frm, x.cid, x.customer)

    def _create_detailed_cust(self, workbook, frm, cid, customer):
        """
        Создание детализации по одному клиенту с кодом cid
        :param workbook: книга эксель
        :param frm: объект с форматами
        :param cid:  код клиента
        :param customer: название клиента
        :return:
        """
        db = MySQLdb.Connect(**self.dsn)
        cursor = db.cursor()
        sql = cfg.sqls['detailed'].format(table=self.table, cid=cid)
        cursor.execute(sql)

        title = "Детализация связей телефонии за период: {period}".format(period=self.period)
        company = "{customer}({cid})".format(customer=customer, cid=cid)

        # заголовки страницы
        ws = workbook.add_worksheet(str(cid))
        # Turn off some of the warnings:
        ws.ignore_errors({'number_stored_as_text': 'A1:XFD1048576'})

        ws.fit_to_pages(1, 100)  # Fit to 1x100 pages.
        ws.write('A1', title, frm.bold)
        ws.write('A2', company, frm.bold)

        # ширина колонок
        ws.set_column(0, 0, 12)     # number_a
        ws.set_column(1, 1, 18)     # date
        ws.set_column(2, 2, 18)     # number_b
        ws.set_column(3, 3, 12)     # code
        ws.set_column(4, 4, 45)     # direction
        ws.set_column(5, 5, 8)      # min
        ws.set_column(6, 6, 10)     # summa

        row = 3
        col = 0

        # строка-заголовок: number_a, date, number_b, code, direction, min, summa
        ws.write(row, col, 'number_a', frm.header_center)
        ws.write(row, col + 1, 'date', frm.header_center)
        ws.write(row, col + 2, 'number_b', frm.header_center)
        ws.write(row, col + 3, 'code', frm.header)
        ws.write(row, col + 4, 'direction', frm.header)
        ws.write(row, col + 5, 'min', frm.header_right)
        ws.write(row, col + 6, 'summa(₽)', frm.header_right)

        row += 1
        summa_number, summa_total, minutes_number, minutes_total, step = (0, 0, 0, 0, 0)
        last_number = ''

        # все связи по номерам
        for line in cursor:
            number_a, date, number_b, code, direction, minutes, summa = line
            # итоги по номеру
            if step > 0 and number_a != last_number:
                ws.write(row, col + 5, minutes_number, frm.bold)
                ws.write(row, col + 6, summa_number, frm.bold)
                minutes_number, summa_number = (0, 0)
                row += 1

            ws.write(row, col, number_a, frm.default)
            ws.write(row, col + 1, date, frm.date_full_center)
            ws.write(row, col + 2, number_b, frm.default)
            ws.write(row, col + 3, code, frm.default)
            ws.write(row, col + 4, direction, frm.default)
            ws.write(row, col + 5, minutes, frm.default)
            ws.write(row, col + 6, summa, frm.default)

            minutes_number += minutes
            minutes_total += minutes
            summa_number += summa
            summa_total += summa
            row += 1
            step += 1
            last_number = number_a

            # итоги по последнену номеру
            if step == cursor.rowcount:
                ws.write(row, col + 5, minutes_number, frm.bold)
                ws.write(row, col + 6, summa_number, frm.bold)
                minutes_number, summa_number = (0, 0)
                row += 1

        ws.write(row, col + 5, minutes_total, frm.bold2)
        ws.write(row, col + 6, summa_total, frm.bold2)
        cursor.close()
        db.close()

    def _get_book_data(self):
        """
        Делает выборку из БД для книги продаж
        :return: список элементов BookItem
        """
        db = MySQLdb.Connect(**self.dsn)
        cursor = db.cursor()
        sql = cfg.sqls['book'].format(year=self.year, month=self.month)
        cursor.execute(sql)

        # period = '{year:04d}_{month:02d}'.format(year=int(self.year), month=int(self.month))  # 2021_01
        # outfile = "{path}/{period}_results.txt".format(path=self.path_outfile, period=period)

        book_list = list()
        for line in cursor:
            cid, customer, uf, contract, account, date, summa, nds, total = line
            book_list.append(BookItem(cid, customer, uf, contract, account, date, summa, nds, total))

        cursor.close()
        db.close()
        return book_list

    def _get_bookf_data(self):
        """
        Делает выборку из БД для книги продаж для физ-лиц
        :return: список элементов BookItem
        """
        db = MySQLdb.Connect(**self.dsn)
        cursor = db.cursor()
        sql = cfg.sqls['book_f'].format(year=self.year, month=self.month)
        cursor.execute(sql)

        book_list = list()
        for line in cursor:
            cid, customer, uf, contract, account, date, summa = line
            book_list.append(BookItem(cid, customer, uf, contract, account, date, summa))

        cursor.close()
        db.close()
        return book_list

    def _get_service_data(self):
        """
        Делает выборку из БД для книги услуг (МГ/ВЗ)
        :return: список элементов ServiceItem
        """
        db = MySQLdb.Connect(**self.dsn)
        cursor = db.cursor()
        sql = cfg.sqls['serv_u'].format(year=self.year, month=self.month)
        cursor.execute(sql)

        items = list()
        for line in cursor:
            cid, customer, uf, contract, account, date, serv, summa, nds, total = line
            items.append(ServiceItem(cid, customer, uf, contract, account, date, serv, summa, nds, total))

        cursor.close()
        db.close()
        return items

    def _get_customers_u(self):
        """
        Делает выборку клиентов из книги продаж для юр-лиц
        :return: список элементов CustItem
        """
        db = MySQLdb.Connect(**self.dsn)
        cursor = db.cursor()
        sql = cfg.sqls['customers_u'].format(year=self.year, month=self.month)
        cursor.execute(sql)

        items = list()
        for line in cursor:
            cid, customer = line
            items.append(CustItem(cid, customer))

        cursor.close()
        db.close()
        return items

    def _create_book(self, title, book_list, worksheet, row_start, frm):
        """
        Создание таблицы Книга продаж
        :param title: заголовок
        :param book_list: список данных для книги продаж из элементов BookItem
        :param worksheet: рабочий лист
        :param row_start: начальная строка на странице
        :param frm: ссылка на объект с форматами
        :return: последнюю занятую строку в листе
        """
        row = row_start
        col = 0
        ws = worksheet

        ws.write(row, col, title, frm.bold)

        row += 1

        # книга продаж
        # строка-заголовок
        ws.write(row, col, 'cid', frm.header_center)
        ws.write(row, col+1, 'customer', frm.header)
        ws.write(row, col+2, 'uf', frm.header_center)
        ws.write(row, col+3, 'contract', frm.header)
        ws.write(row, col+4, 'account', frm.header_right)
        ws.write(row, col+5, 'date', frm.header_center)
        ws.write(row, col+6, 'summa', frm.header_right)
        ws.write(row, col+7, 'nds', frm.header_right)
        ws.write(row, col+8, 'total', frm.header_right)

        row += 1

        summa, nds, total = (0, 0, 0)
        for x in book_list:
            ws.write(row, col, x.cid, frm.default_center)
            ws.write(row, col + 1, x.customer, frm.default)
            ws.write(row, col + 2, x.uf, frm.default_center)
            ws.write(row, col + 3, x.contract, frm.default)
            ws.write(row, col + 4, x.account, frm.default)
            ws.write(row, col + 5, x.date, frm.date)
            ws.write(row, col + 6, x.summa, frm.default)
            ws.write(row, col + 7, x.nds, frm.default)
            ws.write(row, col + 8, x.total, frm.default)
            summa += x.summa
            nds += x.nds
            total += x.total
            row += 1

        ws.write(row, col + 6, summa, frm.bold)
        ws.write(row, col + 7, nds, frm.bold)
        ws.write(row, col + 8, total, frm.bold)

        return row

    def _create_bookf(self, title, book_list, worksheet, row_start, frm):
        """
        Создание таблицы Книга продаж для физических лиц
        :param title: заголовок
        :param book_list: список данных для книги продаж из элементов BookItem
        :param worksheet: рабочий лист
        :param row_start: начальная строка на странице
        :param frm: ссылка на объект с форматами
        :return: последнюю занятую строку в листе
        """
        row = row_start
        col = 0
        ws = worksheet

        ws.write(row, col, title, frm.bold)

        row += 1

        # книга продаж
        # строка-заголовок cid, customer, uf, contract, account, date, sum
        ws.write(row, col, 'cid', frm.header_center)
        ws.write(row, col + 1, 'customer', frm.header)
        ws.write(row, col + 2, 'uf', frm.header_center)
        ws.write(row, col + 3, 'contract', frm.header)
        ws.write(row, col + 4, 'account', frm.header_right)
        ws.write(row, col + 5, 'date', frm.header_center)
        ws.write(row, col + 6, 'summa', frm.header_right)

        row += 1

        summa, nds, total = (0, 0, 0)
        for x in book_list:
            ws.write(row, col, x.cid, frm.default_center)
            ws.write(row, col + 1, x.customer, frm.default)
            ws.write(row, col + 2, x.uf, frm.default_center)
            ws.write(row, col + 3, x.contract, frm.default)
            ws.write(row, col + 4, x.account, frm.default)
            ws.write(row, col + 5, x.date, frm.date)
            ws.write(row, col + 6, x.summa, frm.default)

            summa += x.summa
            row += 1

        ws.write(row, col + 6, summa, frm.bold)

        return row

    def _create_service(self, title, service_list, worksheet, row_start, frm):
        """
        Создание таблицы Книги Услуг на текущем листе worksheet
        :param title: заголовок
        :param service_list: список данных для книги услуг из элементов ServiceItem
        :param worksheet: рабочий лист
        :param row_start: начальная строка на странице
        :param frm: ссылка на объект с форматами
        :return: последнюю занятую строку в листе
        """
        row = row_start
        col = 0
        ws = worksheet

        ws.write(row, col, title, frm.bold)

        row += 1

        # книга услуг
        # строка-заголовок  cid, customer, uf, contract, account, date, serv, sum, nds, total
        ws.write(row, col, 'cid', frm.header_center)
        ws.write(row, col+1, 'customer', frm.header)
        ws.write(row, col+2, 'uf', frm.header_center)
        ws.write(row, col+3, 'contract', frm.header)
        ws.write(row, col+4, 'account', frm.header_right)
        ws.write(row, col+5, 'date', frm.header_center)
        ws.write(row, col+6, 'service', frm.header_center)
        ws.write(row, col+7, 'summa', frm.header_right)
        ws.write(row, col+8, 'nds', frm.header_right)
        ws.write(row, col+9, 'total', frm.header_right)

        row += 1

        summa, nds, total = (0, 0, 0)
        for x in service_list:
            ws.write(row, col, x.cid, frm.default_center)
            ws.write(row, col + 1, x.customer, frm.default)
            ws.write(row, col + 2, x.uf, frm.default_center)
            ws.write(row, col + 3, x.contract, frm.default)
            ws.write(row, col + 4, x.account, frm.default)
            ws.write(row, col + 5, x.date, frm.date)
            ws.write(row, col + 6, x.serv, frm.default)
            ws.write(row, col + 7, x.summa, frm.default)
            ws.write(row, col + 8, x.nds, frm.default)
            ws.write(row, col + 9, x.total, frm.default)
            summa += x.summa
            nds += x.nds
            total += x.total
            row += 1

        ws.write(row, col + 7, summa, frm.bold)
        ws.write(row, col + 8, nds, frm.bold)
        ws.write(row, col + 9, total, frm.bold)

        return row

    def _create_akt(self, workbook, frm):
        """
        Создание Акта
        :param workbook: книга эксель
        :param frm: объект с форматами
        :return:
        """

        db = MySQLdb.Connect(**self.dsn)
        cursor = db.cursor()
        sql = cfg.sqls['akt'].format(year=self.year, month=self.month)
        cursor.execute(sql)

        minutes, iv_d_agent, iv_d_join, summa, us, pi, av, nds = cursor.fetchone()
        cursor.close()
        db.close()

        title = "АКТ сдачи-приёмки, подтвержающего выполнение обязательств Оператором за {month} {year}".\
            format(year=self.year, month=month_names[self.month-1])

        row, col = (0, 0)

        ws = workbook.add_worksheet('akt')
        ws.write('A1', title, frm.bold)
        row += 2

        # ширина колонок
        ws.set_column(0, 0, 50)
        ws.set_column(1, 1, 16)
        ws.set_column(2, 2, 8)
        ws.set_column(3, 3, 50)

        # akt: minutes, iv_d_agent, iv_d_join, summa, us, pi, av, nds
        ws.write(row, col, 'Сумма начисленного дохода без НДС', frm.default)
        ws.write(row, col+1, summa, frm.default)
        ws.write(row, col+2, 'руб.', frm.default_center)
        row += 1

        ws.write(row, col, 'Условная стоимость услуг без НДС', frm.default)
        ws.write(row, col+1, us, frm.default)
        ws.write(row, col+2, 'руб.', frm.default_center)
        row += 1

        ws.write(row, col, 'Обьем минут для расчета условной стоимости', frm.default)
        ws.write(row, col+1, minutes, frm.default)
        ws.write(row, col+2, 'мин.', frm.default_center)
        row += 1

        ws.write(row, col, 'Инициирование без НДС', frm.default)
        ws.write(row, col+1, pi, frm.default)
        ws.write(row, col+2, 'руб.', frm.default_center)
        row += 1

        formula_av = " {av}= {summa} - ({us} + {pi})".format(av=av, summa=summa, us=us, pi=pi)
        ws.write(row, col, 'Вознаграждение Оператора без НДС', frm.default)
        ws.write(row, col+1, av, frm.default)
        ws.write(row, col+2, 'руб.', frm.default_center)
        ws.write(row, col+3, formula_av, frm.default_center)
        row += 1

        ws.write(row, col, 'НДС Вознаграждения Оператора', frm.default)
        ws.write(row, col+1, nds, frm.default)
        ws.write(row, col+2, 'руб.', frm.default_center)
        row += 1
        row += 1

        ws.write(row, col, 'Инициирование вызовов по агентскому договору', frm.default)
        ws.write(row, col+1, iv_d_agent, frm.default)
        ws.write(row, col+2, 'мин.', frm.default_center)
        row += 1

        ws.write(row, col, 'Инициирование вызовов по договору присоединения', frm.default)
        ws.write(row, col+1, iv_d_join, frm.default_blue)
        ws.write(row, col+2, 'мин.', frm.default_center)
        row += 1


if __name__ == '__main__':
    xls = BillReportXls(dsn=cfg.dsn_bill2, year=2021, month=1, path=path_results)
    xls.create_file()
