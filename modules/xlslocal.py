# -*- coding: utf-8 -*-

import os
import MySQLdb
import xlsxwriter
from modules import cfg

owd = os.getcwd()
os.chdir('..')
root = os.getcwd()
os.chdir(owd)
path_results = "{root}/res_local".format(root=root)   # файлы с результатом по местной связи (utf-8)

a2 = dict(name="OOO 'A2-Телеком'", suffix_file='_loc')
month_names = ['январь', 'февраль', 'март', 'апрель', 'май', 'июнь', 'июль', 'август', 'сентябрь', 'октябрь',
               'ноябрь', 'декабрь']


class _ObjectFormat(object):
    pass


class CustItem(object):
    """
    Клиент
    """
    def __init__(self, cid, customer, uf):
        self.cid = cid  # код клиента
        self.customer = customer  # название клиента
        self.uf = uf    # тип клиента


class LoсBookItem(object):
    """
    Локальная связь - элемент данных из итоговой таблицы bill.loc_book
    """
    title = 'Местная связь А2. Год {year}, месяц {month}'

    def __init__(self, account, cid, customer, uf, period, summa):
        self.cid = cid              # код клиента
        self.customer = customer    # название клиента
        self.uf = uf                # u-юр_лицо f-физ_лицо
        self.period = period        # период
        self.account = account      # номер счёта (условный)
        self.summa = float(summa)   # сумма без НДС


class LocNumbersItem(object):
    """
    Локальная связь - элемент данных из (bill.loc_numbers)
    """
    title = 'Местная связь А2. Итоги по номерам. Год {year}, месяц {month}'

    def __init__(self, account, cid, customer, period, number, min, abon_min, prev_min, prev_cost, summa):
        self.account = account      # номер счёта (условный)
        self.cid = cid              # код клиента
        self.customer = customer    # название клиента
        self.period = period        # период
        self.number = number        # номер
        self.min = min              # минут
        self.abon_min = abon_min    # минут в абон_плате
        self.prev_min = prev_min    # минут превышения
        self.prev_cost = float(prev_cost)   # стоимость 1 мин превышения
        self.summa = float(summa)   # сумма без НДС


class BillLocalXls(object):
    """
    Результат по местной связи в виде xls-файла
    """
    def __init__(self, dsn, year, month, path):
        self.dsn = dsn
        self.year = year
        self.month = month
        self.period = '{year:04d}_{month:02d}'.format(year=int(year), month=int(month))  # 2021_04
        self.table = 'Y{year:04d}M{month:02d}'.format(year=int(year), month=int(month))  # Y2021M04
        self.outfile = "{path}/{period}_{suffix}.{ext}".format(path=path, period=self.period, suffix=a2['suffix_file'],
                                                               ext='xlsx')  # 2021_04_loc.xls

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

    @staticmethod
    def create_cust2type(cust2type_list):
        customer2type = dict()

        for it in cust2type_list:
            (cust, type) = it.split('-')
            customer2type[cust] = type

        return customer2type

    def create_file(self):
        # создание книги
        workbook = xlsxwriter.Workbook(self.outfile)
        frm = BillLocalXls.create_formats(workbook)

        # итоги по клиентам
        book_list, customers_u = self._get_book_data()

        # клиенты
        customers = [str(it.cid) for it in book_list]  # ['58','1251',..]

        # тип клиента
        customers_type = [str(it.cid) + "-" + it.uf for it in book_list]  # ['58-u','1271-u',..]
        customer2type = BillLocalXls.create_cust2type(customers_type)

        # счета в итогах
        accounts = [str(it.account) for it in book_list]   # ['2','3',..]
        if len(accounts) == 0:
            print('not local calls for billing')
            return False

        # итоги по номерам
        numbers = self._get_numbers(accounts)

        # номера по клиентам: {'58': ['6428464'], '1271': ['6269515', '123456', ..]}
        customers_numbers = dict()
        for cid in customers:
            if cid not in customers_numbers:
                customers_numbers[cid] = list()

            for it in numbers:
                if str(it.cid) == str(cid):
                    customers_numbers[cid].append(it.number)

        # создание xls-листа с итогами
        self._create_main_sheet(workbook, frm, book_list, numbers, customer2type)

        # листы с детализацией
        self._create_detailed_sheets(workbook, frm, customers_u)

        #workbook.close()

    def _get_book_data(self):
        """
        Делает выборку из БД - итоги по клиентам/суммам местной связи за период
        :return: список элементов BookItem, список элементов CustItem
        """
        db = MySQLdb.Connect(**self.dsn)
        cursor = db.cursor()
        period = '{year:04d}_{month:02d}'.format(year=int(self.year), month=int(self.month))  # 2021_04
        sql = cfg.sqls['local_customer_u'].format(period=period, uf='u')
        cursor.execute(sql)

        result = list()
        custom = list()
        for line in cursor:
            account, cid, customer, uf, period, summa = line
            result.append(LoсBookItem(account, cid, customer, uf, period, summa))
            custom.append(CustItem(cid, customer, uf))

        cursor.close()
        db.close()
        return result, custom

    def _get_numbers(self, accounts):
        """
        Делает выборку из БД - итоги местной связи по номерам за период
        :param accounts: список account для выборки
        :return: список элементов BookItem
        """
        db = MySQLdb.Connect(**self.dsn)
        cursor = db.cursor()
        sql = cfg.sqls['local_numbers'].format(accounts=','.join(accounts))
        cursor.execute(sql)

        result = list()
        for line in cursor:
            account, cid, customer, period, number, min, abon_min, prev_min, prev_cost, summa = line
            result.append(
                LocNumbersItem(account, cid, customer, period, number, min, abon_min, prev_min, prev_cost, summa))

        cursor.close()
        db.close()
        return result

    def _create_main_sheet(self, workbook, frm, book_list, numbers, customer2type):
        """
        Создаёт лист в файле xlsx для книги продаж
        :param workbook: ссылка на книгу эксель
        :param frm: ссылка на объект с форматами
        :param book_list: список элементов LoсBookItem (юр_лица)
        :param numbers: список элементов LocNumbersItem
        :param customer2type: мапа клиент -> тип
        :return:
        """

        period = self.period

        ws = workbook.add_worksheet(period)

        # заголовки страницы
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
        ws.set_column(9, 8, 12)

        book_title = LoсBookItem.title.format(year=self.year, month=self.month)
        row = self._create_book(title=book_title, book_list=book_list, worksheet=ws, row_start=4, frm=frm)

        account_title = LocNumbersItem.title.format(year=self.year, month=self.month)
        row = self._create_accounts(title=account_title, numbers=numbers, worksheet=ws, row_start=row+1,
                                   frm=frm, customer2type=customer2type)
        #
        # bookf_fitle = "Физические лица (НДС включён в стоимость)"
        # self._create_bookf(title=bookf_fitle, book_list=bookf_list, worksheet=ws, row_start=row+1, frm=frm)

        #workbook.close()

    def _create_detailed_sheets(self, workbook, frm, customers_u):
        """
        Создание отдельных листов : каждый лист - детализация по клиенту
        :param workbook: ссылка на книгу эксель
        :param frm: ссылка на объект с форматами
        :param customers_u: список клиентов CustItem
        :return:
        """
        for x in customers_u:
            numbers = self._get_numbers_customer(x.cid)
            q = []
            # fmx LIKE '%6428464' OR fmx LIKE '%6428465'
            for n in numbers:
                # q.append("fmx LIKE '%{n}'".format(n=n))
                q.append("(fmx LIKE '%{n}' OR fm LIKE '%{n}')".format(n=n))

            self._create_detailed_cust(workbook, frm, x.cid, x.customer, 'OR'.join(q))
        workbook.close()

    def _get_numbers_customer(self, cid):
        """
        Возвращает список номеров клиента за период
        :param cid: код клиента
        :return: (6269515, 6269123, ...)
        """
        db = MySQLdb.Connect(**self.dsn)
        cursor = db.cursor()
        sql = cfg.sqls['local_customer_numbers'].format(period=self.period, cid=cid)
        cursor.execute(sql)
        numbers = cursor.fetchall()[0]
        cursor.close()
        db.close()
        return numbers

    def _create_detailed_cust(self, workbook, frm, cid, customer, numbers):
        """
        Создание детализации по одному клиенту с кодом cid
        :param workbook: книга эксель
        :param frm: объект с форматами
        :param cid:  код клиента
        :param customer: название клиента
        :param numbers: список номеров в виде: fmx LIKE '%6428464' OR fmx LIKE '%6428464'
        :return:
        """

        print(numbers)
        db = MySQLdb.Connect(**self.dsn)
        cursor = db.cursor()
        # sql = cfg.sqls['local_num_detail_fmx'].format(table=self.table, numbers=numbers)
        sql = cfg.sqls['local_num_detail_fm'].format(table=self.table, numbers=numbers)

        cursor.execute(sql)
        print(sql)

        title = "Детализация связей местной телефонии за период: {period}".format(period=self.period)
        company = "{customer}({cid})".format(customer=customer, cid=cid)

        # заголовки страницы
        ws = workbook.add_worksheet(str(cid))
        # Turn off some of the warnings:
        ws.ignore_errors({'number_stored_as_text': 'A1:XFD1048576'})

        ws.fit_to_pages(1, 100)  # Fit to 1x100 pages.
        ws.write('A1', title, frm.bold)
        ws.write('A2', company, frm.bold)

        # date, numberA, numberB, min
        # ширина колонок
        ws.set_column(0, 0, 18)  # date
        ws.set_column(1, 1, 18)  # number_a
        ws.set_column(2, 2, 18)  # number_b
        ws.set_column(3, 3, 8)   # min

        row = 3
        col = 0

        # строка-заголовок: date, number_a, number_b, min
        ws.write(row, col, 'date', frm.header_center)
        ws.write(row, col + 1, 'number_a', frm.header_center)
        ws.write(row, col + 2, 'number_b', frm.header_center)
        ws.write(row, col + 3, 'min', frm.header_right)

        row += 1
        sum_min = 0

        # все связи по номерам
        for line in cursor:
            date, number_a, number_b, min = line
            ws.write(row, col, date, frm.date_full_center)
            ws.write(row, col + 1, number_a, frm.default)
            ws.write(row, col + 2, number_b, frm.default)
            ws.write(row, col + 3, min, frm.default)
            sum_min += min
            row += 1

        ws.write(row, col + 3, sum_min, frm.bold)

        cursor.close()
        db.close()

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
        ws.write(row, col+3, 'period', frm.header)
        ws.write(row, col+4, 'summa(₽)', frm.header_right)

        row += 1

        summa = 0
        for x in book_list:
            ws.write(row, col, x.cid, frm.default_center)
            ws.write(row, col + 1, x.customer, frm.default)
            ws.write(row, col + 2, x.uf, frm.default_center)
            ws.write(row, col + 3, x.period, frm.default)
            ws.write(row, col + 4, x.summa, frm.default)
            summa += x.summa
            row += 1

        ws.write(row, col + 4, summa, frm.bold)

        return row

    def _create_accounts(self, title, numbers, worksheet, row_start, frm, customer2type):
        """
        Создание таблицы Книга продаж
        :param title: заголовок
        :param numbers: список данных по номерам из элементов LocNumbersItem
        :param worksheet: рабочий лист
        :param row_start: начальная строка на странице
        :param frm: ссылка на объект с форматами
        :param customer2type: мапа клиент -> тип
        :return: последнюю занятую строку в листе
        """
        row = row_start
        col = 0
        ws = worksheet

        # Turn off some of the warnings:
        ws.ignore_errors({'number_stored_as_text': 'A1:XFD1048576'})

        ws.write(row, col, title, frm.bold)

        row += 1

        # итоги по списку accounts
        # строка-заголовок
        # account, cid, customer, period, number, min, abon_min, prev_min, prev_cost, summa
        ws.write(row, col, 'cid', frm.header_center)
        ws.write(row, col+1, 'customer', frm.header)
        ws.write(row, col+2, 'uf', frm.header_center)
        ws.write(row, col+3, 'period', frm.header_center)
        ws.write(row, col+4, 'number', frm.header_right)
        ws.write(row, col+5, 'min', frm.header_right)
        ws.write(row, col+6, 'abon_min', frm.header_right)
        ws.write(row, col+7, 'prev_min', frm.header_right)
        ws.write(row, col+8, 'prev_cost', frm.header_right)
        ws.write(row, col+9, 'summa(₽)', frm.header_right)

        row += 1

        summa = 0
        for x in numbers:
            ws.write(row, col, x.cid, frm.default_center)
            ws.write(row, col + 1, x.customer, frm.default)
            ws.write(row, col + 2, customer2type.get(str(x.cid)), frm.default_center)
            ws.write(row, col + 3, x.period, frm.default)
            ws.write(row, col + 4, x.number, frm.default_right)
            ws.write(row, col + 5, x.min, frm.default)
            ws.write(row, col + 6, x.abon_min, frm.default)
            ws.write(row, col + 7, x.prev_min, frm.default)
            ws.write(row, col + 8, x.prev_cost, frm.default)
            ws.write(row, col + 9, x.summa, frm.default)
            summa += x.summa
            row += 1

        ws.write(row, col + 9, summa, frm.bold)

        return row


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


if __name__ == '__main__':
    xls = BillLocalXls(dsn=cfg.dsn_bill2, year=2021, month=10, path=path_results)
    xls.create_file()
