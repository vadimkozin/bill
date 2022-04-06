# -*- coding: utf-8 -*-

import os
import MySQLdb
import xlsxwriter
from modules import cfg
from modules.progressbar import Progressbar     # прогресс-бар

owd = os.getcwd()
os.chdir('..')
root = os.getcwd()
os.chdir(owd)
path_results = "{root}/res_mest".format(root=root)   # файлы с результатом по местной связи (utf-8)

a2 = dict(name="OOO 'A2-Телеком'", suffix_file='mest')


class _ObjectFormat(object):
    pass


class CustItem(object):
    """
    Клиент
    """
    def __init__(self, cid, cust_name, cust_type, inn):
        self.cid = cid
        self.cust_name = cust_name
        self.uf = cust_type
        self.inn = inn


class BookItem(object):
    """
     Местная связь - элемент данных из итоговой таблицы bill.mest_book
    """
    title = 'Местная связь А2. Год {year}, месяц {month}'

    def __init__(self, account, period, cid, uf, min, cost1min, summa):
        self.account = account  # условный номер счёта
        self.period = period    # период
        self.cid = cid          # код клиента
        self.uf = uf            # тип клиента
        self.min = int(min)     # количество местных минут
        self.cost1min = float(cost1min)    # стоимость 1-й местной минуты
        self.summa = float(summa)   # сумма к оплате ( summa = min * cost1min)


class BillMestXls(object):
    """
    Результат по местной связи в виде xls-файла
    """
    def __init__(self, dsn, year, month, path):
        self.dsn = dsn
        self.year = year
        self.month = month
        self.period = '{year:04d}_{month:02d}'.format(year=int(year), month=int(month))  # 2022_04
        self.table = 'Y{year:04d}M{month:02d}'.format(year=int(year), month=int(month))  # Y2022M04
        self.outfile = "{path}/{period}_{suffix}.{ext}".format(path=path, period=self.period, suffix=a2['suffix_file'],
                                                               ext='xlsx')  # 2022_04_mest.xls
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

    def _get_book_data(self):
        """
        Делает выборку данных из книги местной связи
        :return: список элементов DataItem
        """
        db = MySQLdb.Connect(**self.dsn)
        cursor = db.cursor()
        sql = cfg.sqls['mest_data'].format(period=self.period)
        # print(sql)
        cursor.execute(sql)

        items = list()
        for line in cursor:
            account, period, cid, uf, min, cost1min, summa = line
            items.append(BookItem(account, period, cid, uf, min, cost1min, summa))

        cursor.close()
        db.close()
        return items

    def _get_customers(self, cid_list):
        """
        Возвращает инфо по списку клиентов
        :return: список элементов CustItem
        """
        db = MySQLdb.Connect(**self.dsn)
        cursor = db.cursor()
        sql = cfg.sqls['mest_customers'].format(custIds=','.join(cid_list))
        cursor.execute(sql)

        items = list()
        for line in cursor:
            cid, cust_name, cust_type, inn = line
            items.append(CustItem(cid, cust_name, cust_type, inn))

        cursor.close()
        db.close()
        return items

    def _create_main_sheet(self, workbook, frm, book_list, cid2name):
        """
        Создаёт лист в файле xlsx для книги продаж
        :param workbook: ссылка на книгу эксель
        :param frm: ссылка на объект с форматами
        :param book_list: список элементов BookItem (юр_лица)
        :param cid2name: мапа cid->name
        :return:
        """

        period = self.period

        ws = workbook.add_worksheet(period)

        # заголовки страницы
        ws.fit_to_pages(1, 100)  # Fit to 1x100 pages.
        ws.write('A1', period, frm.bold)
        ws.write('A2', 'Местная связь (495, 499)', frm.default)

        # ширина колонок
        ws.set_column(0, 0, 10)
        ws.set_column(1, 1, 40)
        ws.set_column(2, 2, 10)
        ws.set_column(3, 3, 18)
        ws.set_column(4, 8, 12)

        book_title = BookItem.title.format(year=self.year, month=self.month)
        row = self._create_book(title=book_title, book_list=book_list, worksheet=ws, row_start=4, frm=frm,
                                cid2name=cid2name)

        ws.write('A{n}'.format(n=row+3), 'Позвонковая детализация по клиентам - в отдельных вкладках', frm.default)

        # workbook.close()

    def _create_book(self, title, book_list, worksheet, row_start, frm, cid2name):
        """
        Создание таблицы Книга продаж
        :param title: заголовок
        :param book_list: список данных для книги продаж из элементов BookItem
        :param worksheet: рабочий лист
        :param row_start: начальная строка на странице
        :param frm: ссылка на объект с форматами
        :param cid2name: мапа cid -> custName
        :return: последнюю занятую строку в листе
        """
        row = row_start
        col = 0
        ws = worksheet

        # ws.write(row, col, title, frm.bold)

        # row += 1

        # книга продаж
        # строка-заголовок
        ws.write(row, col, 'код', frm.header_center)
        ws.write(row, col+1, 'клиент', frm.header)
        ws.write(row, col+2, 'минут', frm.header_center)
        ws.write(row, col+3, 'стоим_1_минуты', frm.header)
        ws.write(row, col+4, 'сумма(₽)', frm.header_right)

        row += 1

        summa = 0
        for x in book_list:
            ws.write(row, col, x.cid, frm.default_center)
            ws.write(row, col + 1, cid2name[str(x.cid)], frm.default)
            ws.write(row, col + 2, x.min, frm.default_center)
            ws.write(row, col + 3, x.cost1min, frm.default)
            ws.write(row, col + 4, x.summa, frm.default)
            summa += x.summa
            row += 1

        ws.write(row, col + 4, summa, frm.bold)

        return row

    def _create_detailed_sheets(self, workbook, frm, customers):
        """
        Создание отдельных листов : каждый лист - детализация по клиенту
        :param workbook: ссылка на книгу эксель
        :param frm: ссылка на объект с форматами
        :param customers: список клиентов CustItem
        :return:
        """

        for x in customers:
            self._create_detailed_cust(workbook, frm, x.cid, x.cust_name)

        # workbook.close()

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
        sql = cfg.sqls['mest_detail'].format(table=self.table, cid=cid)

        cursor.execute(sql)

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

    def create_file(self):

        bar = Progressbar(info='mest.Prepare data', maximum=3)

        # создание книги
        workbook = xlsxwriter.Workbook(self.outfile)
        frm = BillMestXls.create_formats(workbook)

        # итоги по клиентам
        book_list = self._get_book_data()

        bar.update_progress(1)

        # клиенты
        customers_id = [str(it.cid) for it in book_list]  # ['58','1251',..]

        customers = self._get_customers(customers_id)
        cid2name = dict()
        for it in customers:
            cid2name[str(it.cid)] = it.cust_name

        bar.update_progress(2)

        # счета в итогах
        accounts = [str(it.account) for it in book_list]   # ['2','3',..]
        if len(accounts) == 0:
            print('not mest calls for billing')
            return False

        bar.update_progress(3)
        bar.go_new_line()

        bar = Progressbar(info='mest.Create xls-file', maximum=2)
        # создание xls-листа с итогами
        self._create_main_sheet(workbook, frm, book_list, cid2name)
        bar.update_progress(1)

        #
        # листы с детализацией
        self._create_detailed_sheets(workbook, frm, customers)

        bar.update_progress(2)
        bar.go_new_line()

        workbook.close()


if __name__ == '__main__':
    xls = BillMestXls(dsn=cfg.dsn_bill2, year=2022, month=1, path=path_results)
    xls.create_file()
