#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
calc.py - разные функции для вычислений НДС и пр.
"""
from modules import cfg


def sign(val):
    """
    Возвращает знак числа:
    sign(x) = 1,   если x > 0,
    sign(x) = -1,  если x < 0,
    sign(x) = 0,   если x = 0.
    :param val: число
    :return: -1 0 or 1
    """
    x = 1
    if val == 0:
        x = 0
    elif val < 0:
        x = -1
    return x


def rnd(val, ndigits=2):
    """
    Округляет по правилам школьной арифметики
    1.112 -> 1.11,  1.145 -> 1.15,  -99.125 -> -99.13
    :param val: число
    :param ndigits: количество знаков после запятой
    :return: округлённое число
    """
    xpw = pow(10, ndigits)
    sgn = sign(val)  # знак числа
    tmp = abs(val) * xpw + 0.5
    x = sgn * int(tmp)/float(xpw)
    return x


def nds(val, ndigits=2):
    """
    Возвращает НДС числа (само число без НДС)
    :param val: число без ндс
    :param ndigits: количество знаков для округления
    :return: НДС
    """
    return rnd(val*cfg.ndskoff, ndigits)


def nds2(val, ndigits=2):
    """
    Возвращает НДС из числа (само число содержит НДС)
    :param val: число включает ндс
    :param ndigits: количество знаков для округления
    :return: НДС
    """
    # val * 18/118;  cfg.ndskoff=0.18
    return rnd(val*cfg.ndskoff/(1+cfg.ndskoff), ndigits)


def sum_without_nds(val, ndigits=2):
    """
    Возвращает сумму без НДС (само число содержит НДС)
    :param val: число включает НДС
    :param ndigits: количество знаков для округления
    :return: число без НДС
    """
    # S = Sн / (1+0.18)
    return rnd(val/(1+cfg.ndskoff), ndigits)


def sum_with_smart_nds(val, how_calculate_nds='*', ndigits=2):
    """
    Возвращает сумму с учётом НДС (одно из: вычитает НДС, прибавляет НДС, ничего не делает)
    :param val: число (без НДС)
    :param how_calculate_nds: как применяем НДС: [*+-] *-ничего не делаем; +добавляем; -вычитаем
    :param ndigits: количество знаков для округления
    :return: число c учётом НДС, в зависимости от how_calculate_nds
    """
    if how_calculate_nds == '*':
        return val
    elif how_calculate_nds == '+':
        return rnd(val*(1+cfg.ndskoff), ndigits)
    elif how_calculate_nds == '-':
        return sum_without_nds(val)
    else:
        return val


if __name__ == '__main__':

    print("\n# НДС на число:")
    for v in (100, 551, 2000):
        print("val:{val:<7} nds:{nds}".format(val=v, nds=nds(v)))

    print("\n# НДС из числа:")
    for v in (100, 551, 2000):
        print("val:{val:<7} nds:{nds}".format(val=v, nds=nds2(v)))

    print("\n# Сумма без НДС (само число включает НДС):")
    for v in (100, 551, 2000):
        print("val:{val:<7} nds:{nds}".format(val=v, nds=sum_without_nds(v)))

    print("\n# Сумма без НДС (вычисляем с условиями : [*-+]):")
    for v in (100, 551, 2000):
        print("val:{val:<7} (how_calc_nds:*)new_val:{value}".format(val=v, value=sum_with_smart_nds(v, '*')))
        print("val:{val:<7} (how_calc_nds:-)new_val:{value}".format(val=v, value=sum_with_smart_nds(v, '-')))
        print("val:{val:<7} (how_calc_nds:+)new_val:{value}".format(val=v, value=sum_with_smart_nds(v, '+')))



    v = 0.29 * 1.5
    vr = rnd(v, 2)
    print("\n{val} -> {valr}".format(val=v, valr=vr))

    vr = round(v, 2)
    print("\n{val} -> {valr}".format(val=v, valr=vr))
