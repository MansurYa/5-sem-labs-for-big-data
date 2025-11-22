#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
import sys


def main():
    """
    Читает пары (компания, 1) из stdin, суммирует количество коммитов.
    Выводит результат в формате: компания \t количество_коммитов
    """
    current_company = None
    current_count = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        company, count = line.split('\t')
        count = int(count)

        if current_company == company:
            current_count += count
        else:
            if current_company:
                print('{}\t{}'.format(current_company, current_count))
            current_company = company
            current_count = count

    # Вывод последней компании
    if current_company:
        print('{}\t{}'.format(current_company, current_count))


if __name__ == '__main__':
    main()
