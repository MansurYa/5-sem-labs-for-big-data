#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
import sys


def extract_company_from_email(email):
    """
    Извлекает название компании из email адреса.

    :param email: Email адрес автора коммита
    :return: Домен компании или 'unknown'
    """
    if not email or '@' not in email:
        return 'unknown'

    parts = email.split('@')
    if len(parts) != 2:
        return 'unknown'

    domain = parts[1].lower()

    # Фильтруем популярные личные почтовые сервисы
    personal_domains = {'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com',
                        'mail.ru', 'yandex.ru', 'icloud.com'}

    if domain in personal_domains:
        return 'individual'

    return domain


def main():
    """
    Читает строки из stdin, парсит коммиты и выводит пары (компания, 1).
    Входной формат: author_email
    """
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        company = extract_company_from_email(line)
        print('{}\t1'.format(company))


if __name__ == '__main__':
    main()
