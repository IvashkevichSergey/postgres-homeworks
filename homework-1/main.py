"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv

# Создаём соединение с базой данных
with psycopg2.connect(
        host='localhost',
        user='postgres',
        password='123',
        database='north'
) as conn:
    # Создаём курсор
    with conn.cursor() as cur:

        # Поочерёдно открываем все csv файлы на чтение и наполняем
        # таблицы БД данными из файлов
        with open('north_data/customers_data.csv', encoding='utf-8') as f:
            data = csv.DictReader(f)
            for line in data:
                cur.execute('INSERT INTO customers VALUES (%s, %s, %s)',
                            (line['customer_id'], line['company_name'], line['contact_name']))

        with open('north_data/employees_data.csv', encoding='utf-8') as f:
            data = csv.DictReader(f)
            for line in data:
                cur.execute('INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)',
                            (line['employee_id'], line['first_name'],
                             line['last_name'], line['title'],
                             line['birth_date'], line['notes']))

        with open('north_data/orders_data.csv', encoding='utf-8') as f:
            data = csv.DictReader(f)
            for line in data:
                cur.execute('INSERT INTO order_data VALUES (%s, %s, %s, %s, %s)',
                            (line['order_id'], line['customer_id'],
                             line['employee_id'], line['order_date'],
                             line['ship_city']))

# Закрываем соединение с БД
conn.close()
