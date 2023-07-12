import json

import psycopg2

from config import config
from typing import List


def main():
    script_file = 'fill_db.sql'
    json_file = 'suppliers.json'
    db_name = 'db_for_homework_5'

    params = config()
    conn = None

    create_database(params, db_name)
    print(f"БД {db_name} успешно создана")

    params.update({'dbname': db_name})
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                execute_sql_script(cur, script_file)
                print(f"БД {db_name} успешно заполнена")

                create_suppliers_table(cur)
                print("Таблица suppliers успешно создана")

                suppliers = get_suppliers_data(json_file)
                insert_suppliers_data(cur, suppliers)
                print("Данные в suppliers успешно добавлены")

                add_foreign_keys(cur, json_file)
                print(f"FOREIGN KEY успешно добавлены")
                cur.execute('SELECT * FROM products')
                rec = cur.fetchall()
                print(rec)

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_database(params, db_name) -> None:
    """Создает новую базу данных."""
    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS {db_name}')
        cur.execute(f'CREATE DATABASE {db_name}')
    conn.close()


def execute_sql_script(cur, script_file) -> None:
    """Выполняет скрипт из файла для заполнения БД данными."""
    with open(script_file, encoding='utf-8') as sql_file:
        data = sql_file.read()

    cur.execute(data)


def create_suppliers_table(cur) -> None:
    """Создает таблицу suppliers."""
    cur.execute("""
        CREATE TABLE suppliers(
            supplier_id serial PRIMARY KEY,
            company_name varchar(100) NOT NULL,
            contact varchar(100),
            address varchar(100),
            phone varchar(20),
            fax varchar(20),
            homepage varchar(100),
            product varchar(100)            
        )
    """)


def get_suppliers_data(json_file: str) -> List[dict]:
    """Извлекает данные о поставщиках из JSON-файла и возвращает список словарей с соответствующей информацией."""
    with open(json_file) as f:
        data = json.load(f)
    return data


def insert_suppliers_data(cur, suppliers: List[dict]) -> None:
    """Добавляет данные из suppliers в таблицу suppliers."""
    for supplier in suppliers:
        for product in supplier['products']:
            cur.execute("""
                INSERT INTO suppliers (company_name, contact, address, phone, fax, homepage, product) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (supplier['company_name'],
                      supplier['contact'],
                      supplier['address'],
                      supplier['phone'],
                      supplier['fax'],
                      supplier['homepage'],
                      product))


def add_foreign_keys(cur, json_file) -> None:
    """Добавляет foreign key со ссылкой на supplier_id в таблицу products."""
    cur.execute('ALTER TABLE products ADD COLUMN supplier_id smallint REFERENCES suppliers')
    with open(json_file) as f:
        data = json.load(f)
    counter = 1
    for supplier in data:
        for product in supplier['products']:
            cur.execute("""
                        UPDATE products
                        SET supplier_id = %s WHERE product_name = %s""", (counter, product))
            counter += 1


if __name__ == '__main__':
    main()
