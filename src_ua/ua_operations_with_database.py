import psycopg2
import logging
import sys
import pandas as pd
from sqlalchemy import create_engine


def get_data_from_table_clientid_transactions(ua_config, log_datetime):
    """
    Считываем данные из таблицы clientid-transactions в БД
    log_datetime - эти данные необходимо загрузить
    log_datetime ==
    last - брать последние данные
    all - все последние данные
    конкретное значение, скопированное из таблицы в БД
    """
    # Соединение к PostgreSQL
    try:
        con = psycopg2.connect(
            database=ua_config['database'],
            user=ua_config['user'],
            password=ua_config['password'],
            host=ua_config['host']
        )

        cursor = con.cursor()
        logging.info(f"Успешное соединение, БД: {ua_config['host']} - {ua_config['database']}")
    except:
        logging.error(f"Невозможно подключиться к БД: {ua_config['host']} - {ua_config['database']}")
        sys.exit(f"Error: Невозможно подключиться к БД: {ua_config['host']} - {ua_config['database']}")

    schema = ua_config['schema']
    table_name = ua_config['table_prefix'] + "_" + ua_config['table_name_clientid_transactions']
    logging.info(f"Считываем данные из таблицы {table_name}")

    if log_datetime == 'last':
        log_datetime = '(SELECT max(log_datetime) from osk.ga3_ua_clientid_transactions)'
    elif log_datetime == 'all':
        log_datetime = "'2022-06-21 15:56:57.000' OR 1=1"
    else:
        log_datetime = f"'{log_datetime}'"

    sql = f"""
        SELECT * from {schema}.{table_name}
        where log_datetime = {log_datetime};
    """
    logging.info(f"SQL-запрос: {sql}")

    try:
        cursor.execute(sql)
        logging.info(f"Данные успешно считаны из таблицы {table_name}")
    except:
        logging.info(sql)
        logging.error(f"Считывание данных из таблицы {table_name} - ошибка SQL-запроса")
        sys.exit(f"Считывание данных из таблицы {table_name} - ошибка SQL-запроса")

    # Получаем названия столбцов
    column_names = [desc[0] for desc in cursor.description]

    # Получаем строки с данными
    rows = cursor.fetchall()

    con.commit()

    # Закрываем соединение
    con.close()

    df = pd.DataFrame(rows, columns=column_names)
    return df


def create_table_clientid_transactions(ua_config):
    """
    Создаем  таблиц в PostgreSQL для считывания clientid-transactions из GA
    """
    # Соединение к PostgreSQL
    try:
        con = psycopg2.connect(
            database=ua_config['database'],
            user=ua_config['user'],
            password=ua_config['password'],
            host=ua_config['host']
        )

        cursor = con.cursor()
        logging.info(f"Успешное соединение, БД: {ua_config['host']} - {ua_config['database']}")
    except:
        logging.error(f"Невозможно подключиться к БД: {ua_config['host']} - {ua_config['database']}")
        sys.exit(f"Error: Невозможно подключиться к БД: {ua_config['host']} - {ua_config['database']}")

    schema = ua_config['schema']
    table_name = ua_config['table_prefix'] + "_" + ua_config['table_name_clientid_transactions']

    sql = f'''
        CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
            id SERIAL PRIMARY KEY,
            log_datetime timestamp,
            clientid VARCHAR,
            transactions INT);
    '''

    try:
        cursor.execute(sql)
        con.commit()
        logging.info(f"Если таблицы {table_name} - не было, то она была создана")
    except:
        print(sql)
        logging.error(f"Таблица {table_name} - ошибка создания таблицы")
        sys.exit(f"Error: Таблица {table_name} - ошибка создания таблицы. Выход из программы")

    # Закрываем соединение
    con.close()


def create_five_tables(ua_config):
    """
    Создаем  5 таблиц в PostgreSQL
    """
    # Соединение к PostgreSQL
    try:
        con = psycopg2.connect(
            database=ua_config['database'],
            user=ua_config['user'],
            password=ua_config['password'],
            host=ua_config['host']
        )

        cursor = con.cursor()
        logging.info(f"Успешное соединение, БД: {ua_config['host']} - {ua_config['database']}")
    except:
        logging.error(f"Невозможно подключиться к БД: {ua_config['host']} - {ua_config['database']}")
        sys.exit(f"Error: Невозможно подключиться к БД: {ua_config['host']} - {ua_config['database']}")

    schema = ua_config['schema']
    prefix = table_name = ua_config['table_prefix']

    sql = f'''
        CREATE TABLE IF NOT EXISTS {schema}.{prefix}_visits (
            id SERIAL PRIMARY KEY
            ,sessionId text
            ,deviceCategory text
            ,platform text
            ,dataSource text
            ,sessionDate text
            ,clientId text
            ,source_system text
            ,processed_dttm timestamp);

        CREATE TABLE IF NOT EXISTS {schema}.{prefix}_hits (
            id SERIAL PRIMARY KEY
            ,activityTime text
            ,source text
            ,medium text
            ,channelGrouping text
            ,campaign text
            ,keyword text
            ,hostname text
            ,landingPagePath text
            ,activityType text
            ,hitnumber text
            ,event_eventCategory text
            ,event_eventAction text
            ,event_eventCount text
            ,ecommerce_actionType text
            ,ecommerce_ecommerceType text
            ,event_eventLabel text
            ,ecommerce_transaction_transactionId text
            ,ecommerce_transaction_transactionRevenue text
            ,ecommerce_transaction_transactionShipping text
            ,pageview_pagePath text
            ,pageview_pageTitle text
            ,ecommerce_transaction_transactionTax text
            ,event_eventValue text
            ,clientId text
            ,sessionId text
            ,source_system text 
            ,processed_dttm timestamp);            
            
            
        CREATE TABLE IF NOT EXISTS {schema}.{prefix}_customdimensions (
            id SERIAL PRIMARY KEY
            ,index text
            ,value text
            ,clientId text
            ,sessionId text
            ,hitnumber text
            ,source_system text
            ,processed_dttm timestamp);
            
        CREATE TABLE IF NOT EXISTS {schema}.{prefix}_products (
            id SERIAL PRIMARY KEY
            ,productSku text
            ,productName text
            ,itemRevenue text
            ,productQuantity text
            ,clientId text
            ,sessionId text
            ,hitnumber text
            ,source_system text
            ,processed_dttm timestamp);           
            
        CREATE TABLE IF NOT EXISTS {schema}.{prefix}_goals (
            id SERIAL PRIMARY KEY
            ,goalIndex text
            ,goalCompletions text
            ,goalCompletionLocation text
            ,goalPreviousStep1 text
            ,goalPreviousStep2 text
            ,goalPreviousStep3 text
            ,goalName text
            ,clientId text
            ,sessionId text
            ,hitnumber text
            ,source_system text 
            ,processed_dttm timestamp);
    '''

    try:
        cursor.execute(sql)
        con.commit()
        logging.info(f"Если 5 таблиц - не было, то они были созданы")
    except:
        print(sql)
        logging.error(f"5 таблиц - ошибка создания")
        sys.exit(f"Error: 5 таблиц - ошибка создания. Выход из программы")

    # Закрываем соединение
    con.close()
