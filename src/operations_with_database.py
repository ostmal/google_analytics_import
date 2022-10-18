import psycopg2
import logging
import sys
import pandas as pd
from sqlalchemy import create_engine


def create_table_raw(credential_bd, dimensions_metrics, event=""):
    """
    Создаем ОСНОВНУЮ таблицу в PostgreSQL
    Кроме того, эта процедура создает отдельную таблицу для Dimentions, связанные с контекстом "event..."
    list_dimentions_event
    list_metrics_event
    """
    # Соединение к PostgreSQL
    try:
        con = psycopg2.connect(
            database=credential_bd['database'],
            user=credential_bd['user'],
            password=credential_bd['password'],
            host=credential_bd['host']
        )

        cursor = con.cursor()
        logging.info(f"Успешное соединение, БД: {credential_bd['host']} - {credential_bd['database']}")
    except:
        logging.error(f"Невозможно подключиться к БД: {credential_bd['host']} - {credential_bd['database']}")
        sys.exit(f"Error: Невозможно подключиться к БД: {credential_bd['host']} - {credential_bd['database']}")

    try:
        table_name = credential_bd['table_name']
    except:
        logging.error(
            "Некорректное значение параметра 'table_name' в конфигурационном файле. Выход из программы")
        sys.exit("Error: Некорректное значение параметра 'table_name' в конфигурационном файле. Выход из программы")

    sql = f'''
        CREATE TABLE IF NOT EXISTS {credential_bd['schema']}.{table_name}{event} (
            id SERIAL PRIMARY KEY,
            view_id bigint,
            log_datetime timestamp,
    '''

    # *** Считываем все  dimentions - ключевые
    try:
        list_dimentions_keys = dimensions_metrics['list_dimentions_keys']
    except:
        logging.error(
            "Некорректное значение параметра 'list_dimentions_keys' в конфигурационном файле. Выход из программы")
        sys.exit(
            "Error: Некорректное значение параметра 'list_dimentions_keys' в конфигурационном файле. Выход из программы")

    # Добавляем в sql-запрос
    for el in list_dimentions_keys:
        sql += f"{el.split(':')[1]} VARCHAR,"

    # Добавляем поле "date"
    sql += "date DATE,"

    # *** Считываем остальные  dimentions
    try:
        list_dimentions = dimensions_metrics[f'list_dimentions{event}']
    except:
        logging.error(
            f"Некорректное значение параметра list_dimentions{event} в конфигурационном файле. Выход из программы")
        sys.exit(
            f"Error: Некорректное значение параметра list_dimentions{event} в конфигурационном файле. Выход из программы")

    # Добавляем в sql-запрос
    for el in list_dimentions:
        sql += f"{el.split(':')[1]} VARCHAR,"

    # *** Считываем metrics
    try:
        list_metrics = dimensions_metrics[f'list_metrics{event}']
    except:
        logging.error(
            f"Некорректное значение параметра list_metrics{event} в конфигурационном файле. Выход из программы")
        sys.exit(
            f"Error: Некорректное значение параметра list_metrics{event} в конфигурационном файле. Выход из программы")

    # Для проверки: считываем метрики с типом numeric
    try:
        list_metrics_numeric = dimensions_metrics['list_metrics_numeric']
    except:
        # Если список с метрик с типом numeric не считан - будет пустым
        list_metrics_numeric = []

    # Добавляем в sql-запрос
    for el in list_metrics:
        sql += el.split(':')[1]
        if el in list_metrics_numeric:
            sql += " numeric,"
        else:
            sql += " INT,"

    # *** Добавляем пару special_dimentions_metrics (dimentions - metrics) - ТОЛЬКО для ОСНОВНОЙ таблицы
    if event == "":
        try:
            list_of_lists_special_dimentions_metrics = dimensions_metrics['list_of_lists_special_dimentions_metrics']

            # Перебираем все пары со СПЕЦИАЛЬНЫМИ DIM-MET
            for special_dim_met in list_of_lists_special_dimentions_metrics:
                # special dimension
                sql += f"{special_dim_met[0].split(':')[1]} VARCHAR,"
                # metric (которая в паре с dimension)
                metric = special_dim_met[1]
                # Если этой метрики нет в общем личсте метрик....
                if metric not in list_metrics:
                    sql += metric.split(':')[1]
                    if metric in list_metrics_numeric:
                        sql += " numeric,"
                    else:
                        sql += " INT,"

        except:
            logging.info("Специальных dimension - нет")

    # sql-end
    sql = sql[:-1] + ");"

    try:
        cursor.execute(sql)
        con.commit()
        logging.info(f"Если таблицы {credential_bd['table_name']}{event} - не было, то она была создана")
    except:
        print(sql)
        logging.error(f"Таблица {credential_bd['table_name']}{event} - ошибка создания таблицы")
        sys.exit(f"Error: Таблица {credential_bd['table_name']}{event} - ошибка создания таблицы. Выход из программы")

    # Закрываем соединение
    con.close()


def create_table_raw_customdimentions(credential_bd, dimensions_metrics, custom_dim_met):
    """
    Создаем таблицу в PostgreSQL с custom dimentions.
    Состав таблицы:
    - view_id
    - datehourminute
    - clientid
    - sessioncount
    - n "custom dimentions"
    - 1 "metrics"
    """
    # Соединение к PostgreSQL
    try:
        con = psycopg2.connect(
            database=credential_bd['database'],
            user=credential_bd['user'],
            password=credential_bd['password'],
            host=credential_bd['host']
        )

        cursor = con.cursor()
        logging.info(f"Успешное соединение, БД: {credential_bd['host']} - {credential_bd['database']}")
    except:
        logging.error(f"Невозможно подключиться к БД: {credential_bd['host']} - {credential_bd['database']}")
        sys.exit(
            f"Error: Невозможно подключиться к БД: {credential_bd['host']} - {credential_bd['database']}. Выход из программы")

    try:
        table_name = credential_bd['table_name']
    except:
        logging.error(
            "Некорректное значение параметра 'table_name' в конфигурационном файле. Выход из программы")
        sys.exit("Error: Некорректное значение параметра 'table_name' в конфигурационном файле. Выход из программы")

    cust_dim_table_name = table_name + "_" + custom_dim_met[0].split(":")[1] + "_" + custom_dim_met[1].split(":")[1]
    sql = f'''
        CREATE TABLE IF NOT EXISTS {credential_bd['schema']}.{cust_dim_table_name} (
            id SERIAL PRIMARY KEY,
            view_id bigint,
            log_datetime timestamp,
    '''

    # *** Считываем все  dimentions - ключевые
    list_dimentions_keys = dimensions_metrics['list_dimentions_keys']
    # Добавляем в sql-запрос
    for el in list_dimentions_keys:
        sql += f"{el.split(':')[1]} VARCHAR,"

    # Добавляем поле "date"
    sql += "date DATE,"

    # Для проверки: считываем метрики с типом numeric
    list_metrics_numeric = dimensions_metrics['list_metrics_numeric']
    list_metrics_numeric = list(map(str.strip, list_metrics_numeric))  # убираем пробелы

    # *** Добавляем пару custom (dimentions - metrics)
    try:
        # custom dimension
        for custom_dim in custom_dim_met[:-1]:
            sql += f"{custom_dim.split(':')[1]} VARCHAR,"
        # metric
        el = custom_dim_met[-1]
        if el in list_metrics_numeric:
            sql += f"{el.split(':')[1]} numeric,"
        else:
            sql += f"{el.split(':')[1]} INT,"
    except:
        logging.info("Кастомных dimensions_metrics - в конфигурационном файле нет")

    # *** sql-end
    sql = sql[:-1] + ");"

    try:
        cursor.execute(sql)
        con.commit()
        logging.info(f"Если таблицы {cust_dim_table_name} - не было, то она была создана")
    except:
        logging.error(f"Таблица {ccust_dim_table_name} - ошибка создания таблицы")
        sys.exit(f"Error: Таблица {ccust_dim_table_name} - ошибка создания таблицы. Выход из программы")

    # Закрываем соединение
    con.close()


def get_latest_date_from_table_GA(credential_bd, view_id, view_id_value, field_date='date'):
    """
    Вытащить последнюю дату из текущей таблицы
    view_id
    """
    # Соединение к PostgreSQL
    con = psycopg2.connect(
        database=credential_bd['database'],
        user=credential_bd['user'],
        password=credential_bd['password'],
        host=credential_bd['host']
    )

    cursor = con.cursor()

    try:
        table_name = credential_bd['table_name']
    except:
        logging.error(
            "Некорректное значение параметра 'table_name' в конфигурационном файле. Выход из программы")
        sys.exit("Error: Некорректное значение параметра 'table_name' в конфигурационном файле. Выход из программы")

    sql = f"SELECT max({field_date})  FROM {credential_bd['schema']}.{table_name} where view_id = {view_id};"
    cursor.execute(sql)
    # Получаем строки с данными
    rows = cursor.fetchall()
    # Закрываем соединение
    con.close()

    try:
        return rows[0][0].strftime('%Y-%m-%d')
    except:
        flag = False
        while flag == False:
            tmp = input(
                f"'{view_id_value['account_name']}' - В таблице  {credential_bd['table_name']} еще нет скачанных данных. Введите начальную дату с которой необходимо качать данные из Google Anslitycs в формате YYYY-MM-DD: ")
            flag = validate_date(tmp)
        latest_date = tmp
        latest_date = pd.to_datetime(latest_date) - pd.DateOffset(1)
        return latest_date.strftime('%Y-%m-%d')


def add_to_table(credential_bd, df, table_name):
    """
    Добавление данные из ДатаФрейма в таблицу БД
    """
    # https://stackoverflow.com/questions/23103962/how-to-write-dataframe-to-postgres-table
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html
    # from sqlalchemy import create_engine
    # engine = create_engine('postgresql://username:password@host:port/mydatabase')
    logging.info(f"Добавляем данные в таблицу: {table_name}")
    engine_string = f"postgresql://{credential_bd['user']}:{credential_bd['password']}@{credential_bd['host']}:5432/{credential_bd['database']}"
    engine = create_engine(engine_string)
    # try:
    df.to_sql(table_name, engine, schema=credential_bd['schema'], if_exists='append', index=False)
    #     logging.info("Датафрейм успешно записан в БД")
    # except:
    #     logging.error("Ошибка записи датафрейма в БД")
    #     sys.exit("Error: Ошибка записи датафрейма в БД. Выход из программы")


def get_column_names(credential_bd, table_name):
    """
    Только для того, чтобы вытащить названия столбцов.
    Это нужно, чтобы при записи в CSV-файл порядок столбцов был такой же
    """
    # Соединение к PostgreSQL
    try:
        con = psycopg2.connect(
            database=credential_bd['database'],
            user=credential_bd['user'],
            password=credential_bd['password'],
            host=credential_bd['host']
        )

        cursor = con.cursor()
        logging.info(f"Успешное соединение, БД: {credential_bd['host']} - {credential_bd['database']}")
    except:
        logging.error(f"Невозможно подключиться к БД: {credential_bd['host']} - {credential_bd['database']}")
        sys.exit(f"Error: Невозможно подключиться к БД: {credential_bd['host']} - {credential_bd['database']}")

    sql = f"SELECT * FROM {credential_bd['schema']}.{table_name}  LIMIT 1;"

    try:
        cursor.execute(sql)
        # Получаем названия столбцов
        column_names = [desc[0] for desc in cursor.description]
        con.commit()
    except:
        print(sql)
        logging.error(f"Таблица {credential_bd['table_name']} - ошибка")

    # Закрываем соединение
    con.close()

    return column_names[1:]
