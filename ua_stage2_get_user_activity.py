from src_ua.ua_operations_with_database import *
from src_ua.stage1_data_from_ga import *
from src_ua.stage2_user_activity import *

import pandas as pd
import yaml
import logging
import time
import socket


def main():
    """
    ВАЖНО!
    В программе используется API ver4 GA-3
    """

    # Устанавливаем предельное время запроса. Если этого не сделать не будет срабатывать "except HttpError as error", т.е. не будет олавливать ошибки запроса
    TIMEOUT = 600  # 10 мин
    socket.setdefaulttimeout(TIMEOUT)

    # Настройка логирования
    file_log = logging.FileHandler('log/ua_stage2_get_user_activity.log')
    console_out = logging.StreamHandler()
    logging.basicConfig(handlers=(file_log, console_out),
                        format='[%(asctime)s | %(module)s | %(funcName)s | %(levelname)s]: %(message)s',
                        datefmt='%m.%d.%Y %H:%M:%S',
                        level=logging.INFO)

    for _ in range(3): logging.info("******************************************************************")

    #
    # *** Считывание конфигурационных фајлов ***************************************
    with open('etc\ga_ua_config.yml') as file:
        ua_config = yaml.load(file, Loader=yaml.FullLoader)

    # Для логирования
    log_datetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # Для формирования заголовка CSV-файла
    data_in_csv = datetime.datetime.now().strftime("%Y-%m-%d__%H-%M-%S")

    #
    # *** Считываем данные из таблицы clientid_transactions *********************************************
    if ua_config['stage2_read_from'] == 1:  # В настройках - считываем из БД
        # *** Запрашиваем дату-время отгруженных данных для считывания из БД
        print("В конфигурационном файле параметр 'stage2_read_from' указывает на загрузку данных БД.")
        print("Укажите точную лог дату-время данных (можно скопировать из таблицы БД) ")
        log_datetime_st1 = input("или другие варианты: 'last'- данные последней загрузки, 'all' - все данные: ")

        logging.info("Cчитываем данные из таблицы clientid_transactions")
        # Считываем данные из таблицы БД
        df_primary_data = get_data_from_table_clientid_transactions(ua_config, log_datetime_st1)

        # *** Создание 5 таблиц  *******************************************************
        logging.info("Создание 5 таблиц")
        create_five_tables(ua_config)

    #
    # *** Считываем данные из CSV-файла *********************************************
    if ua_config['stage2_read_from'] == 2:  # В настройках - считываем из CSV-файла
        # *** Запрашиваем имя CSV-файла (если читаем из файла)   *********************************************
        print("В конфигурационном файле параметр 'stage2_read_from' указывает на загрузку данных из CSV-файла.")
        file_name_csv = input("Укажите имя CSV-файла (с расширением): ")

        logging.info("Cчитываем данные из CSV-файла")
        df_primary_data = pd.read_csv(ua_config['path_csv'] + file_name_csv, sep=";", dtype={'clientid': str})

    #
    # *** Считываем параметры запроса из конфигурационного файла
    VIEW_ID = ua_config['ua_view_id']
    START_DATE = ua_config['ua_start_date']
    END_DATE = ua_config['ua_end_date']

    # Записывает параметры запроса в отдельный log-файл
    logger = setup_logger('st2', 'log/ua_stage2_get_user_activity___request_parameters.log')
    logger.info(f"*** VIEW_ID:{VIEW_ID}, START_DATE:{START_DATE}, END_DATE:{END_DATE} ***")

    # Цикл, чтобы запрашивать много диапазонов
    while True:
        #
        # *** Запрашиваем диапазон  *******************************************************
        print(f"Всего в исходном массиве - {df_primary_data.shape[0]} строк данных")
        try:
            print(f"{finish_df} - правая граница последнего диапазона")
        except:
            print("Вы еще не запускали ни одного диапазона!")
        print("Задайте диапазон.")
        start_df = int(input("ЛЕВАЯ граница (начало с 1-й) (включительно). 0 - выход из программы: "))
        if start_df == 0:
            print("Выход из программы")
            break

        finish_df = int(input("ПРАВАЯ граница (включительно). 0 - выход из программы: "))
        if finish_df == 0:
            print("Выход из программы")
            break

        df = df_primary_data.iloc[start_df - 1:finish_df, :]

        logging.info(f"Размеры датафрейма текущего диапазона: {df.shape}")

        #
        # *** Основной блок: делаем UA *********************************************

        # Получаем список client_ids
        client_ids = df.clientid.to_list()

        # Подключаемся к сервису
        analytics = authorization_api4_for_ua(ua_config)

        # Перебираем пачками по frame_size_clientid
        frame_size_clientid = ua_config['frame_size_clientid']

        print("************** Начинаем работу user_activity *****************")
        print(f"Диапазон: {start_df} (включительно) - {finish_df} (включительно). Начало отсчета - с 1")
        for ids_clientids in (client_ids[i: i + frame_size_clientid] for i in
                              range(0, len(client_ids), frame_size_clientid)):
            print(f"Очередная порция clientid для обработки, размер порции - {frame_size_clientid}")

            result_dict = get_user_activity(analytics, VIEW_ID, ids_clientids, START_DATE, END_DATE, ua_config)

            # Перебираем словарь result_dict и записываем результат
            for key in result_dict.keys():
                df = result_dict[key]

                # Оставляем только указанные в конфиг-файле CustomDimentions
                # При этом в списке должны быть значения
                if key == 'customdimensions':
                    custom_dimensions_list = ua_config['custom_dimensions_list']
                    if ua_config['custom_dimensions'] and custom_dimensions_list:
                        df = df[df['index'].astype(int).isin(custom_dimensions_list)]

                # Подчищаем таблицу
                df = perfect_df_after_ua(df, log_datetime)

                table_name = ua_config['table_prefix'] + "_" + key

                output_bd, output_csv = '', ''
                #
                # *** Записываем в БД **********************************************************************
                if ua_config['stage2_write_to_db']:  # В настройках - запись в БД
                    output_bd = "БД"
                    add_to_table(ua_config, df, table_name=table_name)

                #
                # *** Записываеи DF в CSV-файл   ************************************************************
                if ua_config['stage2_write_to_csv']:
                    output_csv = "CSV-файле"
                    file_name = f"{ua_config['path_csv']}{table_name}_{data_in_csv}.csv"
                    # Параметры: 1-дописывать файл 2-Проверять - есть ли уже заголовки
                    # Порядок полей - как в таблице БД
                    logging.info(f"Добавляем данные в файл: {file_name}")
                    df.to_csv(file_name, sep=";", mode='a', encoding='utf-8-sig', index=False,
                              header=not os.path.exists(file_name))

        if finish_df >= df_primary_data.shape[0]:
            print("Все диапазоны считаны!")
            break

    try:
        print(f"Программа закончила работу. Результаты работы user_activity в: {output_bd}, {output_csv}")
    except:
        print(f"Программа закончила работу. Результатов нет")


if __name__ == "__main__":
    main()
