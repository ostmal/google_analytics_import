from src.importing_data_from_ga import *
from src.different_procedures import *

import pandas as pd
import yaml
from datetime import date, timedelta
import sys
import logging
import argparse
import time

import socket


def createParser():
    """
    Функүия разбора параметров командной строки
    :return: экземпляр объекта с парсингом командной строки
    """

    # Создаем экземпляр parser
    parser = argparse.ArgumentParser()

    # Парсим все наименования фаилов. Если не указано - берем по умолчанию
    parser.add_argument('-config', type=argparse.FileType(), default=r'etc\config.yml')
    parser.add_argument('-credential_ga', type=argparse.FileType(), default=r'etc\credential_GA.yml')
    parser.add_argument('-dim_met', type=argparse.FileType(), default=r'etc\dimensions_metrics.yml')
    parser.add_argument('-credential_bd', type=argparse.FileType(), default=r'etc\credential_bd.yml')

    # view_id - они "перебивает" view_id в файле credential_GA.yml
    parser.add_argument('-view_id', nargs='+', default=[], type=int)

    return parser


def main():
    """
    ВАЖНО!
    В программе используется API-4 GA-3
    В отдельных случаях  используется API-3 GA-3 (функция "get_all_view_id")
    :return:
    """
    # Устанавливаем предельное время запроса. Если этого не сделать не будет срабатывать "except HttpError as error", т.е. не будет олавливать ошибки запроса
    TIMEOUT = 600  # 10 мин
    socket.setdefaulttimeout(TIMEOUT)

    # Настройка логирования
    file_log = logging.FileHandler('log/ga_import.log')
    console_out = logging.StreamHandler()
    logging.basicConfig(handlers=(file_log, console_out),
                        format='[%(asctime)s | %(module)s | %(funcName)s | %(levelname)s]: %(message)s',
                        datefmt='%m.%d.%Y %H:%M:%S',
                        level=logging.INFO)
    for _ in range(3): logging.info("******************************************************************")

    #
    # *** Парсим командную строку *************************************************
    parser = createParser()
    namespace = parser.parse_args()

    config = namespace.config.name
    credential_bd = namespace.credential_bd.name
    credential_ga = namespace.credential_ga.name
    dim_met = namespace.dim_met.name

    #
    # *** Считывание конфигурационных фајлов ***************************************

    # Считывание обһих параметров конфигурации
    with open(config) as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    # Считывание параметров доступа к БД
    with open(credential_bd) as file:
        credential_bd = yaml.load(file, Loader=yaml.FullLoader)

    # Считывание параметров доступа к GA
    with open(credential_ga) as file:
        credential_GA = yaml.load(file, Loader=yaml.FullLoader)

    # Считывание параметров - всех Dimensions & Metrics, которые будут скачены
    with open(dim_met) as file:
        dimensions_metrics = yaml.load(file, Loader=yaml.FullLoader)

    # Список всех "view_id"
    try:
        if namespace.view_id:
            view_id_list = list(map(int, namespace.view_id))
            logging.info("view_id - берем из командной строки")
        else:
            view_id_list = credential_GA['view_id']
            logging.info("view_id - берем из файла с параметрами доступа к GA (credential_GA.yml)")
        logging.info(f"view_id_list: {view_id_list}")
        # Словарь словарей view_id_dic
        view_id_dic = {el: {} for el in view_id_list}
    except:
        logging.error(
            "Некорректное значение параметра 'view_id' в конфигурационном файле. Выход из программы")
        sys.exit("Error: Некорректное значение параметра 'view_id' в конфигурационном файле. Выход из программы")

    #
    # *** Создание таблицы RAW (если ее еще нет) ************************************
    try:
        if int(dimensions_metrics['options_ga_and_custom_dimentions']) in [1, 2]:
            logging.info("Создание таблицы RAW (если ее еще нет)")
            create_table_raw(credential_bd, dimensions_metrics)
            # Создание таблицы для для Dimentions, связанные с контекстом "event..."
            try:
                if dimensions_metrics['list_dimentions_event']:
                    create_table_raw(credential_bd, dimensions_metrics, event='_event')
            except:
                logging.info("Таблица ..._event - не будет создана")

    except:
        logging.error(
            "Некорректное значение параметра 'options_ga_and_custom_dimentions' в конфигурационном файле. Выход из программы")
        sys.exit("Error: Некорректное значение параметра 'options_ga_and_custom_dimentions' в конфигурационном файле. Выход из программы")

    #
    # *** Создание таблиц RAW - for Custom Dimensions (если ее еще нет) ************************************
    try:
        if int(dimensions_metrics['options_ga_and_custom_dimentions']) in [1, 3]:
            logging.info("Создание таблиц RAW - for Custom Dimensions (если ее еще нет)")
            try:
                list_of_lists_customdimentions_metrics = dimensions_metrics['list_of_lists_customdimentions_metrics']
                # Перебираем все пары с кастомными DIM-MET
                for custom_dim_met in list_of_lists_customdimentions_metrics:
                    create_table_raw_customdimentions(credential_bd, dimensions_metrics, custom_dim_met)
            except:
                logging.info(
                    "Некорректный параметр 'list_of_lists_customdimentions_metrics' в конфигурационном файле. Выход из программы")
                sys.exit("""Error: Некорректный параметр 'list_of_lists_customdimentions_metrics' в конфигурационном файле.
                    Если вам этот параметр не нужен - поставьте корректное значение параметра 'options_ga_and_custom_dimentions'
                    Выход из программы""")
    except:
        logging.error(
            "Некорректное значение параметра 'options_ga_and_custom_dimentions' в конфигурационном файле. Выход из программы")
        sys.exit("Error: Некорректное значение параметра 'options_ga_and_custom_dimentions' в конфигурационном файле. Выход из программы")

    # *
    # *** Подключаемся к сервису GA (Авторизация) ***********************************************

    # Имя файла для подключения
    key_file_location = credential_GA['json_file']
    view_id = credential_GA['view_id']

    # Подключение к сервису GA
    try:
        service = authorization_api(key_file_location)
        service_ver4 = authorization_api_ver4(key_file_location)
        logging.info("Успешное подключение к сервису Google Analitycs")
    except:
        logging.error("Ошибка подключения к сервису Google Analitycs")
        sys.exit("Error: Ошибка подключения к сервису Google Analitycs")

    # Считываем СЛОВАРЬ {view_id: Account Name}
    all_view_id_dic = get_all_view_id(service)

    # Пишем в словарь view_id_dic наименования аккаунтов (следует иметь ввиду, что словарь view_id_dic - подмножество словаря account_dic)
    for view_id in view_id_dic:
        view_id_dic[view_id]['flag_import'] = True  # Импорт по этому view_id - выполнять
        try:
            view_id_dic[view_id]['account_name'] = all_view_id_dic[view_id]
        except:
            logging.warning(f"{view_id} - такого view_id не существует !")
            view_id_dic[view_id]['flag_import'] = False

    #
    # *** Формирование диапазона дат для всех view_id ***********************************
    for view_id in view_id_dic:
        #
        # *** Проверяем файл конфигурации: период задается вручную? *********************
        if config['date_range']:
            flag = False
            while flag == False:
                tmp = input(f"'{view_id_dic[view_id]['account_name']}' - Введите начальную дату в формате YYYY-MM-DD: ")
                flag = validate_date(tmp)
            view_id_dic[view_id]['DateFrom_str'] = tmp

            flag = False
            while flag == False:
                tmp = input(f"'{view_id_dic[view_id]['account_name']}' - Введите конечную дату в формате YYYY-MM-DD: ")
                flag = validate_date(tmp)
            view_id_dic[view_id]['DateTo_str'] = tmp

            view_id_dic[view_id]['DateFrom'] = pd.to_datetime(view_id_dic[view_id]['DateFrom_str'])
            view_id_dic[view_id]['DateTo'] = pd.to_datetime(view_id_dic[view_id]['DateTo_str'])
        else:
            # Ищем последнюю дату в текущей таблице
            latest_date = get_latest_date_from_table_GA(credential_bd, view_id, view_id_dic[view_id])
            logging.info(f"'{view_id_dic[view_id]['account_name']}' - Последняя дата в GA-таблице - {latest_date}")

            # DateFrom: latest_date+1day
            view_id_dic[view_id]['DateFrom'] = pd.to_datetime(latest_date) + pd.DateOffset(1)
            # DateTo - вчерашняя дата
            view_id_dic[view_id]['DateTo'] = (date.today() - pd.DateOffset(1))

        # Проверяем, что начальная дата не превосходит начальную
        if view_id_dic[view_id]['DateFrom'] > view_id_dic[view_id]['DateTo']:
            logging.info(
                f"'{view_id_dic[view_id]['account_name']}' - В базе данных есть самые последние данные! Обновление не требуется!")
            view_id_dic[view_id]['flag_import'] = False  # Импорт не данных не делать

    #
    # *** ИМПОРТИРУЕМ - ОБРАБАТЫВАЕМ - ЗАПИСЫВАЕМ RAW-данные (перебираем ВСЕ view_id) ****************************
    time_start = time.perf_counter()
    for view_id in view_id_dic:
        if view_id_dic[view_id]['flag_import']:  # Можно делать импорт
            processing_data_one_view_id(service_ver4, view_id, view_id_dic[view_id], dimensions_metrics, config,
                                        credential_bd)
        else:
            logging.info(f"'{view_id_dic[view_id]['account_name']}' - Не делаем импорт данных из GA!")

    time_finish = time.perf_counter()
    print(f"Время работы программы:  {time_finish - time_start:0.1f} секунд")


if __name__ == "__main__":
    main()
