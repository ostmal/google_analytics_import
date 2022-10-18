from src_ua.ua_operations_with_database import *
from src_ua.stage1_data_from_ga import *

import yaml
import logging
import time
import socket


def main():
    """
    ВАЖНО!
    В программе используется API ver4 GA-3
    """
    time_start = time.perf_counter()

    # Устанавливаем предельное время запроса. Если этого не сделать не будет срабатывать "except HttpError as error", т.е. не будет олавливать ошибки запроса
    TIMEOUT = 600  # 10 мин
    socket.setdefaulttimeout(TIMEOUT)

    # Настройка логирования
    file_log = logging.FileHandler('log/ua_stage1_get_clientids.log')
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

    #
    # *** Создание таблицы clientid_transactions *********************************************
    if ua_config['stage1_write_to_db']:
        create_table_clientid_transactions(ua_config)

    #
    # *** Считываем данные из GA по параметрам из конфигурационного файла ****************************
    df = get_data_from_ga_to_user_activity(ua_config)
    print(f"Всего считано строк: {df.shape[0]}")

    #
    # *** Преобразуем DF (например добавляем сегодняшнюю дату-время) ****************************
    df = perfect_df_for_ua(df)

    output_bd, output_csv = '', ''
    #
    # *** Записываеи DF в БД   ******************************************************************
    table_name = ua_config['table_prefix'] + "_" + ua_config['table_name_clientid_transactions']
    # Проверяем файл конфигурации: записывать в БД?
    if ua_config['stage1_write_to_db']:
        output_bd = "БД"
        add_to_table(ua_config, df, table_name=table_name)
    #
    # *** Записываеи DF в CSV-файл   ************************************************************
    # Проверяем файл конфигурации: записывать в CSV-файл?
    if ua_config['stage1_write_to_csv']:
        output_csv = "CSV-файл"
        # Для формирования заголовка CSV-файла
        now = datetime.datetime.now()
        data_in_csv = now.strftime("%Y-%m-%d__%H-%M-%S")
        file_name = f"{ua_config['path_csv']}{table_name}_{data_in_csv}.csv"
        # Параметры: 1-дописывать файл 2-Проверять - есть ли уже заголовки
        # Порядок полей - как в таблице БД
        df.to_csv(file_name, sep=";", mode='a', index=False, header=not os.path.exists(file_name))

    time_finish = time.perf_counter()
    print(
        f"Программа подготовки данных для user_activity закончила работу. Данные записаны в: {output_bd}, {output_csv}")
    print(f"Время работы программы:  {time_finish - time_start:0.1f} секунд")


if __name__ == "__main__":
    main()
