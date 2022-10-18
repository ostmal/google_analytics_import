from src.ga_api_base import *
from src.operations_with_database import *

from gaapi4py import GAClient
import logging
from datetime import timedelta
import datetime
import os
import math
import pandas as pd


def processing_data_one_view_id(service, view_id, view_id_value, dimensions_metrics, config, credential_bd):
    """
    Получение всех данных по конкретному view_id из GA и запись в БД (файл)
    :return:
    """

    # Для формирования CSV-файла
    now = datetime.datetime.now()
    data_in_csv = now.strftime("%Y-%m-%d__%H-%M")

    # Фильтрация по hostname (прописано в файле конфигурации)
    try:
        filters = dimensions_metrics['filters']
        filters = filters.strip().replace(" ", "")
    except:
        filters = ""

    #
    # *** Перебираем даты:  DateFrom -> DateTo ******************************
    current_date = view_id_value['DateFrom']
    end_date = view_id_value['DateTo']
    delta = timedelta(days=1)
    while current_date <= end_date:
        logging.info("\n")
        logging.info(
            f"\n'{view_id_value['account_name']}' - {current_date.strftime('%Y-%m-%d')} - ***** ОТРАБАТЫВАЕМ ЭТУ ДАТУ *****")

        # ***
        # *** Отрабатываем ОСНОВНУЮ таблицу ********************************************
        # ***
        if int(dimensions_metrics['options_ga_and_custom_dimentions']) in [1, 2]:
            logging.info("Приступаем к оттрабатке ОСНОВНОЙ таблицы")
            # Получаем датафрейм для записи
            df = together_all_df_from_ga(service, view_id, view_id_value, dimensions_metrics,
                                         current_date.strftime("%Y-%m-%d"), filters)  # Получаем датафрейм для записи
            # Обработка таблицы df_event
            try:
                if dimensions_metrics['list_dimentions_event']:
                    df_event = together_all_df_from_ga(service, view_id, view_id_value, dimensions_metrics,
                                                       current_date.strftime("%Y-%m-%d"), filters,
                                                       event="_event")
            except:
                pass

            table_name = credential_bd['table_name']
            # Проверяем файл конфигурации: записывать в CSV-файл?
            if config['write_to_csv']:
                file_name = f"{config['path_csv']}{table_name}_{view_id_value['account_name']}_{data_in_csv}.csv"
                # Параметры: 1-дописывать файл 2-Проверять - есть ли уже заголовки
                # Порядок полей - как в таблице БД
                columns_for_output = get_column_names(credential_bd, table_name)
                df[columns_for_output].to_csv(file_name, sep=";", decimal=',', mode='a', encoding='utf-8-sig',
                                              index=False, header=not os.path.exists(file_name))

                # event
                try:
                    table_name = credential_bd['table_name'] + "_event"
                    file_name = f"{config['path_csv']}{table_name}_{view_id_value['account_name']}_{data_in_csv}.csv"
                    columns_for_output = get_column_names(credential_bd, table_name)
                    df_event[columns_for_output].to_csv(file_name, sep=";", decimal=',', mode='a', encoding='utf-8-sig',
                                                        index=False, header=not os.path.exists(file_name))
                except:
                    pass
            else:
                # Записываем данные из ДатаФрейма в таблицу БД
                add_to_table(credential_bd, df, table_name=credential_bd['table_name'].lower())

                # Записываем данные из ДатаФрейма (EVent) в таблицу БД
                try:
                    if dimensions_metrics['list_dimentions_event']:
                        add_to_table(credential_bd, df_event, table_name=credential_bd['table_name'].lower() + "_event")
                except:
                    pass

        # ***
        # *** Отрабатываем таблицы с CustomDimentions ********************************************
        # ***
        if int(dimensions_metrics['options_ga_and_custom_dimentions']) in [1, 3]:
            logging.info("")
            logging.info("******** Приступаем к оттрабатке таблицы с CustomDimentions *******")
            # Перебираем все пары с кастомными DIM-MET
            list_of_lists_customdimentions_metrics = dimensions_metrics['list_of_lists_customdimentions_metrics']
            for custom_dim_met in list_of_lists_customdimentions_metrics:
                # Получаем датафрейм для записи
                # Получаем текущий датафрейм
                list_dimentions_keys = dimensions_metrics['list_dimentions_keys']
                current_list_dimentions = list_dimentions_keys + custom_dim_met[:-1]
                df = import_from_GA_BIG_dataframe(service, view_id,
                                                  current_list_dimentions,
                                                  [custom_dim_met[-1]],
                                                  current_date.strftime("%Y-%m-%d"),
                                                  filters)
                # Подчистить (преобразовать) датафрейм
                df = perfect_df(df, view_id, dimensions_metrics)

                # Наименование таблицы с CustomDimension
                cust_dim_table_name = credential_bd['table_name'] + "_" + custom_dim_met[0].split(":")[
                    1].lower() + "_" + custom_dim_met[1].split(":")[1].lower()
                # Проверяем файл конфигурации: записывать в CSV-файл?
                if config['write_to_csv']:
                    file_name = f"{config['path_csv']}{cust_dim_table_name}_{view_id_value['account_name']}_{data_in_csv}.csv"
                    columns_for_output = get_column_names(credential_bd, cust_dim_table_name)
                    # Параметры: 1-дописывать файл 2-Проверять - есть ли уже заголовки
                    df[columns_for_output].to_csv(file_name, sep=";", decimal=',', mode='a', encoding='utf-8-sig',
                                                  index=False, header=not os.path.exists(file_name))
                else:
                    # Добавление данные из ДатаФрейма в таблицу БД
                    add_to_table(credential_bd, df, table_name=cust_dim_table_name)

        current_date += delta

    return True


def get_response_expo(service, params):
    """
    Возвращает объект запроса
    Используется алгоритм экспоненциальной задержки
    params: параматры запроса

    ОПИСАНИЕ СТАНДАРТНЫХ ОШИБОК:
    https://developers.google.com/analytics/devguides/reporting/mcf/v3/mcfErrors?hl=ru

    userRateLimitExceeded(403) - Пользователь превысил ограничение на количество запросов.
    По умолчанию с одного IP-адреса можно выполнять 1 запрос в секунду. Это ограничение можно увеличить в Google API Console,
    но число запросов в секунду в любом случае не должно превышать 10. Рекомендация: Попробуйте применить алгоритм экспоненциальной выдержки. Уменьшите скорость отправки запросов.

    quotaExceeded(403) - Достигнуто ограничение в 10 параллельных запросов на один профиль для Core Reporting API. Рекомендация: Попробуйте применить алгоритм экспоненциальной выдержки.
    Подождите, пока будет завершен хотя бы один выполняемый запрос.

    internalServerError(500) - Непредвиденная ошибка сервера. Рекомендация: Не выполняйте этот запрос повторно (только через задержку).

    backendError(503) - Ошибка сервера. Рекомендация: Не выполняйте этот запрос повторно (только через задержку).
    """

    # --- Эскпоненциальная задержка ---------------------------
    try:
        logging.info(f"Параметры запроса: {params}")
        logging.info("Запуск - query.execute()")
        response = service.get_all_data(params)
        return response
    except HttpError as error:
        logging.info(f"HttpError --- {error}")
        logging.info(f"Эскпоненциальная задержка: Запрос пока не прошел. Ответ GA: {error.status_code}")
        if error.status_code in [403, 500, 503]:
            logging.info("Эскпоненциальная задержка: код указанной ошибке позволяет запустить повтор...")
            t = 0
            expontential_backoff = 2
            while t < 10:
                logging.info(f"Эскпоненциальная задержка: ЗАДЕРЖКА - {2 ** expontential_backoff} сек")
                time.sleep(2 ** expontential_backoff)
                try:
                    response = service.get_all_data(params)
                    return response
                except:
                    logging.info(f"Эскпоненциальная задержка: Запрос пока не прошел. Ответ GA: {error.status_code}")
                    expontential_backoff += 1
                    t += 1
                    logging.info(
                        f"Эскпоненциальная задержка: Увеличен период экспоненциальной задержки до {2 ** expontential_backoff} сек., попытка №{t}")
                    if t >= 10:
                        logging.info(
                            f"Было предпринято {t} попыток сделать запрос. Все попытки не помогли. Возможно перегружен сервер. Необходимо повторить позже.")
                        sys.exit()
                        return False
                    else:
                        continue
        elif error.status_code == 429:
            logging.error(
                f"Error! Исчерпан лимит на объем дневного запроса. {error} ----- {error.status_code}. Выход из программы")
            sys.exit(
                f"Error! Исчерпан лимит на объем дневного запроса. {error} ----- {error.resp.reason}. Выход из программы")
        else:
            logging.error(f"Запрос к GA не прошел. Ошибка: {error} ----- {error.status_code}")
            sys.exit(f"Error: Запрос к GA не прошел. Ошибка: {error} ----- {error.resp.reason}. Выход из программы")


def import_from_GA_BIG_dataframe(service, view_id, dimentions, metrics, my_date, filters):
    """
    Переписано для GA-3 API ver.4
    В GA может быть семплирование. Если 'samplesReadCounts': None + samplingSpaceSizes': None - семплирования нет
    Процедура обходит это ограничение.
    В качестве базового запроса используется процедура: "import_from_GA"

    Логика работы текущей процедуры:
    - Проверяются samplesReadCounts + samplingSpaceSizes.
    - Если они их < 1 млн --> выполняется "import_from_GA_raw_to_dataframe"
    - В противном случае использунтся фильтрация по Dimension "hour": "filters':'ga:hour==..."
    - При этом используется БАЗОВЫЙ список из перечня всех часов (00 - 23)
    - От него откусывается часть (используется коэффициент PART_OF_LIST). Если не получается - часть от части и т.д.
    - далее берется оставшийся кусочек и т.д.
    -
    - Общий DF склеивается из отдельных
    - На каждом этапе, также с помощью "results.get('totalResults')" проверяется кол- во выводимых результатов:
    - Применяется по Dimension "hour": "filters':'ga:minute==..."
    """
    logging.info(f"dimentions:   {dimentions}")
    logging.info(f"metrics:   {metrics}")

    params = {
        'view_id': str(view_id),
        'start_date': my_date,
        'end_date': my_date,
        'dimensions': {*dimentions},
        'metrics': {*metrics},
        'samplingLevel': 'LARGE',
        'filter': 'ga:deviceCategory==mobile;ga:sessionCount==32'
    }

    if filters:
        params['filter'] = filters

    logging.info(f"params --- {params}")

    # Получаем запрос
    response = get_response_expo(service, params)

    metadata = response['info']  # sampling and "golden" metadata
    logging.info(f"metadata: {metadata}")

    df = response['data']  # Pandas dataframe that contains data from GA
    logging.info(f"df.shape: {df.shape}")

    # Для проверки (10000 или 10001)
    count_row = df.shape[0]

    # ****************************************************************************************
    # *** Разбивка на ЧАСЫ - если: 1.Семплирование или 2.Данные в ненскольких фреймах
    # ****************************************************************************************
    if metadata['nextPageToken'] or metadata['samplesReadCounts'] or metadata[
        'samplingSpaceSizes'] or count_row == 10000 or count_row == 10001:
        logging.info(
            f"Внимание!!! Обнаружено Семплирование или Данные в ненскольких фреймах! Дробим запрос с помощью фильтрации (ga:hour==)")
        # Перебираем ЧАСЫ: делаем 24 запроса с фильтрами по часам
        df = pd.DataFrame()
        list_dfs = []
        PART_OF_LIST = 0.7
        # Сгенерируем список часов (00 - 23)
        hour_list = [str(el).zfill(2) for el in range(24)]
        start_pos = 0
        len_current_hour_list = 24

        len_current_hour_list = int(round(len_current_hour_list * PART_OF_LIST, 0))
        finish_pos = start_pos + len_current_hour_list
        logging.info(f"start_pos - {start_pos}")
        logging.info(f"finish_pos - {finish_pos}")
        logging.info(f"len_current_hour_list - {len_current_hour_list}")

        while True:
            current_hour_list = hour_list[start_pos: finish_pos]
            list_to_request = ",".join(["ga:hour==" + el for el in current_hour_list])
            logging.info(f"К текущему значению фильтра будет добавлено (через логическое И):   {list_to_request}")

            # Добавляем фильтр. Если в файле конфигурации указана фильтрация по "hostname" - комбинируем через "И"
            logging.info(f"****** до установки фильтра ******************  filters --- |{filter}|")
            if filters:
                params['filter'] = filters + ";" + list_to_request
                logging.info(f"************************  |{params['filter']}|")
            else:
                params['filter'] = list_to_request
                logging.info(f"************************  |{params['filter']}|")

            # Делаем запрос
            response = get_response_expo(service, params)
            metadata = response['info']  # sampling and "golden" metadata
            logging.info(f"metadata: {metadata}")

            if metadata['nextPageToken'] or metadata['samplesReadCounts'] or metadata[
                'samplingSpaceSizes'] or count_row == 10000 or count_row == 10001:
                logging.info("Необходимо уменьшить список ЧАСОВ (уменьшить фильтр)")
                if len_current_hour_list == 1:
                    # Надо дробить час.....
                    logging.error("Надо дробить час.....")
                    break

                len_current_hour_list = int(round((finish_pos - start_pos) * PART_OF_LIST, 0))
                finish_pos = start_pos + len_current_hour_list
                logging.info(f"start_pos - {start_pos}")
                logging.info(f"finish_pos - {finish_pos}")
                logging.info(f"len_current_hour_list - {len_current_hour_list}")

            else:
                # Склеиваем датафрейм
                list_dfs.append(response['data'])
                # просмотрели последний диапазон?
                if finish_pos == 24:
                    # Склеиваем датафреймы
                    df = pd.concat(list_dfs)
                    return df
                # передвигаем диапазон
                start_pos = finish_pos
                finish_pos = 24
    else:

        return df


def perfect_df(df, view_id, dimensions_metrics):
    """
    Подчистить (преобразовать) df
    :param df:
    :return: df
    """
    list_metrics_numeric = dimensions_metrics['list_metrics_numeric']
    list_metrics_numeric_without_ga = list(map(lambda x: x.split(':')[1], list_metrics_numeric))  # убираем ga

    # *
    # *** Переведем все наименования колонок в нижний регистр ****************************
    df.columns = df.columns.str.lower()

    # *
    # *** Записываем значение view_id **********************************************
    df['view_id'] = view_id

    # *
    # *** Записываем значение log_datetime - текущая дата-время (логирование) *****************************
    df['log_datetime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # *
    # *** Меняем формат поля в  numeric ********************************************
    for el in list_metrics_numeric_without_ga:
        try:
            # Перевести поле в numeric
            df[el] = pd.to_numeric(df[el])
        except:
            pass
            # logging.warning(f"Нельзя перевести поле {el} в numeric - такого поля в датафрейме нет!")

    # *
    # *** Создаем поле дата *****************************************************
    try:
        df['date'] = df['datehourminute'].apply(lambda x: x[0:4] + "-" + x[4:6] + "-" + x[6:8])
    except:
        logging.warning("В конфигурационном файле нет dimention 'ga:dateHourMinute'")

    # *
    # *** Переводим название всех колонок в нижний регистр **********************
    df.columns = map(str.lower, df.columns)

    return df


def together_all_df_from_ga(service, view_id, view_id_value, dimensions_metrics, my_date, filters, event=""):
    """
    Выполняет перечень запросов к GA и собирает из многих датафреймов общий
    Входные параметры:
    service - объект подключения
    ...
    Возвращает готовый датафрейм

    Логика работы:
    В GA существует ограничение на кол-во dimensions&metrics. Во всех запросах обязательно присутствуют ТРИ ключевые dimentions
    Считаем кол-во запросов исходя из ограничений GA
    За какое кол-во итераций мы можем считать DIM и MET - берем максимальное число
    Контролируем сколько метрик остается. Если их остается столько же, сколько будущих итераций - берем только по ОДНОЙ метрике (без метрики нвозможен запрос)
    """

    # Ограничение GA
    MAX_COUNT_DIMENTIONS = 9
    MAX_COUNT_METRICS = 10

    list_dimentions_keys = dimensions_metrics['list_dimentions_keys']
    list_dimentions = dimensions_metrics[f'list_dimentions{event}']
    list_metrics = dimensions_metrics[f'list_metrics{event}']

    list_dimentions_keys_without_ga = list(map(lambda x: x.split(':')[1], list_dimentions_keys))  # убираем пробелы и ga

    # Считаем кол-во запросов исходя из ограничений GA
    # За какое кол-во итераций мы можем считать DIM и MET - берем максимальное число
    count_iterations = max(
        math.ceil(len(list_dimentions) / (MAX_COUNT_DIMENTIONS - len(list_dimentions_keys))),
        math.ceil(len(list_metrics) / MAX_COUNT_METRICS)
    )

    logging.info(f"'{view_id_value['account_name']}' - кол-во итераций (dem+met): {count_iterations}")

    step_size_dimention = math.ceil(len(list_dimentions) / count_iterations)
    logging.info(f"'{view_id_value['account_name']}' - step_size_dimention: {step_size_dimention}")

    step_size_metrics = math.ceil(len(list_metrics) / count_iterations)
    logging.info(f"'{view_id_value['account_name']}' - step_size_metrics: {step_size_metrics}")

    # Итерации - запросы API
    start_list_dimentions = 0
    start_list_metrics = 0
    finish_list_dimentions = start_list_dimentions + step_size_dimention
    finish_list_metrics = start_list_metrics + step_size_metrics
    for i in range(count_iterations):
        logging.info("\n")
        logging.info(f"ИТЕРАЦИЯ (dem+met) - {i + 1}")
        current_list_dimentions = list_dimentions_keys + list_dimentions[start_list_dimentions: finish_list_dimentions]
        current_list_metrics = list_metrics[start_list_metrics: finish_list_metrics]
        if not current_list_metrics:
            logging.info(
                "list_metrics - исчерпан. Берем запасную metric из файла канфигурации (параметр - reserve_metric)")
            try:
                current_list_metrics = [dimensions_metrics['reserve_metric'].strip()]
            except:
                logging.error(
                    "Некорректное значение параметра 'reserve_metric' в конфигурационном файле. Выход из программы")
                sys.exit(
                    "Error: Некорректное значение параметра 'reserve_metric' в конфигурационном файле. Выход из программы")

        # Получаем текущий датафрейм
        df_current = import_from_GA_BIG_dataframe(service, view_id,
                                                  current_list_dimentions,
                                                  current_list_metrics, my_date,
                                                  filters)
        logging.info(f"Размер текущего df: {df_current.shape}")

        # Удаляем столбец с метрикой, если она УЖЕ есть в df (берем последнюю метрику в списке)
        col = current_list_metrics[-1].split(':')[1]
        try:
            if col in df.columns.to_list():
                logging.info(f"Столбец (metrics) - {col} - уже есть в итоговом df!  УДАЛЯЕМ!")
                df_current.drop(col, inplace=True, axis=1)
        except:
            pass

        try:
            # Cлева - таблица бОльшего размера (в принципе - они должны быть одинаковыми)
            if df.shape[0] >= df_current.shape[0]:
                logging.info("Слияние таблиц (merge). Базовая таблица - СЛЕВА")
                df = df.merge(df_current, how='left', on=list_dimentions_keys_without_ga)
            else:
                logging.info("Слияние таблиц (merge). Базовая таблица - СПРАВА")
                df = df_current.merge(df, how='left', on=list_dimentions_keys_without_ga)
        except:
            df = df_current
        logging.info(f"Размер df ПОСЛЕ merge: {df.shape}")

        # указатель на на начало очередного списка
        start_list_dimentions += step_size_dimention
        start_list_metrics += step_size_metrics

        # Поправка: чтобы хватило метрик до конца (должна быть хотя бы одна метрика!)
        steps_to_end = count_iterations - i - 1
        if (len(list_metrics) - start_list_metrics) == steps_to_end:
            step_size_metrics = 1

        # указатель на на конец списка
        finish_list_dimentions = start_list_dimentions + step_size_dimention
        finish_list_metrics = start_list_metrics + step_size_metrics

    # *
    # ************************** ДОПОЛНИТЕЛЬНО: обрабатываем special_dimentions ****************
    # Поскольку  special_dimentions "убивают" другие dimentions - реализовал отдельный
    # параметр в файле конфигурации: list_of_lists_special_dimentions_metrics. В случае наличия
    # там значений запросы по ним выполняются отдельно.\
    # Отслеживается: дублируются ли метрики, если да – они не пишутся в итоговую таблицу

    # Только для основной таблицы
    if event == "":
        try:
            logging.info("--------------------------------------------------------------------------------------")
            logging.info(
                "Работаем с special_dimentions (параметр list_of_lists_special_dimentions_metrics в конфигурационном файле)")
            list_of_lists_special_dimentions_metrics = dimensions_metrics['list_of_lists_special_dimentions_metrics']
            # Перебираем все пары  DIM-MET
            for special_met in list_of_lists_special_dimentions_metrics:
                logging.info(f"special DIM-MET: {special_met[0]} --- {special_met[1]}")

                current_list_dimentions = list(list_dimentions_keys)
                current_list_dimentions.append(special_met[0].lower())
                current_metric = special_met[1].lower()

                # Получаем текущий датафрейм
                df_current = import_from_GA_BIG_dataframe(service, view_id,
                                                          current_list_dimentions,
                                                          [current_metric], my_date, filters)

                logging.info(f"Размер текущего df: {df_current.shape}")

                # Удаляем столбец с метрикой, если она УЖЕ есть в df
                if current_metric.split(':')[1] in df.columns.to_list():
                    logging.info(f"Столбец (metrics) - {current_metric} - уже есть в итоговом df!  УДАЛЯЕМ!")
                    df_current.drop(current_metric.split(':')[1], inplace=True, axis=1)

                try:
                    # Cлева - таблица бОльшего размера (в принципе - они должны быть одинаковыми)
                    if df.shape[0] >= df_current.shape[0]:
                        df = df.merge(df_current, how='left', on=list_dimentions_keys_without_ga)
                        logging.info("merge: Итоговая - слева")
                    else:
                        df = df_current.merge(df, how='left', on=list_dimentions_keys_without_ga)
                        logging.info("merge: Итоговая - справа")
                except:
                    df = df_current

                # print(df.iloc[:, -2].value_counts())

                logging.info(f"Размер df ПОСЛЕ merge: {df.shape}")
                logging.info(f"Список полей df: {df.columns.to_list()}")

        except:
            logging.info("Кастомных dimensions_metrics - в конфигурационном файле нет")

    # Подчистить (преобразовать) датафрейм
    df = perfect_df(df, view_id, dimensions_metrics)

    return df
