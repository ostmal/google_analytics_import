from src.importing_data_from_ga import *
from src.different_procedures import *

import logging

# --- Google Analytics API ver 4
from gaapi4py import GAClient


def authorization_api_ver4(key_file_location: object):
    """
    Авторизация для считывания данных из GA на первом этапе
    библиотека_gaapi4py
    """
    service_ver4 = GAClient(json_keyfile=key_file_location)
    return service_ver4

def get_data_from_ga_to_user_activity(ua_config):
    """
    Получаем данные из GA ver4 (Report), библиотека_gaapi4py
    Возвращаем датафрейм
    """
    key_file_location = ua_config['key_file_location']
    view_id = ua_config['report_view_id']
    start_date = ua_config['report_start_date']
    end_date = ua_config['report_end_date']
    dimentions = ua_config['report_dimentions']
    metrics = ua_config['report_metrics']

    # Приводим к типу list
    if type(dimentions) == str:
        dimentions = list[dimentions]
    if type(metrics) == str:
        metrics = [metrics]

    params = {
        'view_id': str(view_id),
        'start_date': start_date,
        'end_date': end_date,
        'dimensions': {*dimentions},
        'metrics': {*metrics},
        'samplingLevel': 'LARGE'
    }

    # Добавляем параметры
    try:
        params['filter'] = ua_config['report_filters']
    except:
        pass

    # Авторизуемся
    logging.info("Авторизуемся в GA")
    service_ver4 = authorization_api_ver4(key_file_location)

    logging.info("Делаем запрос к GA-3 (API ver4). Параметры запроса:")
    # Записывает параметры запроса в отдельный файл
    logger = setup_logger('st1', 'log/ua_stage1_get_clientids___request_parameters.log')
    logger.info(f"{params}")

    # Делаем запрос
    response = get_response_expo(service_ver4, params)
    df = response['data']

    # Метаданные - забираем параметры семплирования
    metadata = response['info']
    if metadata['samplesReadCounts'] or metadata['samplingSpaceSizes']:
        print(
            f"Обнаружено семплирование!!! samplesReadCounts (выборка) - {metadata['samplesReadCounts']} | samplingSpaceSizes (генер.совокупность) - {metadata['samplingSpaceSizes']}")
        logging.info(
            f"Обнаружено семплирование!!! samplesReadCounts (выборка) - {metadata['samplesReadCounts']} | samplingSpaceSizes (генер.совокупность) - {metadata['samplingSpaceSizes']}")
    else:
        print("Отлично!!! Семплирования нет!")
        logging.info("Отлично!!! Семплирования нет!")

    return df


def perfect_df_for_ua(df):
    """
    Подчистить (преобразовать) df
    """

    # *
    # *** Записываем значение log_datetime - текущая дата-время (логирование) *****************************
    df['log_datetime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # *
    # *** Сортируем по transactions *****************************
    df = df.sort_values('transactions', ascending=False)

    return df
