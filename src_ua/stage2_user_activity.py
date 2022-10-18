import json
import datetime
import logging

import pandas as pd

from datetime import timedelta
from googleapiclient.errors import HttpError
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

# Packages needed for authentication
import httplib2 as lib2  # Example of the "as" function
import google.oauth2.credentials  # Importing a sub-package
from google_auth_httplib2 import AuthorizedHttp

# Packages needed for connecting with Google API
from googleapiclient.discovery import build as google_build  # An example with all the statements together
# from googleapiclient.errors import HttpError
from apiclient.errors import HttpError

from src.ga_api_base import *


def authorization_api4_for_ua(ua_config):
    """
    Авторизация для User Activity
    """

    analytics = get_service(
        api_name='analyticsreporting',
        api_version='v4',
        scopes=['https://www.googleapis.com/auth/analytics.readonly'],
        key_file_location=ua_config['key_file_location'])

    return analytics


def get_user_activity(api_service, view_id, client_ids, start_date, end_date, ua_config):
    """
    Получить User Activity по списку клиентов
    client_ids - список client_id
    """

    table_names = ['visits', 'hits', 'customdimensions', 'goals', 'products']
    response_arr = {'visits': [], 'hits': [], 'customdimensions': [], 'goals': [], 'products': []}

    if type(client_ids) == str:
        client_ids = [client_ids]
    n = 0
    for client_id in client_ids:
        n +=1
        print(f"{n:03} из {len(client_ids)}: {client_id}")
        # TODO: async
        # TODO: use function user_activity_sample_handling

        # Получаем данные по 1 клиенту (в формате JSON)
        api_response = user_activity_request(api_service, view_id, client_id, start_date, end_date)
        if type(api_response) != HttpError:
            if api_response['sampleRate'] != 1:
                # Семплирование!!!
                print(f"Есть семплирование! client_id: {client_id}, sampleRate: {api_response['sampleRate']}")
                logging.info(f"Есть семплирование! client_id: {client_id}, sampleRate: {api_response['sampleRate']}")
            # Парсим
            parsed_dict = get_parse_one_response(api_response, client_id)
            for key in parsed_dict.keys():
                # Раскидываем данные по 5 спискам (они внутри словаря)
                response_arr[key].append(parsed_dict[key])
        else:
            print(f'*** Warning! {client_id} has not found')
            logging.info(f'*** Warning! {client_id} has not found')


    processed_dttm = pd.Timestamp.now()
    result_dict = dict()
    sort_names = {
        'visits': ['sessionId', 'deviceCategory', 'platform', 'dataSource', 'sessionDate', 'clientId', 'source_system',
                   'processed_dttm'],
        'hits': ['activityTime', 'source', 'medium', 'channelGrouping', 'campaign', 'keyword', 'hostname',
                 'landingPagePath', 'activityType', 'hitnumber', 'event_eventCategory', 'event_eventAction',
                 'event_eventCount', 'ecommerce_actionType', 'ecommerce_ecommerceType', 'event_eventLabel',
                 'ecommerce_transaction_transactionId', 'ecommerce_transaction_transactionRevenue',
                 'ecommerce_transaction_transactionShipping', 'pageview_pagePath', 'pageview_pageTitle',
                 'ecommerce_transaction_transactionTax', 'event_eventValue', 'clientId', 'sessionId', 'source_system',
                 'processed_dttm'],
        'customdimensions': ['index', 'value', 'clientId', 'sessionId', 'hitnumber', 'source_system', 'processed_dttm'],
        'goals': ['goalIndex', 'goalCompletions', 'goalCompletionLocation', 'goalPreviousStep1', 'goalPreviousStep2',
                  'goalPreviousStep3', 'goalName', 'clientId', 'sessionId', 'hitnumber', 'source_system',
                  'processed_dttm'],
        'products': ['productSku', 'productName', 'itemRevenue', 'productQuantity', 'clientId', 'sessionId',
                     'hitnumber', 'source_system', 'processed_dttm']
    }

    # *
    # ******************** Перерабатываем response_arr **************************************
    for key in response_arr.keys():
        # *** Пропускаем CustomDimensions (если в конфиг-файле - no) ******************************************************
        if not ua_config['custom_dimensions'] and key == 'customdimensions':
            continue

        result_dict[key] = pd.concat(response_arr[key])
        result_dict[key]['source_system'] = 'ga:' + str(view_id)
        result_dict[key]['processed_dttm'] = processed_dttm
        # result_dict[key].info()
        # TODO: добавить сортировку полей и добавление полей, которые не существуют
        for column_name in sort_names[key]:
            if column_name not in result_dict[key]:
                result_dict[key][column_name] = pd.NA
        result_dict[key] = result_dict[key][sort_names[key]]
    del response_arr
    return result_dict


def user_activity_request(api_service, view_id, client_id, start_date, end_date):
    """
    Making request and getting response
    Запрос activity_request на получение ОДНОГО клиента
    Ограничение 100тыс записей в день (пока не достигало  )
    """

    request = api_service.userActivity().search(
        body={
            "viewId": view_id,
            "user": {
                "type": "CLIENT_ID",
                "userId": client_id
            },
            "dateRange": {
                "startDate": start_date,
                "endDate": end_date,
            },
            "pageSize": 100000
        }
    )
    try:
        res = request.execute()
    except HttpError as e:
        return e

    return res


def get_parse_one_response(response, client_id):
    """
    Parse result of json into dataframes
    Распарсить результат одного клиента на таблицы
    """
    # Добавить hitnumber
    for session in response['sessions']:
        session['clientId'] = client_id
        for index, value in enumerate(session['activities']):
            value['hitnumber'] = len(session['activities']) - index

    # Sessions
    sessions_df = pd.json_normalize(response['sessions'])
    sessions_df.drop(columns=['activities'], inplace=True)

    # Hits
    hits_df = pd.json_normalize(response['sessions'], record_path=['activities'], meta=['clientId', 'sessionId'],
                                sep='_')
    # hits_df.info()

    event_json = hits_df.to_json(orient='records')

    # CustomDimensions
    try:
        customdimensions_df = pd.json_normalize(json.loads(event_json), record_path=['customDimension'],
                                                meta=['clientId', 'sessionId', 'hitnumber'])
    except:
        customdimensions_df = None
    # Goals
    try:
        goals_df = pd.json_normalize(json.loads(event_json), record_path=['goals_goals'],
                                     meta=['clientId', 'sessionId', 'hitnumber'])
    except:
        goals_df = None
    # Products
    try:
        products_df = pd.json_normalize(json.loads(event_json), record_path=['ecommerce_products'],
                                        meta=['clientId', 'sessionId', 'hitnumber'])
    except:
        products_df = None
    hits_df.drop(columns=['customDimension', 'goals_goals', 'ecommerce_products'], errors='ignore', inplace=True)
    return {
        'visits': sessions_df,
        'hits': hits_df,
        'customdimensions': customdimensions_df,
        'goals': goals_df,
        'products': products_df}


def perfect_df_after_ua(df, log_datetime):
    """
    Подчистить (преобразовать) df
    """

    # *
    # *** Переведем все наименования колонок в нижний регистр ****************************
    df.columns = df.columns.str.lower()

    # *
    # *** Названия колонок - сортируем по возрастанию ****************************
    df = df[sorted(df.columns.to_list())]

    # *
    # *** Записываем значение log_datetime - текущая дата-время (логирование) *****************************
    df['processed_dttm'] = log_datetime

    return df
