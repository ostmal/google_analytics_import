import time
import logging
import sys

# --- Google Analytics
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

# При ошибках авторизации возвращается ошибка oauth2client.client.AccessTokenRefreshError,
# которая свидетельствует о проблеме с токеном авторизации.
# В этом случае приложение перенаправляет пользователя к процессу авторизации для получения нового токена.
from oauth2client.client import AccessTokenRefreshError

# При ошибках Management API возвращается ошибка apiclient.errors.HttpError, которая свидетельствует о проблемах
# с доступом к API. В этом случае необходимо ознакомиться с сообщением об ошибке
# и исправить процедуру доступа приложения к API.
from apiclient.errors import HttpError


def get_service(api_name, api_version, scopes, key_file_location):
    """
    Получаем сервис (объект), который взаимодействует с Google API

    Аргументы:
        api_name: Имя API-сервиса
        api_version: Версия API
        scopes: A list auth scopes to authorize for the application.
        key_file_location: Имя JSON файла

    Returns:
        Служба, подключенная к указанному API
    """

    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        key_file_location, scopes=scopes)

    # Build the service object.
    service = build(api_name, api_version, credentials=credentials)

    return service


def authorization_api(key_file_location: object) -> object:
    """
    Авторизация
    """
    scope = 'https://www.googleapis.com/auth/analytics.readonly'

    # Конструируем службу
    service = get_service(
        api_name='analytics',
        api_version='v3',
        scopes=[scope],
        key_file_location=key_file_location)

    return service


def get_all_view_id(service):
    """
    Вовращает словарь из всех view_id:
        view_id: Account Name
    """
    account_dic = {}

    try:
        accounts = service.management().accounts().list().execute()

    except TypeError as error:
        # Handle errors in constructing a query.
        logging.exception('There was an error in constructing your query : %s' % error)

    except HttpError as error:
        # Handle API errors.
        logging.exception('There was an API error : %s : %s' % (error.resp.status, error.resp.reason))

    # *** Перебираем объект "accounts"
    for account in accounts.get('items', []):
        AccountName = account.get('name')

        # *** get profile_id
        account_id = account.get('id')
        # Get a list of all the properties for the first account.
        properties = service.management().webproperties().list(accountId=account_id).execute()

        if properties.get('items'):
            # Get the first property id.
            property = properties.get('items')[0].get('id')

        try:
            # Получаем список  view_id
            profiles = service.management().profiles().list(accountId=account_id, webPropertyId=property).execute()
        except TypeError as error:
            # Handle errors in constructing a query.
            logging.info('There was an error in constructing your query : %s' % error)
        except HttpError as error:
            # Handle API errors.
            logging.info('There was an API error : %s : %s' % (error.resp.status, error.resp.reason))

        # *** Перебираем объект "profiles"
        for profile in profiles.get('items', []):
            account_dic[int(profile.get('id'))] = AccountName

    return account_dic