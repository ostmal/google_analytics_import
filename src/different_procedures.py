import datetime
import logging

def validate_date(date_text):
    """
    Проверка правильного формата даты
    """
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
        return True
    except ValueError:
        print("Некорректный формат даты, правильно: YYYY-MM-DD")
        return False


def setup_logger(name, log_file, level=logging.INFO):
    """Создание отдельного логера"""
    # TODO надо исправить - пишет несколько экземпляров
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

