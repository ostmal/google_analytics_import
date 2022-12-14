*** Скрипт для импорта данных из Google Analytics-3 ***

В работе скрипта используется в основном API ver.4  Для отдельной функции используется API ver.3

Файл запуска: ga_import.py

В папке "etc" содержатся конфигурационные файлы:
config.yml 		- общий файл конфигурации
credential_bd.yml 	- параметры доступа к базе данных
credential_GA.yml 	- параметры доступа к GA: имя JSON-файла для доступа к Google Anslytics, перечень ViewId
dimensions_metrics.yml	- перечень всех Dimensions & Metrics для импорта, некоторые настройки, связанные с Dimensions & Metrics

Скрипт имеет два основных режима работы:
- Режим 1 (Параметр "date_range: yes" в файле "config"): пользователь должен вручную указать диапазон дат для скачивания (для каждого view_id)
- Режим 2 (Параметр "date_range: no" в файле "config"): программа определяет последнюю дату в основном файле (имя указано в файле "config") и последовательно импортирует данные по каждому дню до даты, предшедствующей текущей. Если данных в основной таблице нет - дата запрашивается у пользователя.
Есть возможность сохранить импортируемые данные в csv-файл (параметр "write_to_csv: yes" в файле "config").
Параметр "date_range: yes" - определяет, что необходимо задавать конкретный диапазон дат, или (no) - последняя дата ищется в БД и считываются недостающие данные по вчерашнее число включительно.


Отдельные параметры работы скрипта
- Можно указывать отдельные Dimentions, которые запрашиваются отдельно, но сливаются в общую таблицу 
- Можно указывать CustomDimentions (в паре с Metrics) и они записываются в отдельную таблицу
- Можно указывать отдельные Dimentions и Metrics (сейчас это те, что, связаны с контекстом "event...") и они также записываются в отдельную таблицу


Возможен запуск из командной строки. Если параметр отсутствует, то он берется по умолчанию. Ключи для параметров:
-config		имя общего файла конфигурации (etc\config.yml - по умолчанию)
-credential_bd	параметры доступа к базе данных (etc\credential_bd.yml - по умолчанию) 
-credential_ga	параметры доступа к GA (etc\credential_GA.yml - по умолчанию) 
-dim_met	перечень всех Dimensions & Metrics (etc\dimensions_metrics.yml - по умолчанию) 
-view_id	перечень ViewId (по умолчанию берётся из файла credential_GA.yml)

Пример запуска из командной строки:
python ga_import.py  -config etc\config.yml -credential_bd etc\credential_bd.yml -credential_ga etc\credential_GA.yml -dim_met etc\dimensions_metrics.yml -view_id 149451125 223029378


Установка скрипта: 
1. Переписать все папки и файлы
2. Для установки окружения запустить: pip install -r requirements.txt
