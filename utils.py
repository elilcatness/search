import time
import os.path
# noinspection PyUnresolvedReferences
from apiclient.discovery import build
from httplib2 import Http
from oauth2client.service_account import ServiceAccountCredentials
from csv import DictWriter, DictReader
import json
from selenium.webdriver import Chrome, ChromeOptions
from selenium.common.exceptions import WebDriverException, NoSuchElementException
from selenium.webdriver.common.by import By

from constants import API_ROWS_LIMIT, TEMP_FIELDNAMES


def get_domain(url: str) -> str:
    return '//'.join(url.split('/')[0:3:2])


def get_console(creds: dict):
    scope = 'https://www.googleapis.com/auth/webmasters.readonly'
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds, scopes=[scope])
    http = credentials.authorize(Http())
    return build('searchconsole', 'v1', http=http)


def refactor_date(dt: str):
    if len(dt) != 8:
        return
    return '-'.join([dt[:4], dt[4:6], dt[6:8]])


def retrieve_dates():
    st_date, end_date = input('Введите начальную и конечную даты в формате ггггммдд через пробел: ').split()
    if (len(st_date) != 8 or len(end_date) != 8
            or any(not s0.isdigit() for s in (st_date, end_date) for s0 in s)):
        raise Exception('Неверный формат даты')
    return refactor_date(st_date), refactor_date(end_date)


def retrieve_headers(headers_filename: str):
    with open(headers_filename, encoding='utf-8') as f:
        return f.readline().strip()


def retrieve_json(filename: str):
    with open(filename, encoding='utf-8') as f:
        return json.loads(f.read())


def write_headers(filename: str, fieldnames: list, delimiter: str = ';'):
    with open(filename, 'w', newline='', encoding='utf-8') as csv_file:
        writer = DictWriter(csv_file, fieldnames, delimiter=delimiter)
        writer.writeheader()


def write_row(row: dict, filename: str, fieldnames: list, delimiter: str = ';'):
    with open(filename, 'a', newline='', encoding='utf-8') as csv_file:
        writer = DictWriter(csv_file, fieldnames, delimiter=delimiter)
        writer.writerow(row)


def _execute_request(service, url: str, request: dict):
    return service.searchanalytics().query(siteUrl=url, body=request).execute()


def load_data_from_file(filename: str):
    with open(filename, encoding='utf-8') as f:
        return list(DictReader(f, TEMP_FIELDNAMES, delimiter=';'))[1:]


def save_data(filename: str, data: list, extra_fields: list = None):
    extra_fields = extra_fields if extra_fields and isinstance(extra_fields, list) else []
    fieldnames = TEMP_FIELDNAMES + extra_fields
    write_headers(filename, fieldnames)
    for row in data:
        write_row(row, filename, fieldnames)


def get_url_queries(url: str, st_date: str, end_date: str, creds: dict, folder: str,
                    extra_fields: list = None):
    extra_fields = extra_fields if extra_fields and isinstance(extra_fields, list) else []
    domain = get_domain(url)
    path = os.path.join(folder, f"{'_'.join(domain.split('://'))}.csv")
    if os.path.exists(path):
        return [row for row in load_data_from_file(path) if row['page'].rstrip('/') == url.rstrip('/')]
    service = get_console(creds)
    headers = ['page', 'query'] + extra_fields
    start_row = 0
    raw_output, current_rows = [], []
    while len(current_rows) == API_ROWS_LIMIT or start_row == 0:
        request = {'startDate': st_date,
                   'endDate': end_date,
                   'dimensions': headers,
                   'rowLimit': API_ROWS_LIMIT,
                   'startRow': start_row}
        current_rows = _execute_request(service, domain, request).get('rows', [])
        current_rows = [row for row in current_rows]
        raw_output.extend(current_rows)
        start_row += API_ROWS_LIMIT
    output = []
    for row in raw_output:
        keys = row.pop('keys')
        data = {headers[i]: keys[i] for i in range(len(keys))}
        output.append({**data, **row})
    if not os.path.exists(path):
        save_data(path, output, extra_fields)
    return [row for row in output if row['page'].rstrip('/') == url.rstrip('/')]


def get_driver(headers: str):
    options = ChromeOptions()
    options.add_argument(f'user-agent={headers}')
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    return Chrome(options=options)


def get_text(url: str, headers: str, attempts_limit: int = 3):
    attempts = 0
    while attempts < attempts_limit:
        attempts += 1
        driver = get_driver(headers)
        try:
            driver.get(url)
        except WebDriverException:
            continue
        time.sleep(2)
        try:
            return driver.page_source.lower(), driver.find_element(By.TAG_NAME, 'html').text.lower()
        except NoSuchElementException:
            return driver.page_source.lower(), ''
    raise Exception(f'Failed to parse text from {url}')


def process_url(url: str, st_date: str, end_date: str, creds: dict, folder: str,
                headers: str = '', extra_fields: list = None):
    extra_fields = extra_fields if extra_fields and isinstance(extra_fields, list) else []
    data = get_url_queries(url, st_date, end_date, creds, folder, extra_fields=extra_fields)
    output = []
    if not data:
        return output
    html, text = get_text(url, headers)
    for d in data:
        row = {'query': d['query'], 'freq_html': html.count(d['query'].lower()),
               'freq_text': text.count(d['query'].lower()),
               'impressions': d['impressions'],
               'clicks': d['clicks'], 'position': float(d['position'])}
        for i, field in enumerate(extra_fields):
            try:
                row[field] = d[field]
            except KeyError:
                row[field] = d[None][i]
        output.append(row)
    return output