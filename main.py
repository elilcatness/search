import os
from shutil import rmtree

from utils import retrieve_dates, retrieve_headers, retrieve_json, write_headers, process_url, write_row
from constants import FIELDNAMES


def main(input_filename: str, headers_filename: str = 'headers.txt',
         creds_filename: str = 'creds.json', output_filename: str = 'output.csv',
         temp_folder: str = 'temp'):
    if os.path.exists(output_filename) and input(f'Файл {output_filename} будет перезаписан. '
                                                 f'Вы готовы продолжить? (y\\n) ').lower() != 'y':
        return
    if os.path.exists(temp_folder):
        if input(f'Папка {temp_folder} будет перезаписана. Вы готовы продолжить? (y\\n) ').lower() != 'y':
            return
        rmtree(temp_folder)
    os.mkdir(temp_folder)
    st_date, end_date = retrieve_dates()
    headers = retrieve_headers(headers_filename)
    creds = retrieve_json(creds_filename)
    with open(input_filename, encoding='utf-8') as f:
        urls = [x.strip() for x in f.readlines()]
        count = len(urls)
    write_headers(output_filename, FIELDNAMES)
    for i in range(count):
        for key, val in process_url(urls[i], st_date, end_date, creds, temp_folder, headers=headers).items():
            write_row({'URL': urls[i], 'Query': key, 'Frequency_HTML': val['freq_html'],
                       'Frequency_TEXT': val['freq_text'], 'Impressions': val['impressions'],
                       'Clicks': val['clicks'], 'Position': val['position']}, output_filename, FIELDNAMES)
        print(f'[{i + 1}/{count}] {urls[i]}')


if __name__ == '__main__':
    main('urls.txt', output_filename='output2.csv')