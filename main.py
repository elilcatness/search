import os
from time import time
from shutil import rmtree
from multiprocessing import Pool

from utils import (retrieve_dates, retrieve_headers, retrieve_json,
                   write_headers, process_url, write_row)
from unite import unite_files
from constants import FIELDNAMES


def task(data: dict):
    length = len(data['urls'])
    filename = os.path.join(data['output_folder'], f'{data["number"]}.csv')
    write_headers(filename, FIELDNAMES)
    for i, url in enumerate(data['urls']):
        for key, val in process_url(url, data['st_date'], data['end_date'], data['creds'],
                                    data['temp_folder'], headers=data['headers']).items():
            write_row({'URL': url, 'Query': key, 'Frequency_HTML': val['freq_html'],
                       'Frequency_TEXT': val['freq_text'], 'Impressions': val['impressions'],
                       'Clicks': val['clicks'], 'Position': val['position']},
                      filename, FIELDNAMES)
        print(f'[{i + 1}/{length}] {data["urls"][i]}')


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
    processes_count = int(input('Введите количество процессов: '))
    os.mkdir(temp_folder)
    st_date, end_date = retrieve_dates()
    headers = retrieve_headers(headers_filename)
    creds = retrieve_json(creds_filename)
    with open(input_filename, encoding='utf-8') as f:
        urls = [x.strip() for x in f.readlines()]
        count = len(urls)
    write_headers(output_filename, FIELDNAMES)
    pages_per_process = count // processes_count
    tasks = []
    output_folder = str(time()).replace('.', '')
    os.mkdir(output_folder)
    for i in range(processes_count):
        start = i * pages_per_process
        end = (((i + 1) * pages_per_process) + 1 if i != processes_count - 1
               or ((i + 1) * pages_per_process) + 1 >= count
               else (i * pages_per_process + (count - i * pages_per_process)) + 1)
        tasks.append({'number': i + 1, 'urls': urls[start:end - 1],
                      'st_date': st_date, 'end_date': end_date,
                      'headers': headers, 'creds': creds, 'output_filename': output_filename,
                      'output_folder': output_folder, 'temp_folder': temp_folder})
    pool = Pool(processes=processes_count)
    pool.map(task, tasks)
    filenames = [os.path.join(output_folder, f_name)
                 for f_name in sorted(os.listdir(output_folder), key=lambda x: int(x.split('.')[0]))]
    print()
    unite_files(output_filename, None, filenames)
    rmtree(output_folder)


if __name__ == '__main__':
    main('urls.txt')