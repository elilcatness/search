import csv


def get_reader(filename, delimiter=';'):
    file = open(filename, newline='', encoding='utf-8')
    return csv.reader(file, delimiter=delimiter), file


def get_headers(filename, delimiter=';'):
    with open(filename, encoding='utf-8') as f:
        return f.readline().strip().split(delimiter)


def unite_files(output_filename, headers_filename, filenames, delimiter=';', no_headers=False):
    if not no_headers:
        skip = not bool(headers_filename)
        headers = get_headers(headers_filename if headers_filename else filenames[0], delimiter)
    else:
        skip, headers = False, []
    with open(output_filename, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file, delimiter=delimiter)
        if headers:
            writer.writerow(headers)
        for filename in filenames:
            reader, main_file = get_reader(filename, delimiter)
            file_headers = []
            for i, row in enumerate(reader):
                if not no_headers and (skip or filename == headers_filename):
                    if i == 0:
                        file_headers = row
                        continue
                    data = {key: val for key, val in zip(file_headers, row) if key in headers}
                    row = [data.get(key) for key in headers]
                row = row if len(row) <= len(headers) or no_headers else row[:len(headers)]
                writer.writerow(row)
            print(f'Записан {filename} в {output_filename}')
            if main_file:
                main_file.close()