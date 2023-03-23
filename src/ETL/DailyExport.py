import pandas as pd
import os
import sys
import requests
import gzip
import shutil
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
from src.ETL.DeserializeMixin import DeserializeMixin

class DailyExport(DeserializeMixin):
    
    def __init__(self, file_base_url = '', final_base_directory = ''):
        self.file_base_url = file_base_url
        self.final_base_directory = final_base_directory
        self.final_directory = ''
        self.final_filename = ''
        self.filename = ''
        self.compressed_name = ''

    def break_file(self, lines_per_file = 38000):
        for idx, df in enumerate(pd.read_json(self.final_filename, chunksize=lines_per_file, orient='records', lines=True)):
            name, ext = os.path.splitext(self.filename)
            part_filename = os.path.join(self.final_directory, f"{name}_parte{idx:02}{ext}")
            df.to_json(part_filename, orient='records', lines=True)

    def download_file(self, filename, directory):
        self.final_directory = os.path.join(self.final_base_directory, directory)
        self.compressed_name = os.path.join(self.final_directory, 'compressed.gz')
        self.filename = filename
        file_url = f'{self.file_base_url}/{filename}.gz'
        
        os.makedirs(self.final_directory, exist_ok=True)
        res = requests.get(file_url, stream=True)
        with open(self.compressed_name, 'wb') as movies_file:
            movies_file.write(res.content)

    def extract_file(self):
        self.final_filename = os.path.join(self.final_directory, self.filename)
        with gzip.open(self.compressed_name) as extracted:
            with open(self.final_filename, 'wb') as final_file:
                shutil.copyfileobj(extracted, final_file)

    def cleanup(self):
        try:
            os.remove(self.final_filename)
            os.remove(self.compressed_name)        
        except OSError:
            pass

    def main(self, filename, directory):
        self.download_file(filename, directory)
        self.extract_file()
        self.break_file()
        self.cleanup()


if __name__ == '__main__':
    import os.path
    from datetime import datetime

    curr_month = datetime.strftime(datetime.strptime('2023-03-9', r'%Y-%m-%d'), r'%m')
    curr_day = datetime.strftime(datetime.strptime('2023-03-9', r'%Y-%m-%d'), r'%d')
    curr_year = datetime.strftime(datetime.strptime('2023-03-9', r'%Y-%m-%d'), r'%Y')

    filename = f'movie_ids_{curr_month}_{curr_day}_{curr_year}.json'
    directory = f'{curr_month}_{curr_day}_{curr_year}'
    final_base_directory = os.path.join('..', '..', 'files')

    export = DailyExport('http://files.tmdb.org/p/exports', final_base_directory)

    export.main(filename, directory)