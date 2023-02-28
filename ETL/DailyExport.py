import pandas as pd
import os.path
import requests
import gzip
import shutil

class DailyExport:
    
    def __init__(self, filename, file_url, final_directory):
        self.filename = filename
        self.file_url = file_url
        self.final_directory = final_directory
        self.final_filename = os.path.join(self.final_directory, self.filename)
        self.compressed_name = os.path.join(self.final_directory, 'compressed.gz')

    def break_file(self, lines_per_file = 38000):
        for idx, df in enumerate(pd.read_json(self.final_filename, chunksize=lines_per_file, orient='records', lines=True)):
            name, ext = os.path.splitext(self.filename)
            part_filename = os.path.join(self.final_directory, f"{name}_parte{idx:02}{ext}")
            df.to_json(part_filename, orient='records', lines=True)

    def download_file(self):
        os.makedirs(self.final_directory, exist_ok=True)
        res = requests.get(self.file_url, stream=True)
        with open(self.compressed_name, 'wb') as movies_file:
            movies_file.write(res.content)

    def extract_file(self):
        with gzip.open(self.compressed_name) as extracted:
            with open(self.final_filename, 'wb') as final_file:
                shutil.copyfileobj(extracted, final_file)

    def cleanup(self):
        os.remove(self.final_filename)
        os.remove(self.compressed_name)        

    def main(self):
        self.download_file()
        self.extract_file()
        self.break_file()
        self.cleanup()
