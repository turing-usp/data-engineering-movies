import pandas as pd
import os
import requests
import gzip
import shutil

def download_file(file_url, base_directory):
  compressed_name = os.path.join(base_directory, 'compressed.gz')

  os.makedirs(base_directory, exist_ok=True)
  res = requests.get(file_url, stream=True)
  with open(compressed_name, 'wb') as movies_file:
      movies_file.write(res.content)
  

def extract_file(base_directory, filename):
  compressed = os.path.join(base_directory, 'compressed.gz')
  full_file = os.path.join(base_directory, filename)
  with gzip.open(compressed) as extracted:
      with open(full_file, 'wb') as final_file:
          shutil.copyfileobj(extracted, final_file)


def break_file(base_directory, filename, lines_per_file = 38000):
  full_file = os.path.join(base_directory, filename)
  for idx, df in enumerate(pd.read_json(full_file, chunksize=lines_per_file, orient='records', lines=True)):
    name, ext = os.path.splitext(filename)
    part_filename = os.path.join(base_directory, f"{name}_parte{idx:02}{ext}")
    df.to_json(part_filename, orient='records', lines=True)


def cleanup(base_directory, filename):
  try:
      full_file = os.path.join(base_directory, filename)
      os.remove(full_file)
      os.remove(os.path.join(base_directory, 'compressed.gz'))       
  except OSError:
      pass
  
if __name__ == '__main__':
  from datetime import datetime
  
  date = datetime.strptime('03-13-2023', r'%m-%d-%Y')
  month = datetime.strftime(date, r'%m')
  day = datetime.strftime(date, r'%d')
  year = datetime.strftime(date, r'%Y')
  date_string = f'{month}_{day}_{year}'
  filename = f'movie_ids_{date_string}.json'
  url = f'http://files.tmdb.org/p/exports/{filename}.gz'
  base_dir = os.path.join('..', 'files', date_string)

  download_file(url, base_dir)
  extract_file(base_dir, filename)
  break_file(base_dir, filename)
  cleanup(base_dir, filename)