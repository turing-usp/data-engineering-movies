import os
import json
from datetime import datetime
import re

from .extract import get_and_customize_genres, get_detailed_response_list, create_dataframe

class EmptyFileFolderError(Exception):
  pass

def get_media(files_dir, month, day, year, line_limit = None, file_index = 0):
  response = []
  count = 0
  date_string = f'{month}_{day}_{year}'
  file_directory = os.path.join(files_dir, date_string)
  files = os.listdir(file_directory)
  try:
    file = os.path.join(file_directory, files[file_index])
    with open(file, 'r') as curr_file:
      for line in curr_file:
        response.append(json.loads(line))
        count += 1
        if line_limit and count == line_limit:
          break
  except IndexError:
    raise EmptyFileFolderError

  return response, file

def get_export_dataframe(files_dir, file_date, details, media_type, line_limit = None, file_index = 0):
  file_month = datetime.strftime(file_date, r'%m')
  file_day = datetime.strftime(file_date, r'%d')
  file_year = datetime.strftime(file_date, r'%Y')

  genres_map = get_and_customize_genres()
  response, _ = get_media(files_dir, file_month, file_day, file_year, line_limit, file_index)
  get_detailed_response_list(response, media_type, details, genres_map)
  dataframe = create_dataframe(response)
  return dataframe

if __name__ == '__main__':
  fields = [
		'title', 'original_title', 'overview', 'poster_path', 
		'popularity', 'release_date', 'vote_average', 
		'vote_count', 'imdb_id', 'status', 'revenue', 'budget', 'release_year', 
		{
			'fieldName': 'genres', 'finalName': 'genre_ids', 
			'key': 'id'
		}
	]
  files_dir = os.path.join('..', 'files')
  file_date = datetime.strptime('2023-03-13', r'%Y-%m-%d')
  df = get_export_dataframe(files_dir, file_date, fields, 'movie', 5)
  print(df.head())
  df.info()