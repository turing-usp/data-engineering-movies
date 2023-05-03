from airflow.decorators import dag, task

from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import numpy as np

from datetime import timedelta, datetime

import os
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
# file_df = pd.read_csv(AIRFLOW_HOME + '/dags/data/ztrending_movies.csv')

def find_text(element):
  # Looking for text nested
  text = ''
  for tag in element.children:
    text += clean_string(str(tag.string))
  return text

def clean_string(text):
  # Removes spaces and replacing non-breaking spaces (\xa0)
  return re.sub(r'(?<!\w)\s(?!\w)|\n', '', text.replace(u'\xa0', ' '))

def scrape_rotten(url):
  res = requests.get(url)
  
  if res.ok:
    soup = BeautifulSoup(res.text, 'html.parser')

    score_board = soup.find('score-board')
    tomatometer = score_board['tomatometerscore']
    audience = score_board['audiencescore']
    tomatometer_reviews, audience_reviews = [int(re.sub(r'[^0-9]', '', tag.string) or 0) for tag in score_board.find_all('a')]
    
    movie_info_data = soup.find('section', {'id': 'movie-info'}).find_all('li')
    movie_info = {clean_string(str(tag.find('b').string[:-1])): find_text(tag.find('span')) for tag in movie_info_data}
    movie_info.update({
      'tomattometer_score': tomatometer, 
      'audience_score': audience, 
      'tomattometer_reviews': tomatometer_reviews, 
      'audience_reviews': audience_reviews
    })

    return movie_info
  
  return {}

def rottenize(title):
        reaplace_dict = {':': '', "'": '', ".":"", ",":"", "-":"_", " ":"_"}
        return title.translate(str.maketrans(reaplace_dict)).lower()

def get_rotten_data(movie_titles, ids, keys):
  data = []
  for tmdb_id, title in zip(ids, movie_titles):
    url = f'https://www.rottentomatoes.com/m/{rottenize(title)}'
    rotten_data = scrape_rotten(url)
    if rotten_data:
      final_dict = dict.fromkeys(keys)
      final_dict['tmdb_id'] = tmdb_id
      final_dict['original_title'] = title
      final_dict.update(rotten_data)
      # Tratando o Box Office
      if 'Box Office (Gross USA)' in final_dict.keys():
        K = 1000
        M = 1000000
        original_box_office = final_dict['Box Office (Gross USA)']
        if original_box_office:
          box_office = float(re.sub(r'[^0-9\.]', '', str(original_box_office)))
          final_dict['Box Office (Gross USA)'] = box_office * K if original_box_office[-1] == 'K' else box_office * M
        else:
          final_dict['Box Office (Gross USA)'] = np.nan

      data.append(final_dict)
  return data

def get_rotten_dataframe(movies_dataframe):
  keys = ['tmdb_id', 'original_title', 'Box Office (Gross USA)', 'Release Date (Theaters)', 'Release Date (Streaming)', 'tomattometer_score', 'audience_score', 'tomatometer_reviews', 'audience_reviews', 'Distributor', 'Aspect Ratio', 'Runtime', 'Writer', 'Original Language', 'Rating', 'Director', 'Genre', 'Producer', 'Sound Mix', 'Aspect Ratio']
  data = get_rotten_data(movies_dataframe['original_title'], movies_dataframe['id'], keys)
  df_out = pd.DataFrame.from_dict(data).set_index('tmdb_id')
  return df_out

default_args = {
    'owner': 'hugo',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

@dag(dag_id='dag_rotten_v1.2', 
    default_args=default_args, 
    schedule_interval='@daily', 
    start_date=datetime(2023, 4, 7)
)

def rotten_etl():

    @task()
    def get_df():
        """essa funcao pega o csv e serializa ele para ser usado no rotten"""
        # reading the csv file
        df_in = pd.read_csv(AIRFLOW_HOME + '/dags/data/ztrending_movies.csv')
        # serializing the dataframe and store it in a python variable
        return df_in.to_json(orient='split')
      
    @task()
    def rotten(df_in):
        """essa funcao pega o dataframe serializado e retorna ele com as informacoes do rotten"""
        # deserializing the dataframe
        df_in = pd.read_json(df_in, orient='split')
        # calling the function to get the rotten dataframe
        get_rotten_dataframe(df_in)
        #serializing the rotten dataframe again
        rotten_df = df_in.to_json(orient='split')
        return rotten_df

    df_in = get_df()
    rotten(df_in=df_in)

rotten_dag = rotten_etl()