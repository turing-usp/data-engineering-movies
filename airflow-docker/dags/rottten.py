from airflow.decorators import task, dag, task_group
from airflow.models import Variable
from airflow import DAG
from datetime import datetime
from src.ETL.extract_data import get_route, get_discover_movie_pages, pega_resultados_de_intervalo_de_paginas
from src.ETL.utils import generate_date_intervals, gera_intervalos
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import requests
import re
from datetime import timezone, timedelta
import numpy as np

from google.cloud import bigquery

from google.oauth2 import service_account
from airflow.models import Variable

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
credentials = service_account.Credentials.from_service_account_file(
    filename = AIRFLOW_HOME + '/dags/data/credentials.json',
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
)
# AIRFLOW_HOME + '/dags/data/turingdb-f911c3126edd.json'

# FUNCTIONS

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
    try:
      rotten_data = scrape_rotten(url)
    except:
      continue
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
  """recebe um dataframe com os filmes"""
  keys = ['tmdb_id', 'original_title', 'Box Office (Gross USA)', 
          'Release Date (Theaters)', 'Release Date (Streaming)', 'tomattometer_score', 
          'audience_score', 'tomatometer_reviews', 'audience_reviews', 
          'Distributor', 'Aspect Ratio', 'Runtime', 
          'Writer', 'Original Language', 'Rating', 
          'Director', 'Genre', 'Producer', 
          'Sound Mix', 'tomattometer_reviews', 'Aspect Ratio']
  data = get_rotten_data(movies_dataframe['original_title'], movies_dataframe['id'], keys)
  df_out = pd.DataFrame.from_dict(data).set_index('tmdb_id')
  # df_out = df_out.astype(str).reset_index()
  df_out = df_out.reset_index().astype(str)
  # rename the columns to match the table schema
  df_out.columns = [
        'tmdb_id', 'original_title', 'Box Office',
        'Release Date Theaters', 'Release Date Streaming', 'tomattometer_score', 
        'audience_score', 'tomatometer_reviews', 'audience_reviews', 
        'Distributor', 'Aspect Ratio', 'Runtime', 
        'Writer', 'Original Language', 'Rating', 
        'Director', 'Genre', 'Producer',
        'Sound Mix', 'tomattometer_reviews', 'Production Co'
  ]
  # parse nan values as strings

  return df_out

def get_bigquery_tmdb_df(begin_date, end_date):
    """this function gets the tmdb_id and original_title 
    from the discover table in bigquery"""

    # Construct a BigQuery client object.
    client = bigquery.Client(credentials=credentials)

    query = """
        SELECT id, original_title FROM `turingdb.data_warehouse.discover` 
        where release_date between '{}' and '{}';
    """.format(begin_date, end_date)
    query_job = client.query(query)  # Make an API request.

    print("The query data:")

    tmdb_id = []
    original_titles = []
    for row in query_job:
        # Row values can be accessed by field name or index.
        # store the data in the lists
        tmdb_id.append(row[0])
        original_titles.append(row[1])

    df = pd.DataFrame(list(zip(tmdb_id, original_titles)), columns =['id', 'original_title'])
    return df

# insert the data into the bigquery table using insert into
def insert_into_bigquery(df_rotten):
    """this function inserts the data into the bigquery table"""
    client = bigquery.Client(credentials=credentials)
    table_id = "turingdb.data_warehouse.rotten_tomatoes"

    job = client.load_table_from_dataframe(
        df_rotten, table_id,
        # , job_config=job_config    ### Uncomment to create the table for the first time
    )  # Make an API request.

    job.result()  # Wait for the job to complete.

# FUNCTIONS END
default_args = {
    'owner': 'hugo',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_rotten_v2.20',
    default_args=default_args,  # Nome da dag
    schedule="30 11 * * *",
    start_date=datetime(2022, 12, 4, 10, 0),
    catchup=False,
    max_active_tasks=3,

) as dag:
    """
    DAG que ingere dados da API do TMDB, carregando-os no BigQuery

    Define uma query com filtros e paraleliza a ingestão das páginas
    """
    @task
    def setup():
        # Hard-coded. Se for maior que isso tem o perigo de ter mais que 500
        # páginas na query
        INTERVAL_SIZE_IN_MONTHS = 3
        intervals = generate_date_intervals('2023-01-01', '2023-04-01', 1)
        return intervals

    @task_group
    def etl(interval: tuple):

        @task()
        def get_df(interval):
            begin_date = interval[0]
            end_date = interval[1]
            # reading the csv file
            df_in = get_bigquery_tmdb_df(begin_date, end_date)
            # serializing the dataframe and store it in a python variable
            return df_in.to_json(orient='split')
        
        @task()
        def rotten(df_in):
            # deserializing the dataframe
            df_in = pd.read_json(df_in, orient='split')
            # calling the function to get the rotten dataframe
            df_in = get_rotten_dataframe(df_in)
            #serializing the rotten dataframe again
            rotten_df = df_in.to_json(orient='split')
            return rotten_df
        
        @task()
        def insert_into(rotten_df):
            # deserializing the rotten dataframe
            rotten_df = pd.read_json(rotten_df, orient='split').astype(str)
            # inserting the rotten dataframe into bigquery
            insert_into_bigquery(rotten_df)

        insert_into(rotten(get_df(interval)))

    intervals = setup()

    etl.expand(interval=intervals)


if __name__ == "__main__":
    dag.test(
        execution_date=datetime(2023, 4, 9, 12, 0, 0, tzinfo=timezone.utc)
    )