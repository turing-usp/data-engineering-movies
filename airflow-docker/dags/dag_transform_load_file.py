from airflow.decorators import task, dag
from airflow.models import Variable
from datetime import datetime
from datetime import timedelta
import os

from tmdb_api import get_detail, create_dataframe, get_and_customize_genres
from src.ETL.extract_exports import get_media
from src.ETL.upload import upload_to_bq_from_dataframe

@dag(
    "transform_and_load_file", # Nome da dag
    schedule_interval = "00 15 * * 2", # Agendamento da execução (CRON: https://crontab.guru/)
    start_date=datetime(2023, 3, 13), 
    catchup=True,
    max_active_runs=5
)
def generate_dag():
    
  @task
  def get_and_addapt_genres(**kwargs):
    genres_map = get_and_customize_genres()
    return genres_map

  @task
  def get_media_values(doc, **kwargs):
    files_dir = Variable.get('FILES_DIR')
    
    date = datetime.strptime(kwargs['ds'], r'%Y-%m-%d')
    export_date = date - timedelta(days=date.weekday())
    month = export_date.strftime(r'%m')
    day = export_date.strftime(r'%d')
    year = export_date.strftime(r'%Y')

    responses, filename = get_media(files_dir, month, day, year, 10)
    doc['responses'] = responses
    doc['filename'] = filename

    return doc

  @task
  def get_responses(doc):
    return doc['responses']
  

  @task(max_active_tis_per_dag=100)
  def get_media_details(media, genres_map):
    fields = [
      'title', 'original_title', 'overview', 'poster_path',
      'popularity', 'release_date', 'vote_average', 
      'vote_count', 'imdb_id', 'status', 'revenue', 'budget', 'release_year', 
      {
        'fieldName': 'genres', 'finalName': 'genre_ids', 
        'func': lambda x: [g['id'] for g in x]
		  }
    ]
    get_detail(media, 'movie', fields, genres_map)
    return media

  @task
  def upload_to_bigquery(detailed):
    print(detailed)
    dataframe = create_dataframe(detailed)
    upload_to_bq_from_dataframe('turingdb', 'data_warehouse', 'movies', dataframe)

  @task 
  def handle_trash(doc):
    if (doc['delete_file_when_processed']):
      os.remove(os.path.join(Variable.get('FILES_DIR'), doc['filename']))

  genres_map = get_and_addapt_genres()
  
  doc = {'delete_file_when_processed': False}
  doc = get_media_values(doc)
  media = get_responses(doc)
  handle_trash(doc)
  
  detailed_media = get_media_details.partial(genres_map=genres_map).expand(media=media)
  upload_to_bigquery(detailed_media)

  print(detailed_media)


generate_dag()