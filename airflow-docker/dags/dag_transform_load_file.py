from airflow.decorators import task, dag
from airflow.models import Variable
from datetime import datetime
from datetime import timedelta
import pandas as pd

from src.ETL.ExtractExportsTMDB import ExtractExportsTMDB
from src.ETL.Upload import Upload

@dag(
    "transform_and_load_file", # Nome da dag
    schedule_interval = "00 15 * * 2", # Agendamento da execução (CRON: https://crontab.guru/)
    start_date=datetime(2023, 3, 13), 
    catchup=True,
    max_active_runs=5
)
def generate_dag():

  fields = [
        'title', 'original_title', 'overview', 'poster_path',
        'popularity', 'release_date', 'vote_average', 
        'vote_count', 'imdb_id', 'status', 'revenue', 'budget', 'release_year', 
        {
          'fieldName': 'genres', 'finalName': 'genre_ids', 
          'func': lambda x: [g['id'] for g in x]
        }
      ]
    
  @task
  def get_and_addapt_genres(instance):
      export = ExtractExportsTMDB(details=fields)
      export.fromDict(instance)
      export._get_and_customize_genres()
      export.__dict__.pop('_details')
      return export.__dict__

  @task
  def get_media(instance, **kwargs):
    export = ExtractExportsTMDB(details=fields)
    export.fromDict(instance)
    files_dir = Variable.get('FILES_DIR')
    date = datetime.strptime(kwargs['ds'], r'%Y-%m-%d')
    export_date = date - timedelta(days=date.weekday())
    month = datetime.strftime(export_date, r'%m')
    year = datetime.strftime(export_date, r'%Y')
    day = datetime.strftime(export_date, r'%d')
    export._get_media(files_dir, month=month, year=year, day=day)
    export.__dict__.pop('_details')
    return export.__dict__

  @task
  def get_details(instance):
    export = ExtractExportsTMDB(details=fields)
    export.fromDict(instance)
    export._get_details()
    export.__dict__.pop('_details')
    return export.__dict__

  @task
  def upload_to_bigquery(instance):
    export = ExtractExportsTMDB()
    export.fromDict(instance)
    export._create_dataframe()
    upload = Upload(export._dataframe)
    upload.upload()


  extract = ExtractExportsTMDB(fields, 'movie', show_credits=True)

  upload_to_bigquery(get_details(get_media(get_and_addapt_genres(extract.__dict__))))


generate_dag()