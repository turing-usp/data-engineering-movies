from airflow.decorators import task, dag
from airflow.models import Variable
import os
from datetime import datetime

from src.ETL.daily_export import download_file, extract_file, break_file, cleanup 

@dag(
    "extract_file", # Nome da dag
    schedule_interval = "30 11 * * 1", # Agendamento da execução (CRON: https://crontab.guru/)
    start_date=datetime(2023, 3, 12), 
    catchup=True,
    max_active_runs=5
)
def generate_dag():

  @task
  def download(**context):
    date = datetime.strptime(context['ds'], r'%Y-%m-%d')
    year = datetime.strftime(date, r'%Y')
    day = datetime.strftime(date, r'%d')
    month = datetime.strftime(date, r'%m')
    string_date = f'{month}_{day}_{year}'
    
    filename = f'movie_ids_{string_date}.json'
    base_directory = os.path.join(Variable.get('FILES_DIR'), string_date)
    file_url = f'http://files.tmdb.org/p/exports/{filename}.gz'

    download_file(file_url, base_directory)

    return {'filename': filename, 'base_directory': base_directory}

  @task
  def extract(doc):
    extract_file(doc.get('base_directory'), doc.get('filename'))
    return doc

  @task
  def break_in_pieces(doc):
    break_file(doc.get('base_directory'), doc.get('filename'))
    return doc
  
  @task
  def cleanup_trash(doc):
    cleanup(doc.get('base_directory'), doc.get('filename'))
    return doc

  cleanup_trash(break_in_pieces(extract(download())))

generate_dag() # Instancia DAG
