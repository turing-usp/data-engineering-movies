from airflow.decorators import task, dag
import os
from datetime import datetime
from datetime import datetime
from src.ETL.DailyExport import DailyExport

@dag(
    "download_and_process_file", # Nome da dag
    schedule_interval = "30 11 * * 1", # Agendamento da execução (CRON: https://crontab.guru/)
    start_date=datetime(2023, 3, 13), 
    catchup=True,
    max_active_runs=5
)
def generate_dag():

    @task
    def download(export_attrs, **kwargs):
        export = DailyExport()
        export.fromDict(export_attrs)
        date = datetime.strptime(kwargs['ds'], r'%Y-%m-%d')
        year = datetime.strftime(date, r'%Y')
        day = datetime.strftime(date, r'%d')
        month = datetime.strftime(date, r'%m')
        filename = f'movie_ids_{month}_{day}_{year}.json'
        directory = f'{day}_{month}_{year}'
        export.download_file(filename, directory)
        return export.__dict__

    @task
    def extract(export_attrs):
        export = DailyExport()
        export.fromDict(export_attrs)
        export.extract_file()
        return export.__dict__

    @task
    def break_in_pieces(export_attrs):
        export = DailyExport()
        export.fromDict(export_attrs)
        export.break_file()
        return export.__dict__
    
    @task
    def cleanup(export_attrs):
        export = DailyExport()
        export.fromDict(export_attrs)
        export.cleanup()

    file_base_url = 'http://files.tmdb.org/p/exports'
    final_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'files')
    export = DailyExport(file_base_url, final_directory)

    # executando as tasks
    cleanup(break_in_pieces(extract(download(export.__dict__))))
    

generate_dag() # Instancia DAG
