from datetime import datetime
from airflow.decorators import task, dag

@dag(
    "download_and_process_file", # Nome da dag
    schedule_interval = "30 11 * * 1", # Agendamento da execução (CRON: https://crontab.guru/)
    start_date=datetime(2023, 1, 1), 
    catchup=False,
)
def generate_dag():

    # Dicionário com informações
    doc = {}
    @task
    def download(doc : dict):
        return doc

    @task
    def extract(doc : dict):
        return doc

    @task
    def break_in_pieces(doc : dict):
        return doc
    
    @task
    def cleanup(doc : dict):
        return doc


    cleanup(break_in_pieces(extract(download(doc))))
    

generate_dag() # Instancia DAG
