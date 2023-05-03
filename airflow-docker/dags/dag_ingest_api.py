from airflow.decorators import task, dag
from airflow.models import Variable
from airflow import DAG

from datetime import timezone
from datetime import datetime
from src.ETL.extract_data import get_route 

with DAG(
    "extract_file", # Nome da dag
    schedule="30 11 * * *",
    start_date=datetime(2022, 12, 4, 10, 0),
    catchup=False,
    max_active_tasks=10,
) as dag:
    """
    DAG que ingere dados da API do TMDB, carregando-os no BigQuery

    Define uma query com filtros e paraleliza a ingestão das páginas
    """
    @task
    def extract(doc):
        params = {
            'primary_release_date.gte': '2023-01-01',
            'primary_release_date.lte': '2023-06-01',
        }

        x = get_route(
            route='/discover/:t', 
            media_type='movie', 
            details=[], 
            initial_page = 1, 
            final_page = 12, 
            params=params
        )

        return doc

    @task
    def transform(doc):
        return doc
    
    @task
    def load(doc):
        return doc

    load(transform(extract({})))


if __name__ == "__main__":
    dag.test(
        execution_date=datetime(2023, 4, 9, 12, 0, 0, tzinfo=timezone.utc)
    )
