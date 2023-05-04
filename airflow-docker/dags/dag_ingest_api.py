from airflow.decorators import task, dag, task_group
from airflow.models import Variable
from airflow import DAG
from google.oauth2 import service_account
from datetime import timezone
from datetime import datetime
from src.ETL.extract_data import get_route, get_discover_movie_pages, pega_resultados_de_intervalo_de_paginas
from src.ETL.utils import generate_date_intervals, gera_intervalos
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    def setup():
        # Hard-coded. Se for maior que isso tem o perigo de ter mais que 500
        # páginas na query
        INTERVAL_SIZE_IN_MONTHS = 3

        return generate_date_intervals(Variable.get('initial_date'), Variable.get('final_date'), INTERVAL_SIZE_IN_MONTHS)

    @task_group
    def etl(interval: tuple):
        @task
        def get_total_pages(interval):
            params = {
                'primary_release_date.gte': interval[0],
                'primary_release_date.lte': interval[1],
            }
            return (get_discover_movie_pages(params), params)

        @task
        def consume_api(info):
            total_pages = info[0]
            params = info[1]
            page_intervals = gera_intervalos(1, total_pages + 1, 4)[:2]

            # Paraleliza requests à API
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(pega_resultados_de_intervalo_de_paginas, intervalo[0], intervalo[1], params) for intervalo in page_intervals]
                resultados = []
                for future in as_completed(futures):
                    # Obtém o resultado do objeto Future
                    df_resultado = future.result()

                    # Adiciona o resultado à lista de resultados
                    resultados.append(df_resultado)
                
                # Consolida Dataframe final (Junção dos dfs gerados pelos processos paralelos)
                final_df = pd.concat(resultados, ignore_index=True)

            return final_df
        
        @task
        def upload_to_bq(df):

            credentials = service_account.Credentials.from_service_account_file(
                filename=Variable.get('credentials_path'),
                scopes = ["https://www.googleapis.com/auth/cloud-platform"]
            )

            df_to_discover_table: pd.DataFrame = df.drop(['genre_ids', 'backdrop_path', 'overview', 'vote_average', 'vote_count'], axis=1)

            many_to_many: pd.DataFrame = df[['id', 'genre_ids']].explode('genre_ids').dropna(axis=0)

            df_to_discover_table.to_gbq(
                destination_table='turingdb.data_warehouse.discover',
                credentials=credentials,
                if_exists='append'
            )

            many_to_many.to_gbq(
                destination_table='turingdb.data_warehouse.discover_to_genre',
                credentials=credentials,
                if_exists='append'
            )
            return
        
        upload_to_bq(consume_api(get_total_pages(interval)))

    intervals = setup()

    etl.expand(interval=intervals)


if __name__ == "__main__":
    dag.test(
        execution_date=datetime(2023, 4, 9, 12, 0, 0, tzinfo=timezone.utc)
    )
