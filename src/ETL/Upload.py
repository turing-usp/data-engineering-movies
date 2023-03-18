from google.cloud import bigquery
from pandas import DataFrame

class Upload:
    def __init__(self, dataframe: DataFrame) -> None:
        self.dataframe = dataframe

    def upload(self):
        client = bigquery.Client()

        table_id = "turingdb.data_warehouse.movies"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(
            self.dataframe,
            table_id,
            job_config=job_config
        ) 
        job.result()  
        table = client.get_table(table_id)
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )

if __name__ == '__main__':
    import os
    from ExtractDataTMDB import ExtractDataTMDB

    fields = [
		'title', 'original_title', 'overview', 'poster_path', 
		'popularity', 'release_date', 'vote_average', 
		'vote_count', 'imdb_id', 'status', 'revenue', 'budget', 'release_year', 
		{
			'fieldName': 'genres', 'finalName': 'genre_ids', 
			'func': lambda x: [g['id'] for g in x]
		}
	]

    os.environ['PROJECT_ID'] = 'turingdb'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', '..', 'turingdb-f911c3126edd.json')

    extract = ExtractDataTMDB(details=fields, media_type='movie', show_credits=True)
    df = extract.get_route('/trending/:t/week', initial_page=1, final_page=4, route_name='trending_movie')
    bq_upload = Upload(df)

    bq_upload.upload()