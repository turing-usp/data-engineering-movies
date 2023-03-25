from google.cloud import bigquery

def upload_to_bq_from_dataframe(project, database, tablename, dataframe):
  client = bigquery.Client()

  table_id = f"{project}.{database}.{tablename}"
  job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
  job = client.load_table_from_dataframe(
      dataframe,
      table_id,
      job_config=job_config
  ) 
  job.result()
  table = client.get_table(table_id)
  print(f'Uploaded {dataframe.shape[0]} rows (total of {table.num_rows} rows on table)')

if __name__ == '__main__':
  from .extract_exports import get_export_dataframe
  import os
  from datetime import datetime

  os.environ['PROJECT_ID'] = 'turingdb'
  os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'credentials', 'turingdb-f911c3126edd.json')

  fields = [
		'title', 'original_title', 'overview', 'poster_path', 
		'popularity', 'release_date', 'vote_average', 
		'vote_count', 'imdb_id', 'status', 'revenue', 'budget', 'release_year', 
		{
			'fieldName': 'genres', 'finalName': 'genre_ids', 
			'func': lambda x: [g['id'] for g in x]
		}
	]
  files_dir = os.path.join('..', 'files')
  file_date = datetime.strptime('03-13-2023', r'%m-%d-%Y')

  df = get_export_dataframe(files_dir, file_date, fields, 'movie', 10, 2)
  upload_to_bq_from_dataframe('turingdb', 'data_warehouse', 'movies', df)