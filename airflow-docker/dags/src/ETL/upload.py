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