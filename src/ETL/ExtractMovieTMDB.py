import json
import os

from Extract import Extract


class ExtractExportsTMDB(Extract):


	def _get_media(self, files_dir, initial_page = None, final_page = None, *args, **kwargs):
		initial_page = 1 if not initial_page or initial_page <= 0 else initial_page
		final_page = initial_page if not final_page else final_page
		self._response = []
		try:
			file_month, file_day, file_year = kwargs['month'], kwargs['day'], kwargs['year']
			date_string = f'{file_month:02}_{file_day:02}_{file_year:02}'
			file_directory = os.path.join(files_dir, date_string)
			file_basename = os.path.join(file_directory, f'{self._type}_ids_{date_string}')
			filenames = [f'{file_basename}_parte{num:02}.json' for num in range(initial_page-1, final_page)]
			for file in filenames:
				with open(file, 'r') as curr_file:
					for line in curr_file:
						self._response.append(json.loads(line))
		except ValueError:
			raise Exception('Inform the date of the files on [month, day, year] arguments')


	def get_dataframe(self, files_dir, month, day, year, initial_page = 1, final_page = 1):
		self._get_media(files_dir, initial_page, final_page, month=month, day=day, year=year)
		self._get_details()
		self._serialize_genres()
		self._create_dataframe()
		self._dataframe['type'] = self._type
		return self._dataframe


if __name__ == '__main__':
	fields = [
		'title', 'original_title', 'overview', 'poster_path',
		'popularity', 'release_date', 'vote_average', 
		'vote_count', 'imdb_id', 'status', 'revenue', 'budget', 'release_year', 
		{
			'fieldName': 'genres', 'finalName': 'genre_ids', 
			'func': lambda x: [g['id'] for g in x]
		}
	]
	extract = ExtractExportsTMDB(fields, 'movie', show_credits=True)
	df = extract.get_dataframe(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'files'), month=3, day=18, year=2023)
	print(df.head())
	print(df.head()['cast'])
	df.info()
