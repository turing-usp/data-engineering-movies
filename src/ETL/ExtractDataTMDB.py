import json

from Extract import Extract


class ExtractDataTMDB(Extract):


	def _get_media(self, initial_page = None, final_page = None, *args, **kwargs):
			if not initial_page:
				initial_page = 0
			if not final_page or final_page < initial_page:
				final_page = initial_page

			params = kwargs.get('params') or {}
			self._response = []

			for page in range(initial_page, final_page + 1):
				params['page'] = page
				res = self._make_request(self._endpoint, params)
				if res.ok:
					responseData = json.loads(res.text)
					if not responseData['results']:
						break
					self._response.extend(responseData['results'])
				else:
					raise Exception(f'Error on request [page {page}] - status {res.status_code}')
			

	def get_route(self, route, initial_page = 1, final_page = 1, route_name='', params = {}):
		self._endpoint = route.replace(':t', self._type)
		self._get_and_customize_genres()
		self._get_media(initial_page, final_page, params=params)
		self._get_details()
		self._create_dataframe()
		return self._dataframe


if __name__ == '__main__':
	import os

	fields = [
		'title', 'original_title', 'overview', 'poster_path', 
		'popularity', 'release_date', 'vote_average', 
		'vote_count', 'imdb_id', 'status', 'revenue', 'budget', 'release_year', 
		{
			'fieldName': 'genres', 'finalName': 'genre_ids', 
			'func': lambda x: [g['id'] for g in x]
		}
	]
	extract = ExtractDataTMDB(details=fields, media_type='movie', show_credits=True)
	df = extract.get_route('/trending/:t/week', initial_page=1, final_page=4, route_name='trending_movie')
	df.to_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tmp', 'trending_week.csv'))