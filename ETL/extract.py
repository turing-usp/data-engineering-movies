import requests
import pandas as pd
import json

from settings import API_KEY, BASE_URL


class Extract:


	def __init__(self, details = [], media_type = 'movie'):
		self._response = []
		self._dataframe = pd.DataFrame()
		self._type = media_type
		self._details = details
		self._default_params = {'api_key': API_KEY, 'language': 'pt-BR'}
		self._genres_map = {}
		self._endpoint = ''

		self._get_and_customize_genres()
	
	
	@property
	def df(self):
		return self._dataframe


	def _make_request(self, endpoint, params = {}):
		...


	def _get_and_customize_genres(self):
		...


	def _get_details(self):
		for media in self._response:
			endpoint = f'/{self._type}/{media["id"]}'
			res = self._make_request(endpoint)
			data = {}
			values = {}

			if res.ok:
				try:
					data = json.loads(res.text)
				except:
					data = {}

				for field in self._details:
					try:
						if type(field) != dict:
							values[field] = data[field]
						else:
							newFieldName = field['finalName'] # nome que ficará para o campo final
							serialize = field['func'] # função que irá tratar o dado retornado pela API para aquele campo
							fieldName = field['fieldName'] # nome do campo dentro do retorno da API
							values[newFieldName] = serialize(data[fieldName])
					except (KeyError):
						fieldName = field['finalName'] if type(field) == dict else field
						values[fieldName] = None
				media.update(values)
			else:
				raise Exception(f'Error on request for detail [media {media["id"]}]')


	def _get_media(self, initial_page = None, final_page = None, params = {}):
		if not initial_page:
			initial_page = 0
		if not final_page or final_page < initial_page:
			final_page = initial_page

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


	def _serialize_genres(self):
		for media in self._response:
			media['genres'] = []
			for genre_id in media['genre_ids']:
				if type(self._genres_map[genre_id]) == list:
					for g in self._genres_map[genre_id]:
						media['genres'].append(g)
				else:
					media['genres'].append(self._genres_map[genre_id])
			media.pop('genre_ids')


	def _create_dataframe(self):
		final_df = pd.DataFrame.from_dict(self._response)
		final_df.set_index('id', inplace=True)
		
		# Padroniza os nomes das features para filmes e séries
		final_df.rename(columns={
			'name': 'title',
			'original_name': 'original_title',
			'first_air_date': 'release_date'
		}, inplace=True, errors='ignore')
		
		final_df.drop(['adult', 'backdrop_path', 'original_language', 'video'], inplace=True, axis=1, errors='ignore')
		
		final_df['poster_path'] = 'https://image.tmdb.org/t/p/w220_and_h330_face' + final_df['poster_path']
		
		final_df['release_date'] = pd.to_datetime(final_df['release_date'])
		final_df['release_year'] = final_df['release_date'].dt.year
		
		self._dataframe = final_df

