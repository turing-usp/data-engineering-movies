import requests
import pandas as pd
import json
import os
import sys

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
from src.ETL.settings import API_KEY, BASE_URL


class Extract:


	def __init__(self, details = [], media_type = 'movie', show_credits=False):
		self._response = []
		self._dataframe = None
		self._type = media_type
		self._details = details
		self._default_params = {'api_key': API_KEY, 'language': 'pt-BR'}
		self._genres_map = {}
		self._endpoint = ''
		self._credits = show_credits

	
	def _make_request(self, endpoint = '', params = {}):
		if params:
			self._default_params.update(params)
		return requests.get(BASE_URL + endpoint, params=self._default_params)
	

	@property
	def df(self):
		return self._dataframe


	def _get_and_customize_genres(self):
		endpoint_genres_tv = '/genre/tv/list'
		endpoint_genres_movie = '/genre/movie/list'

		genres_tv_res = self._make_request(endpoint_genres_tv)
		genres_tv = json.loads(genres_tv_res.text)['genres']
		genres_movie_res = self._make_request(endpoint_genres_movie)
		genres_movie = json.loads(genres_movie_res.text)['genres']

		unique_genres = list(set([tuple(d.items()) for d in genres_movie + genres_tv]))
		unique_genres = [dict(genre) for genre in unique_genres]

		for genre in unique_genres:
			self._genres_map[genre['id']] = genre['name']

		# Customizando gêneros
		self._genres_map[10766] = 'Novela'
		self._genres_map[10765] = ['Ficção científica', 'Fantasia']
		self._genres_map[10762] = 'Infantil'
		self._genres_map[10768] = ['Guerra', 'Política']
		self._genres_map[10759] = ['Ação', 'Aventura']

		self._genres_map = {str(key):value for key, value in self._genres_map.items()}

	def _get_details(self):
		i = 1
		for media in self._response:
			print(f'Getting details for {media["original_title"]} ({i}/{len(self._response)})')
			media['type'] = self._type
			endpoint = f'/{self._type}/{media["id"]}'
			res_details = self._make_request(endpoint)
			details_data = {}
			values = {}
			i += 1

			if res_details.ok:
				try:
					details_data = json.loads(res_details.text)
				except:
					details_data = {}

				for field in self._details:
					try:
						if type(field) != dict:
							values[field] = details_data[field]
						else:
							newFieldName = field['finalName'] # nome que ficará para o campo final
							serialize = field['func'] # função que irá tratar o dado retornado pela API para aquele campo
							fieldName = field['fieldName'] # nome do campo dentro do retorno da API
							values[newFieldName] = serialize(details_data[fieldName])
					except (KeyError):
						fieldName = field['finalName'] if type(field) == dict else field
						values[fieldName] = None

				if self._credits:
					url = f'/movie/{media["id"]}/credits'
					res_credits = self._make_request(url)
					if res_credits.ok:
						credits_data = json.loads(res_credits.content)
						values['cast'] = list(map(lambda item: item['original_name'], credits_data['cast']))
						director_found = next(filter(lambda item: item['job'] == 'Director', credits_data['crew']), None)
						values['director'] = director_found and director_found['original_name']
					else:
						raise Exception(f'Error on request for credits detail [media {media["id"]}]')
				
				media.update(values)
				self._serialize_genre(media)
			else:
				raise Exception(f'Error on request for detail [media {media["id"]}]')


	def _get_media(self, initial_page = None, final_page = None, *args, **kwargs):
		...


	def _serialize_genre(self, media):
		media['genres'] = []
		for genre_id in media['genre_ids'] or []:
			if type(self._genres_map[str(genre_id)]) == list:
				for g in self._genres_map[str(genre_id)]:
					media['genres'].append(g)
			else:
				media['genres'].append(self._genres_map[str(genre_id)])
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

