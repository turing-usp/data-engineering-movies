import requests
import pandas as pd
import json

from settings import BASE_URL
from Extract import Extract


class ExtractDataTMDB(Extract):


	def _make_request(self, endpoint, params = {}):
		if params:
			self._default_params.update(params)
		return requests.get(BASE_URL + endpoint, params=self._default_params)


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


	def get_route(self, route, initial_page = 1, final_page = 1):
		self._endpoint = route.replace(':t', self._type)
		self._get_media(initial_page, final_page)
		self._get_details()
		self._serialize_genres()
		self._create_dataframe()
		return self._dataframe
