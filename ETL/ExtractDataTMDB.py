import requests
import pandas as pd

from settings import BASE_URL
from Extract import Extract


class ExtractDataTMDB(Extract):


	def _make_request(self, endpoint = '', params = {}):
		if params:
			self._default_params.update(params)
		return requests.get(BASE_URL + endpoint, params=self._default_params)


	def get_route(self, route, initial_page = 1, final_page = 1, route_name=''):
		self._endpoint = route.replace(':t', self._type)
		self._get_media(initial_page, final_page)
		self._get_details()
		self._serialize_genres()
		self._create_dataframe()
		self._dataframe['type'] = route_name
		return self._dataframe
