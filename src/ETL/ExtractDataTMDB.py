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
		self._get_media(initial_page, final_page, params=params)
		self._get_details()
		self._serialize_genres()
		self._create_dataframe()
		self._dataframe['type'] = route_name
		return self._dataframe
