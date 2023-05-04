import json
from typing import Literal
import logging
import pandas as pd
from .tmdb_api import make_request, get_and_customize_genres, get_detailed_response_list, create_dataframe

logging.getLogger().setLevel(logging.INFO)

def get_media(endpoint: str, initial_page: int, final_page: int = None, params: dict = {}):
  """
    - Faz a requisição para a rota passada em `endpoint` e retorna uma lista com todos
  os resultados que aparecem da página `initial_page`até a página `final_page`
  """
  response = []

  for page in range(initial_page, final_page + 1):
    params['page'] = page
    res = make_request(endpoint, params)
    if res.ok:
      responseData = json.loads(res.text)
      if not responseData['results']:
        break
      response.extend(responseData['results'])
    else:
      raise Exception(f'Error on request [page {page}] - status {res.status_code}')

  return response
    
def get_route(route: int, media_type: Literal['movie', 'tv'], details: list, initial_page: int = 1, 
              final_page: int = None, params: dict={}):
  """
    - Faz a requisição para obter os resultados da API na rota `route`
      - Onde iria aparecer "movie" ou "tv" deve ser passado o placeholder ":t", que será
    substituído pelo valor de `media_type`
    - Pega os detalhes desses resultados a partir do campo `details`
    - Mostra os resultados da página `initial_page` até a página `final_page`
    - Os parâmetros passados na requisição para a API devem estar em `params`
  """
  
  if not final_page or final_page < initial_page:
    final_page = initial_page

  endpoint = route.replace(':t', media_type)
  genres_map = get_and_customize_genres()
  response = get_media(endpoint, initial_page, final_page, params)
  get_detailed_response_list(response, media_type, details, genres_map)
  dataframe = create_dataframe(response)
  return dataframe


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
  df = get_route('/trending/:t/week', 'movie', fields, 1)
  print(df.head())

def get_discover_movie_pages(params):
    return json.loads(make_request('/discover/movie', params).content.decode())['total_pages']

def make_discover_request(page=1, params={}):
    params['page'] = page

    return json.loads(make_request('/discover/movie', params).content.decode())

def pega_resultados_de_intervalo_de_paginas(inicio: int, fim: int, params: dict) -> pd.DataFrame:
    results = []
    for page in range(inicio, fim):
        response = make_discover_request(page, params=params)
        if 'results' not in response:
            print('Deu ruim, não tem resultados')
            print(page)
            print(response)
            continue

        results.extend(response['results'])
    result_df = pd.DataFrame(results)
    return result_df