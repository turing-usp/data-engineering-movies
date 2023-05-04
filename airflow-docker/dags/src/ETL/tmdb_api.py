import requests
import json
import pandas as pd
import logging
from typing import Literal, List, Union

from .settings import API_KEY, BASE_URL


def make_request(endpoint: str, params: dict = {}):
  """
    Faz uma requisição para a API do TMDB para `endpoint` e com parâmetros `params`
  """
  default_params = {'api_key': API_KEY, 'language': 'pt-BR'}
  if params:
    default_params.update(params)
  return requests.get(BASE_URL + endpoint, params=default_params)

def get_and_customize_genres() -> dict[int, str]:
    """
      - Faz uma requisição para obter todos os gêntros disponíveis na API do TMDB
      - Adapta esses gêneros para as regras de negócio da aplicação
      - Retorna um dicionário no qual a chave é o ID do gênero e o valor é o nome do gênero
    """
    logging.info("Getting genres")

    genres_map = {}
    endpoint_genres_tv = '/genre/tv/list'
    endpoint_genres_movie = '/genre/movie/list'

    genres_tv_res = make_request(endpoint_genres_tv)
    genres_tv = json.loads(genres_tv_res.text)['genres']
    genres_movie_res = make_request(endpoint_genres_movie)
    genres_movie = json.loads(genres_movie_res.text)['genres']

    unique_genres = list(set([tuple(d.items()) for d in genres_movie + genres_tv]))
    unique_genres = [dict(genre) for genre in unique_genres]

    for genre in unique_genres:
      genres_map[genre['id']] = genre['name']

    # Customizando gêneros
    genres_map[10766] = 'Novela'
    genres_map[10765] = ['Ficção científica', 'Fantasia']
    genres_map[10762] = 'Infantil'
    genres_map[10768] = ['Guerra', 'Política']
    genres_map[10759] = ['Ação', 'Aventura']

    return {str(key):value for key, value in genres_map.items()}

def serialize_genre(media: str, genres_map: dict[int, str]):
  """
   Resolve os IDs dos gêneros retornados em uma requisição para os gêneros customizados segundo
  as regras de negócio da aplicação
  """
  logging.info(f"\tResolving genres name")
  media['genres'] = []
  for genre_id in media['genre_ids'] or []:
    if type(genres_map[str(genre_id)]) == list:
      for g in genres_map[str(genre_id)]:
        media['genres'].append(g)
    else:
      media['genres'].append(genres_map[str(genre_id)])
  media.pop('genre_ids')

def get_credits(media_id: int, media_type: Literal['tv', 'movie'], values: dict):
  """
    - Faz uma requisição para o endpoint "credits" da API do TMDB a fim de pegar
  dados sobre o elenco e o diretor do filme de ID `media_id`
    - `values` é um dicionário (passado por referência -- me perdoem mestres da programação
    funcional) que irá ser atualizado com os dados de elenco e diretor
  """
  logging.info(f"\tGetting credits")

  url = f'/{media_type}/{media_id}/credits'
  res_credits = make_request(url)
  if res_credits.ok:
    credits_data = json.loads(res_credits.content)
    values['cast'] = list(map(lambda item: item['original_name'], credits_data['cast']))
    director_found = next(filter(lambda item: item['job'] == 'Director', credits_data['crew']), None)
    values['director'] = director_found and director_found['original_name']
  else:
    raise Exception(f'Error on request for credits detail [media {media_id}]') 

def get_custom_fields(values: dict, 
                      details: List[Union[str, dict[Literal['finalName', 'fieldName', 'func'], Union[str, callable]]]], 
                      details_data: dict):
  """
    - Resolve a lista de campos passados em `details` para os dados retornados na requisição
  para o endpint `<media_type>/<media_id>('tv' ou 'movie')>` (que devem estar em `details_data`)
    - Os campos de details devem ser passados de duas maneiras diferentes:
      - Uma string com o nome do campo que deve ser recuperado do resultado da requisição, sendo 
      que o campo no resultado final terá esse mesmo nome
      - Um dicionário contendo o nome final do campo em `finalName`, o nome com que o campo aparece
      dentro da resposta da requisição em `fieldName` e um função para processar os dados do campo que
      estão na resposta original em `func`
      -> Exemplo: se em details existe o campo "revenue" e você quer esse campo no seu resultado final,
      você deve passar a string `revenue` dentro da lista de details.
      -> Exemplo: se em details tem uma lista de produtoras em `producers` sendo que cada valor dessa lista
      tem o nome da produtora (chave `name`) e país de origem (chave `country`), caso você queira só o nome 
      das produtoras em um nome campo "producers_name", você devem mandar o dicionário 
      `{'finalName': 'producers_name', 'fieldName': 'producers', 'func': lambda p: p['name']}`
    - Os resultados finais serão atualizados em um diciário `values` passado por referência (espero que os
    mestres da programação funcional estejam de bom humor ;-;)
  """
  for field in details:
    logging.info(f"\tGettind field {field}")
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

def get_detail(media: dict, media_type: Literal['tv', 'movie'], details: List[Union[str, dict[Literal['finalName', 'fieldName', 'func'], Union[str, callable]]]], genres_map: dict[int, str]):
  """
    - Para o elemento `media` passado
      - Faz uma requisição para a rota `details` (pegando os campos de detalhe especificados em details)
      - Faz uma requisição para a rota `credits` (pegando informações de elenco e diretores)
      - Mapeia os gêneros customizados
  """
  logging.info(f"Getting details for movie {media['title']}")

  media['type'] = media_type
  endpoint = f'/{media_type}/{media["id"]}'
  res_details = make_request(endpoint)
  details_data = {}
  values = {}

  if res_details.ok:
    try:
      details_data = json.loads(res_details.text)
    except:
      details_data = {}

    get_custom_fields(values, details, details_data)
    get_credits(media['id'], media_type ,values)
    media.update(values)

    serialize_genre(media, genres_map)
  else:
    raise Exception(f'Error on request for detail [media {media["id"]}]')

def create_dataframe(detailed_response: dict):
  """
    Cria um dataframe a partir dos dados ingeridos da API
      - Renomeia alguns nomes para deixá-los padrão em todos os tipos de mídia
      - Dropa colunas que não serão usadas
      - Converte para `datetime` os dados de data
      - Cria uma coluna para o ano de lançamento
  """
  final_df = pd.DataFrame.from_dict(detailed_response)
  final_df.set_index('id', inplace=True)
  
  # Padroniza os nomes das features para filmes e séries
  final_df.rename(columns={
    'name': 'title',
    'original_name': 'original_title',
    'first_air_date': 'release_date'
  }, inplace=True, errors='ignore')
  
  final_df.drop(['adult', 'backdrop_path', 'original_language', 'video', 'overview'], inplace=True, axis=1, errors='ignore')
  
  final_df['poster_path'] = 'https://image.tmdb.org/t/p/w220_and_h330_face' + final_df['poster_path']
  
  final_df['release_date'] = pd.to_datetime(final_df['release_date'])
  final_df['release_year'] = final_df['release_date'].dt.year
  
  return final_df

def get_detailed_response_list(response: dict, media_type: Literal['movie', 'tv'], 
                               details: List[Union[str, dict[Literal['finalName', 'fieldName', 'func'], Union[str, callable]]]], 
                               genres_map: dict[int, str]):
  """
    Para cada mídia retornada na resposta
      - Pega os detalhes especificados em `details`
      - Pega as informações de elenco e diretor
      - Mapeia os gêneros para o gêneros customizados
  """
  for media in response:
    get_detail(media, media_type, details, genres_map)


def get_discover(page, params={}):
    params['page'] = page
    response = make_request('/discover/movie', params=params)

    return json.loads(response.content.decode())