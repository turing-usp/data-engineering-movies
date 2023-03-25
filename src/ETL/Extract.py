import requests
import json
import pandas as pd
import logging

from src.ETL.settings import API_KEY

BASE_URL = 'https://api.themoviedb.org/3'


def make_request(endpoint = '', params = {}):
  default_params = {'api_key': API_KEY, 'language': 'pt-BR'}
  if params:
    default_params.update(params)
  return requests.get(BASE_URL + endpoint, params=default_params)

def get_and_customize_genres():
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

def serialize_genre(media, genres_map):
  logging.info(f"\tResolving genres name")
  media['genres'] = []
  for genre_id in media['genre_ids'] or []:
    if type(genres_map[str(genre_id)]) == list:
      for g in genres_map[str(genre_id)]:
        media['genres'].append(g)
    else:
      media['genres'].append(genres_map[str(genre_id)])
  media.pop('genre_ids')

def get_credits(media_id, media_type, values):
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

def get_custom_fields(values, details, details_data):
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

def get_detail(media, media_type, details, genres_map):
  
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

def create_dataframe(detailed_response):
  final_df = pd.DataFrame.from_dict(detailed_response)
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
  
  return final_df

def get_detailed_response_list(response, media_type, details, genres_map):
  count = 1
  total = len(response)
  for media in response:
    get_detail(media, media_type, details, genres_map, count, total)
    count += 1
