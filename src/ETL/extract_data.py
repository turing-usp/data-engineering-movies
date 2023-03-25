import json

from .extract import make_request, get_and_customize_genres, get_detailed_response_list, create_dataframe

def get_media(endpoint, initial_page = 1, final_page = None, params = {}):
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
    
def get_route(route, media_type, details, initial_page = 1, final_page = None, params={}):
  
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