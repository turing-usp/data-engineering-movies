from bot_studio import *
import pandas as pd

def ScrapComments(title,N = 2,n = 10, searchword = 'trailer', stop = 2,**kwargs):
    '''
    Coleta os comentários dos primeiros N vídeos da busca
    title + searchword, coletando n comentários de cada vídeo.
    O argumento stop atua como condição de parada que indica
    o número máximo de scrolls em uma página de vídeo ao coletar
    os comentários.

    INPUTS:
    title      --> Título do filme
    N          --> Número de vídeos a serem coletados
    n          --> Número mínimo de comentários coletados por página
    searchword --> Termo que vem após o nome do filme na pesquisa
    stop       --> Número máximo de scrolls para coletar comentários

    OUTPUT:
    df         --> DataFrame com os comentários
    '''
    
    # Abrimos o youtube e pesquisamos pelo título
    youtube = bot_studio.youtube(**kwargs)
    youtube.search(title + ' ' + searchword)
    res = youtube.search_results()

    # Aqui separamos os primeiros N links da busca
    results = res['body'][:N]
    links = [i['link'] for i in results]

    # Salvamos os comentários numa lista e sua conposição me um dicionário
    comments = []
    likes = []
    data = {}

    num_com = 0

    for link in links:
        stop_condition = stop
        youtube.open(link)
        
        while len(data) - num_com < n and stop_condition > 0:
        
            youtube.scroll()

            # Coletamos os comentários
            com = youtube.video_comments()
            comments_tmp = [i['Comment'] for i in com['body']]
            likes_tmp = [i['Likes'] for i in com['body']]
            comments.extend(comments_tmp)
            likes.extend(likes_tmp)

            for i, j in zip(comments,likes):
                data[i] = j

            stop_condition -= 1
        num_com += len(data)
                
    # Criação do DataFrame
    df = pd.DataFrame.from_dict(data, orient = 'index', columns = ['likes'])

    return df

### Teste da função
df = ScrapComments('The Menu',N=2,n=10, searchword = 'trailer', headless = True)
df.to_csv('test_scrap')
print('--- FIM ---')
