import pandas as pd
import numpy as np
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium import webdriver


class RottenCrawler:

    def __init__(self, df, headless):
        self.dataset = df
        driver_options = webdriver.ChromeOptions()
        if headless:
            driver_options.add_argument("--headless")
        self.driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=driver_options)

    @staticmethod
    def rottenize(title):
        """
        Esta funcao sustitui os carateres por vazio ou por _
        para depois entrar nos links (urls) dos filmes no rotten
        """
        title = title.translate(str.maketrans({':': '', "'": '', ".":"", ",":"", 
                                            "-":"_", " ":"_"}))
        
        return title.lower()


    def create_df(self):
        links = []
        for tit in self.dataset['original_title'].apply(self.rottenize):
            links.append('https://www.rottentomatoes.com/m/{}'.format(tit))
        
        # listas para armazenar a informacao
        tmdb_id = []

        nomes = []
        TOMATOMETER = []
        TOMATOMETER_rev = []
        aud_scr = []
        aud_scr_rev = []
        main6 = []
        main7 = []
        main8 = []
        main9 = []

        genre = []
        language = []
        director = []
        producer = []
        writer = []
        rel_date_th = []
        rel_date_stream = []
        box = []
        runtime = []
        distributor = []
        description = []
        
        # neste dataset iremos armazenar as informacoes extraidas do Rotten Tomatoes
        df = pd.DataFrame()

        for j in range(len(links)):
            
            # mostra quantos filmes ja varreu
            print(j, end="\r", flush=True)
            
            # entramos no site do filme
            self.driver.get(links[j])
            
            # se aparecer algun erro no navegador voltamos ao comeco do loop
            try:
                # informacoes da descricao do filme
                info_we = self.driver.find_elements(By.CLASS_NAME, "media-body")
                # informações mais breves da descricao
                short_info = info_we[0].text.split('\n')[2:]

                # informacoes principais do filme: nome, scores e quantidade de reviews
                info_main = self.driver.find_elements(By.CLASS_NAME, "thumbnail-scoreboard-wrap")
                info_main = info_main[0].text.split('\n')

                # dicionario para armazenar as infos do filme
                short_info_dic = dict()

                # acrescentamos tambem as informacoes principais
                short_info_dic['Nome'] = info_main[0]
                short_info_dic['TOMATOMETER'] = info_main[2]
                short_info_dic['TOMATOMETER reviews'] = info_main[4]
                short_info_dic['AUDIENCE SCORE'] = info_main[5]
                short_info_dic['AUDIENCE SCORE reviews'] = info_main[7]
                
                # colunas auxiliares
                short_info_dic['main6'] = info_main[1]
                short_info_dic['main7'] = info_main[3]
                short_info_dic['main8'] = info_main[6]
                
                try:
                    short_info_dic['main9'] = info_main[8]
                except:
                    continue
                
            except:
                continue

            # informacoes breves do filme
            for i in range(len(short_info)):
                infos = short_info[i].split(': ')
                try:
                    short_info_dic[infos[0]] = infos[1]
                except:
                    short_info_dic[infos[0]] = np.nan

            # resumo e descricao do filme
            short_info_dic[info_we[0].text.split('\n')[0]] = info_we[0].text.split('\n')[1]

            # colocamos as informacoes nas listas
            try:
                nomes.append(short_info_dic['Nome'])
                tmdb_id.append(self.dataset.loc[[j],['id']].values[0][0])
            except:
                nomes.append(np.nan)
            
            try:
                TOMATOMETER.append(short_info_dic['TOMATOMETER'])
            except:
                TOMATOMETER.append(np.nan)
            
            try:
                TOMATOMETER_rev.append(short_info_dic['TOMATOMETER reviews'])
            except:
                TOMATOMETER_rev.append(np.nan)
            
            try:
                aud_scr.append(short_info_dic['AUDIENCE SCORE'])
            except:
                aud_scr.append(np.nan)
            
            try:
                main6.append(short_info_dic['main6'])
            except:
                main6.append(np.nan)
                
            try:
                main7.append(short_info_dic['main7'])
            except:
                main7.append(np.nan)
                
            try:
                main8.append(short_info_dic['main8'])
            except:
                main8.append(np.nan)
                
            try:
                main9.append(short_info_dic['main9'])
            except:
                main9.append(np.nan)
            
            try:
                aud_scr_rev.append(short_info_dic['AUDIENCE SCORE reviews'])
            except:
                aud_scr_rev.append(np.nan)
            
            try:
                genre.append(short_info_dic['Genre'])
            except:
                genre.append(np.nan)
            
            try:
                language.append(short_info_dic['Original Language'])
            except:
                language.append(np.nan)
            
            try:
                director.append(short_info_dic['Director'])
            except:
                director.append(np.nan)
            
            try:
                producer.append(short_info_dic['Producer'])
            except:
                producer.append(np.nan)
            
            try:
                writer.append(short_info_dic['Writer'])
            except:
                writer.append(np.nan)
            
            try:
                rel_date_th.append(short_info_dic['Release Date (Theaters)'])
            except:
                rel_date_th.append(np.nan)
            
            try:
                rel_date_stream.append(short_info_dic['Release Date (Streaming)'])
            except:
                rel_date_stream.append(np.nan)
            
            try:
                box.append(short_info_dic['Box Office (Gross USA)'])
            except:
                box.append(np.nan)
            
            try:
                runtime.append(short_info_dic['Runtime'])
            except:
                runtime.append(np.nan)
            
            try:
                distributor.append(short_info_dic['Distributor'])
            except:
                distributor.append(np.nan)
                
            try:
                description.append(short_info_dic['MOVIE INFO'])
            except:
                description.append(np.nan)
                
        # Alimentamos o database

        df = pd.DataFrame() 

        df['tmdb_id'] = tmdb_id
    #     df['Movie Name'] = nomes
        df['tomatometer_reviews'] = aud_scr
    #     df['AUDIENCE SCORE reviews'] = aud_scr_rev
        # mudou a estrutura do site, entao algumas listas das q foram criadas inicialmente
        # nao coincidem com o nome da coluna
        df['genre_year'] = TOMATOMETER
    #     df['TOMATOMETER reviews'] = nomes

        df['movie_name'] = main6
        df['tomatometer_score'] = main7
        df['audience_score'] = main8
        df['audience_reviews'] = main9
        
        df['Genre'] = genre
        df['Original Language'] = language
        df['Director'] = director
        df['Producer'] = producer
        df['Writer'] = writer
        df['Release Date (Theaters)'] = rel_date_th
        df['Release Date (Streaming)'] = rel_date_stream
        df['box office'] = box
        df['Runtime'] = runtime
        df['Distributor'] = distributor
        df['MOVIE INFO'] = description
            
        return df


if __name__ == '__main__':

    import sys
    import os
    sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'ETL'))
    
    
    from ExtractDataTMDB import ExtractDataTMDB
    extract = ExtractDataTMDB([], 'movie')
    trending_day = extract.get_route('/trending/:t/day')
    rotten = RottenCrawler(trending_day[:5], True)
    df = rotten.create_df()
    print(trending_day.head())
    print(df.head())

