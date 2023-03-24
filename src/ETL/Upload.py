from google.cloud import bigquery

class Upload:
    def __init__(self, data) -> None:
        self.data = data

    def upload(self):
        client = bigquery.Client()

        table_id = "turingdb.data_warehouse.movies"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(
            self.data,
            table_id,
            job_config=job_config
        ) 
        job.result()  
        table = client.get_table(table_id)
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )

if __name__ == '__main__':
    import os
    import pandas as pd
    from ExtractExportsTMDB import ExtractExportsTMDB
    from ExtractDataTMDB import ExtractDataTMDB

    fields = [
		'title', 'original_title', 'overview', 'poster_path', 
		'popularity', 'release_date', 'vote_average', 
		'vote_count', 'imdb_id', 'status', 'revenue', 'budget', 'release_year', 
		{
			'fieldName': 'genres', 'finalName': 'genre_ids', 
			'func': lambda x: [g['id'] for g in x]
		}
	]

    os.environ['PROJECT_ID'] = 'turingdb'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', '..', 'turingdb-f911c3126edd.json')

    #test = [{'adult': False, 'id': 55405, 'original_title': 'Le Roi Lear', 'popularity': 0.6, 'video': False, 'type': 'movie', 'title': 'Le Roi Lear', 'overview': '', 'poster_path': None, 'release_date': '2007-09-19', 'vote_average': 0.0, 'vote_count': 0, 'imdb_id': 'tt1103253', 'status': 'Released', 'revenue': 0, 'budget': 0, 'release_year': None, 'cast': ['Michel Piccoli', 'Anne See', 'Lisa Martino', 'Gérard Watkins'], 'director': None, 'genres': ['Drama']}, {'adult': False, 'id': 55406, 'original_title': 'Kyläraittien kuningas', 'popularity': 0.6, 'video': False, 'type': 'movie', 'title': 'Kyläraittien kuningas', 'overview': '', 'poster_path': None, 'release_date': '1945-11-03', 'vote_average': 5.0, 'vote_count': 1, 'imdb_id': 'tt0037856', 'status': 'Released', 'revenue': 0, 'budget': 0, 'release_year': None, 'cast': ['Tuire Orri', 'Esko Vettenranta', 'Sirkka Sipilä', 'Unto Salminen', 'Litja Ilmari', 'Paavo Jännes', 'Kyllikki Väre', 'Esko Saha', 'Maire Hyvönen', 'Aku Peltonen', 'Joel Asikainen', 'Vilho Ruuskanen', 'Toppo Elonperä', 'Reijo Huttunen', 'Otto Noro'], 'director': 'Jorma Nortimo', 'genres': ['Drama', 'Romance']}, {'adult': False, 'id': 55407, 'original_title': "Donald's Off Day", 'popularity': 2.177, 'video': False, 'type': 'movie', 'title': "Donald's Off Day", 'overview': '', 'poster_path': '/suwi9cvmq2jEkXPQgDGpq3LRJOD.jpg', 'release_date': '1944-12-08', 'vote_average': 6.3, 'vote_count': 20, 'imdb_id': 'tt0036769', 'status': 'Released', 'revenue': 0, 'budget': 0, 'release_year': None, 'cast': ['Clarence Nash'], 'director': 'Jack Hannah', 'genres': ['Animação']}, {'adult': False, 'id': 55408, 'original_title': 'Kymmenen riivinrautaa', 'popularity': 1.536, 'video': False, 'type': 'movie', 'title': 'Kymmenen riivinrautaa', 'overview': '', 'poster_path': '/lg29QI92XAkf6H8oMfcvLUGAeqQ.jpg', 'release_date': '2002-08-30', 'vote_average': 3.0, 'vote_count': 3, 'imdb_id': 'tt0328045', 'status': 'Released', 'revenue': 0, 'budget': 0, 'release_year': None, 'cast': ['Esko Salminen', 'Santeri Kinnunen', 'Petra Frey', 'Rinna Paatso', 'Anu Sinisalo', 'Kirsi Ylijoki', 'Kristiina Elstelä', 'Veera Degerholm', 'Susanna Haavisto', 'Aino Seppo', 'Rea Mauranen', 'Tea Ista', 'Kari Väänänen', 'Jukka Juolahti', 'Mikael Rejström', 'Linda Brava', 'Susanna Anteroinen', 'Risto Palm', 'Anja Pohjola'], 'director': 'Ere Kokkonen', 'genres': ['Comédia']}, {'adult': False, 'id': 55409, 'original_title': "Donald's Penguin", 'popularity': 2.098, 'video': False, 'type': 'movie', 'title': 'O Pinguim do Donald', 'overview': 'Donald recebe por correio um pinguim direto do Polo Sul.', 'poster_path': '/xmZsqdcFKxHV9rc0jnyDgPlulta.jpg', 'release_date': '1939-08-11', 'vote_average': 5.8, 'vote_count': 26, 'imdb_id': 'tt0031243', 'status': 'Released', 'revenue': 0, 'budget': 0, 'release_year': None, 'cast': ['Clarence Nash', 'Louis Manley'], 'director': 'Jack King', 'genres': ['Animação']}, {'adult': False, 'id': 55410, 'original_title': 'Lapin kullan kimallus', 'popularity': 0.985, 'video': False, 'type': 'movie', 'title': 'Lapin kullan kimallus', 'overview': '', 'poster_path': '/oZVsK3Rg1FVOmihTOTTiNTO4dNd.jpg', 'release_date': '1999-09-10', 'vote_average': 5.2, 'vote_count': 6, 'imdb_id': 'tt0158717', 'status': 'Released', 'revenue': 0, 'budget': 0, 'release_year': None, 'cast': ['Vesa Vierikko', 'Pirkka-Pekka Petelius', 'Riko Eklundh'], 'director': 'Åke Lindman', 'genres': ['Drama', 'História', 'Aventura']}, {'adult': False, 'id': 55412, 'original_title': 'Lapualaismorsian', 'popularity': 1.562, 'video': False, 'type': 'movie', 'title': 'Lapualaismorsian', 'overview': '', 'poster_path': '/Av1oh03qAbG2pQseUqPeAY1akZK.jpg', 'release_date': '1967-12-01', 'vote_average': 7.8, 'vote_count': 4, 'imdb_id': 'tt0121853', 'status': 'Released', 'revenue': 0, 'budget': 0, 'release_year': None, 'cast': ['Kristiina Halkola', 'Kirsti Wallasvaara', 'Aulikki Oksanen', 'Pekka Laiho', 'Pekka Autiovuori', 'Jukka Sipilä', 'Heikki Kinnunen', 'Kaj Chydenius', 'Vesa-Matti Loiri', 'Kalle Holmberg', 'Mikko Niskanen'], 'director': 'Mikko Niskanen', 'genres': ['Drama']}, {'adult': False, 'id': 55413, 'original_title': 'Leikkikalugangsteri', 'popularity': 1.016, 'video': False, 'type': 'movie', 'title': 'Leikkikalugangsteri', 'overview': '', 'poster_path': '/oPxXV9FqxsMWb8uRUOkyKdAKfeI.jpg', 'release_date': '1969-08-22', 'vote_average': 5.3, 'vote_count': 3, 'imdb_id': 'tt0134777', 'status': 'Released', 'revenue': 0, 'budget': 0, 'release_year': None, 'cast': ['Spede Pasanen', 'Tamara Lund', 'Vesa-Matti Loiri', 'Simo Salminen', 'Leo Jokela'], 'director': 'Ere Kokkonen', 'genres': ['Comédia']}, {'adult': False, 'id': 55414, 'original_title': 'Brennendes Herz', 'popularity': 0.6, 'video': False, 'type': 'movie', 'title': 'Brennendes Herz', 'overview': '', 'poster_path': None, 'release_date': '2006-11-02', 'vote_average': 6.0, 'vote_count': 2, 'imdb_id': 'tt0968259', 'status': 'Released', 'revenue': 0, 'budget': 0, 'release_year': None, 'cast': ['Alexander Scheer', 'Ivan Anderson', 'Martin Kiefer', 'Marlon Kittel', 'Peter Moltzen', 'Stefan Konarske', 'Max Mauff', 'Kida Khodr Ramadan', 'Akif Aydin', 'Ali Biryar', 'Erhan Emre', 'Christoph Franken', 'Tilo Prückner', 'Ingeborg Westphal'], 'director': 'Manfred Stelzer', 'genres': ['Drama']}, {'adult': False, 'id': 55415, 'original_title': 'Liian iso keikka', 'popularity': 0.615, 'video': False, 'type': 'movie', 'title': 'Liian iso keikka', 'overview': '', 'poster_path': '/wSHCJ9l35VaRuD2DUM8vIr92ik7.jpg', 'release_date': '1986-11-14', 'vote_average': 5.6, 'vote_count': 5, 'imdb_id': 'tt0091412', 'status': 'Released', 'revenue': 0, 'budget': 0, 'release_year': None, 'cast': ['Vesa-Matti Loiri', 'Hannele Lauri', 'Ritva Valkama', 'Pentti Siimes', 'Ville-Veikko Salminen', 'Aake Kalliala', 'Titta Jokinen'], 'director': 'Ere Kokkonen', 'genres': ['Comédia', 'Crime']}]
    #extract = ExtractExportsTMDB(fields, 'movie', show_credits=True)
    #extract.get_dataframe(os.path.join('..', '..', 'files'), '03', '13', '2023')
    #data = [{'adult': False, 'id': 55405, 'original_title': 'Le Roi Lear', 'popularity': 0.6, 'video': False, 'type': 'movie', 'title': 'Le Roi Lear', 'overview': '', 'poster_path': None, 'release_date': '2007-09-19', 'vote_average': 0.0, 'vote_count': 0, 'imdb_id': 'tt1103253', 'status': 'Released', 'revenue': 0, 'budget': 0, 'release_year': None, 'cast': ['Michel Piccoli', 'Anne See', 'Lisa Martino', 'Gérard Watkins'], 'director': None, 'genres': ['Drama']}, {'adult': False, 'id': 55406, 'original_title': 'Kyläraittien kuningas', 'popularity': 0.6, 'video': False, 'type': 'movie', 'title': 'Kyläraittien kuningas', 'overview': '', 'poster_path': None, 'release_date': '1945-11-03', 'vote_average': 5.0, 'vote_count': 1, 'imdb_id': 'tt0037856', 'status': 'Released', 'revenue': 0, 'budget': 0, 'release_year': None, 'cast': ['Tuire Orri', 'Esko Vettenranta', 'Sirkka Sipilä', 'Unto Salminen', 'Litja Ilmari', 'Paavo Jännes', 'Kyllikki Väre', 'Esko Saha', 'Maire Hyvönen', 'Aku Peltonen', 'Joel Asikainen', 'Vilho Ruuskanen', 'Toppo Elonperä', 'Reijo Huttunen', 'Otto Noro'], 'director': 'Jorma Nortimo', 'genres': ['Drama', 'Romance']}]
    extract = ExtractExportsTMDB(fields, 'movie', True)
    df = extract.get_dataframe(os.path.join('..', '..', 'files'), '03', '13', '2023', 2, 2)
    df.info()
    up = Upload(df)
    up.upload()