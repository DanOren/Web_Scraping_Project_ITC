import pymysql
import config as cfg
import logging
import sys
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
# Create formatter
formatter = logging.Formatter('%(asctime)s-FILE:%(filename)s-FUNC:%(funcName)s-LINE:%(lineno)d-%(message)s')

# Create a file handler and add it to logger.
file_handler = logging.FileHandler('web_scraper.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
#
# stream_handler = logging.StreamHandler(sys.stdout)
# stream_handler.setLevel(logging.INFO)
# stream_handler.setFormatter(formatter)
# logger.addHandler(stream_handler)


class Database:
    def __init__(self):
        """
        Initialisation function for Database class.
        """
        self.db_name = cfg.DATABASE_NAME
        logging.info(f'DB name is {self.db_name}')
        self.con, self.cur = self.connect_to_db()
        # self.con, self.cur updated after DB confirmed/created
        self.con, self.cur = self.create_db()
        self.create_tables_db()

    @staticmethod
    def connect_to_db():
        """
        Creates initial connection and cursor objects.
        :return: con and cursor
        """
        # Create initial connection object.
        try:
            con = pymysql.connect(host='localhost', user=cfg.USER_DB,
                                  password=cfg.PASSWORD_DB_SERVER, cursorclass=pymysql.cursors.DictCursor)
        except TypeError:
            logging.critical(f'Could not create connection object for database. Please check parameters used.')
            sys.exit(1)
        # Create initial cursor
        cur = con.cursor()
        return con, cur

    def create_db(self):
        """
        Checks if DB exists. If not, creates it.
        :return: con, cursor
        """
        query = f"CREATE DATABASE IF NOT EXISTS {self.db_name}"
        self.cur.execute(query)
        # Update con with confirmed/new DB info
        try:
            con = pymysql.connect(host='localhost', user=cfg.USER_DB, password=cfg.PASSWORD_DB_SERVER,
                                  database=self.db_name, cursorclass=pymysql.cursors.DictCursor)
        except TypeError:
            logging.critical(f'Could not create cursor object for database. Please check parameters used.')
            sys.exit(1)
            # Updated cursor
        cur = con.cursor()
        return con, cur

    def create_tables_db(self):
        """
        Assembles tables as required if don't exist in self.db_name
        :return: None
        """

        try:
            self.cur.execute("""CREATE TABLE IF NOT EXISTS frequent (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            meta_score varchar(255),
                            user_score varchar(255),
                            wiki_url varchar(1000)
                            );""")
            logging.info(f'studios table functioning.')
        except pymysql.err.ProgrammingError:
            logging.critical(f'Could not execute the SQL to create the frequent table. Please check query.')
            sys.exit(1)

        try:
            self.cur.execute("""   
                            CREATE TABLE IF NOT EXISTS studios (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            name varchar(255)
                            );""")
            logging.info(f'studios table functioning.')
        except pymysql.err.ProgrammingError:
            logging.critical(f'Could not execute the SQL to create the studios table. Please check query.')
            sys.exit(1)

        try:
            self.cur.execute("""    
                            CREATE TABLE IF NOT EXISTS directors (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            name varchar(255)
                            );""")
            logging.info(f'directors table functioning.')
        except pymysql.err.ProgrammingError:
            logging.critical(f'Could not execute the SQL to create the directors table. Please check query.')
            sys.exit(1)

        try:
            self.cur.execute("""    
                            CREATE TABLE IF NOT EXISTS genres (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            name varchar(255)
                            );""")
            logging.info(f'genres table functioning.')
        except pymysql.err.ProgrammingError:
            logging.critical(f'Could not execute the SQL to create the genres table. Please check query.')
            sys.exit(1)

        try:
            self.cur.execute("""    
                            CREATE TABLE IF NOT EXISTS creators (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            name varchar(255)
                            );""")
            logging.info(f'creators table functioning.')
        except pymysql.err.ProgrammingError:
            logging.critical(f'Could not execute the SQL to create the creators table. Please check query.')
            sys.exit(1)

        try:
            self.cur.execute("""    
                            CREATE TABLE IF NOT EXISTS platforms (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            name varchar(255)
                            );""")
            logging.info(f'platforms table functioning.')
        except pymysql.err.ProgrammingError:
            logging.critical(f'Could not execute the SQL to create the platforms table. Please check query.')
            sys.exit(1)

        try:
            self.cur.execute("""    
                            CREATE TABLE IF NOT EXISTS movies (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            title varchar(255) NOT NULL,
                            unique_identifier varchar(500) NOT NULL,
                            release_year varchar(255),
                            rating varchar(255),
                            runtime varchar(255),
                            summary varchar(10000),
                            studio_id int,
                            director_id int,
                            frequent_id int,
                            FOREIGN KEY(studio_id) REFERENCES studios(id),
                            FOREIGN KEY(director_id) REFERENCES directors(id),
                            FOREIGN KEY(frequent_id) REFERENCES frequent(id)
                            );""")
            logging.info(f'movies table functioning.')
        except pymysql.err.ProgrammingError:
            logging.critical(f'Could not execute the SQL to create the movies table. Please check query.')
            sys.exit(1)

        try:
            self.cur.execute("""    
                            CREATE TABLE IF NOT EXISTS movies_genres (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            movie_id int,
                            genre_id int,
                            FOREIGN KEY(movie_id) REFERENCES movies(id),
                            FOREIGN KEY(genre_id) REFERENCES genres(id)
                            );""")
            logging.info(f'movies_genres table functioning.')
        except pymysql.err.ProgrammingError:
            logging.critical(f'Could not execute the SQL to create the movies_genres table. Please check query.')
            sys.exit(1)

        try:
            self.cur.execute("""    
                            CREATE TABLE IF NOT EXISTS tv_shows (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            title varchar(255) NOT NULL,
                            unique_identifier varchar(255) NOT NULL,
                            release_date varchar(255),
                            summary varchar(10000),
                            studio_id int,
                            creator_id int,
                            frequent_id int,
                            FOREIGN KEY(studio_id) REFERENCES studios(id),
                            FOREIGN KEY(creator_id) REFERENCES creators(id),
                            FOREIGN KEY(frequent_id) REFERENCES frequent(id)
                            );""")
            logging.info(f'tv_shows table functioning.')
        except pymysql.err.ProgrammingError:
            logging.critical(f'Could not execute the SQL to create the tv_shows table. Please check query.')
            sys.exit(1)

        try:
            self.cur.execute("""
                            CREATE TABLE IF NOT EXISTS tv_shows_genres (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            tv_show_id int,
                            genre_id int,
                            FOREIGN KEY(tv_show_id) REFERENCES tv_shows(id),
                            FOREIGN KEY(genre_id) REFERENCES genres(id)    
                            );""")
            logging.info(f'tv_shows_genres table functioning.')
        except pymysql.err.ProgrammingError:
            logging.critical(f'Could not execute the SQL to create the tv_shows_genres table. Please check query.')
            sys.exit(1)

        try:
            self.cur.execute("""
                            CREATE TABLE IF NOT EXISTS games (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            title varchar(255) NOT NULL,
                            unique_identifier varchar(255) NOT NULL,
                            release_date varchar(255),
                            rating varchar(255),
                            summary varchar(10000),
                            studio_id int,
                            frequent_id int,
                            FOREIGN KEY(studio_id) REFERENCES studios(id),
                            FOREIGN KEY(frequent_id) REFERENCES frequent(id)
                            );""")
            logging.info(f'games table functioning.')
        except pymysql.err.ProgrammingError:
            logging.critical(f'Could not execute the SQL to create the games table. Please check query.')
            sys.exit(1)

        try:
            self.cur.execute("""
                            CREATE TABLE IF NOT EXISTS games_genres (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            game_id int,
                            genre_id int,
                            FOREIGN KEY(game_id) REFERENCES games(id),
                            FOREIGN KEY(genre_id) REFERENCES genres(id)
                            );""")
            logging.info(f'games_genres table functioning.')
        except pymysql.err.ProgrammingError:
            logging.critical(f'Could not execute the SQL to create the games_genres table. Please check query.')
            sys.exit(1)

        try:
            self.cur.execute("""                          
                            CREATE TABLE IF NOT EXISTS games_platforms (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            game_id int,
                            platform_id int,
                            FOREIGN KEY(game_id) REFERENCES games(id),
                            FOREIGN KEY(platform_id) REFERENCES platforms(id)  
                            );""")
            logging.info(f'games_platforms table functioning.')
        except pymysql.err.ProgrammingError:
            logging.critical(f'Could not execute the SQL to create the games_platforms table. Please check query.')
            sys.exit(1)

    def add_to_database_by_type(self, container, container_type):
        """
        Checks the type of the Dataframe and calls the correct method to add to the
        database (movies, tv shows, games).
        :param container: Dataframe
        :param container_type: Type of dataframe
        :return: None
        """
        if container_type == 'movies':
            self.populate_tables_movies(container)
            logging.info(f'Finished populating Movies to database.')
        elif container_type == 'tv':
            self.populate_tables_tv_shows(container)
            logging.info(f'Finished populating TV shows to database.')
        elif container_type == 'games':
            self.populate_tables_games(container)
            logging.info(f'Finished populating Games to database.')
        else:
            logging.error(f'Failed to add to database. Please check the item type. '
                          f'It must be either movies, tv or games.')

    def populate_tables_movies(self, container):
        """
        Takes in a pd Dataframe. Each row series contains all information for a movie item.
        Inserts the data from the df to the database.
        :param self:
        :param container: pd DataFrame.
        :return: None
        """
        counter = 0
        for index, row_df in container.iterrows():
            unique_identifier = index
            # Check if movie already in table
            self.cur.execute(f"""SELECT id as id, unique_identifier as unique_identifier 
                                 FROM movies WHERE unique_identifier="{unique_identifier}";""")
            movie_existence_query = self.cur.fetchone()

            # If movie already in db, need to check if frequently updated items have changed
            if movie_existence_query:
                movie_id = movie_existence_query['id']
                # Get rating and wiki_url to check
                self.cur.execute(f"""SELECT frequent_id, meta_score, user_score, 
                                     wiki_url
                                     FROM movies LEFT JOIN frequent
                                     ON movies.frequent_id = frequent.id
                                     WHERE movies.id={movie_id};""")
                frequent_query_movies = self.cur.fetchone()
                # Id in frequent table
                frequent_id = frequent_query_movies['frequent_id']

                # If items in frequent table haven't changed, no need to update
                if frequent_query_movies['meta_score'] == row_df['Metascore'] and \
                        frequent_query_movies['user_score'] == row_df['User score'] and\
                        frequent_query_movies['wiki_url'] == row_df['wiki_url']:
                    continue
                else:
                    # Update items in frequent table
                    sql_to_execute_frequent = fr"""UPDATE frequent
                                                    SET meta_score = %s, user_score = %s,
                                                    wiki_url = %s
                                                    WHERE id = %s;"""
                    self.cur.execute(sql_to_execute_frequent, (row_df['Metascore'], row_df['User score'],
                                                               row_df['wiki_url'], frequent_id))
                    self.con.commit()
                    logging.info(f' frequent table updated for {unique_identifier} in {self.db_name}')
            else:
                # If no record for movie in movies table, need to add
                # Check if studio in studios table
                self.cur.execute(f"""SELECT id as id FROM studios WHERE name="{row_df['Studio']}";""")
                studio_existence_query = self.cur.fetchone()
                if studio_existence_query is None:
                    # If studio not in studio table, insert it and then select the id to use for movies FK.
                    self.cur.execute(f"""INSERT INTO studios (name) VALUES ("{row_df['Studio']}");""")
                    self.cur.execute(f"""SELECT id AS id FROM studios WHERE name="{row_df['Studio']}";""")
                    studio_existence_query = self.cur.fetchone()
                    studio_id = studio_existence_query['id']
                else:
                    studio_id = studio_existence_query['id']
                # Check if directors in directors table
                self.cur.execute(f"""SELECT id AS id FROM directors WHERE name="{row_df['Director']}";""")
                director_existence_query = self.cur.fetchone()
                if director_existence_query is None:
                    self.cur.execute(f"""INSERT INTO directors (name) VALUES ("{row_df['Director']}");""")
                    self.cur.execute(f"""SELECT id AS id FROM directors WHERE name="{row_df['Director']}";""")
                    director_existence_query = self.cur.fetchone()
                    director_id = director_existence_query['id']
                else:
                    director_id = director_existence_query['id']

                # Insert into frequent table first, as movies table references frequent
                sql_to_execute_frequent = fr"""INSERT INTO frequent (meta_score, user_score, wiki_url) 
                                            VALUES (%s, %s, %s);"""
                self.cur.execute(sql_to_execute_frequent, (row_df['Metascore'],
                                                           row_df['User score'], row_df['wiki_url']))
                # Get id from frequent table to use for movies table
                self.cur.execute(f"""SELECT max(id) as id FROM frequent;""")
                frequent_id_query = self.cur.fetchone()
                frequent_id = frequent_id_query['id']
                # sql for movies table
                sql_to_execute_movies = fr"""INSERT INTO movies (title, unique_identifier,  
                                             release_year, rating, runtime, summary, studio_id,
                                             director_id, frequent_id) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"""
                # Insert record into movies table
                self.cur.execute(sql_to_execute_movies, (row_df['Title'], unique_identifier, row_df['Release Year'], row_df['Rating'],
                                                         row_df['Runtime'], row_df['Summary'], studio_id, director_id, frequent_id))

                self.cur.execute(f"""SELECT id FROM movies WHERE unique_identifier="{unique_identifier}";""")
                movie_id_query = self.cur.fetchone()
                movie_id = movie_id_query['id']
                # Insert into movies_genres table
                for genre in row_df['Genres']:
                    self.cur.execute(f"""SELECT id FROM genres WHERE name="{genre}";""")
                    genre_id_query = self.cur.fetchone()
                    if genre_id_query is None:
                        self.cur.execute(f"""INSERT INTO genres (name) VALUES ("{genre}");""")
                        self.cur.execute(f"""SELECT id FROM genres WHERE name="{genre}";""")
                        genre_id_query = self.cur.fetchone()
                        genre_id = genre_id_query['id']
                    else:
                        genre_id = genre_id_query['id']
                    # Insert this movie, genre combination into movies_genres table
                    self.cur.execute(f"""INSERT INTO movies_genres (movie_id, genre_id) 
                                         VALUES ({movie_id}, {genre_id});""")
                counter += 1
                # if counter % cfg.SIZE_OF_COMMIT == 0 or counter == len(container) - 1:
                self.con.commit()
                logging.info(f'{unique_identifier} successfully added to {self.db_name}')
                logging.debug(f'Commit of {cfg.SIZE_OF_COMMIT} entries')

    def populate_tables_tv_shows(self, container):
        """
        Takes in a pd Dataframe. Each row series contains all information for a tv_show item.
        Inserts the data from the df to the database.
        :param self:
        :param container: pd DataFrame.
        :return: None
        """
        counter = 0
        for index, row_df in container.iterrows():
            unique_identifier = index
            # Check if tv_show already in table
            self.cur.execute(f"""SELECT id as id, unique_identifier as unique_identifier 
                                 FROM tv_shows WHERE unique_identifier="{unique_identifier}";""")
            tv_show_existence_query = self.cur.fetchone()
           
            if tv_show_existence_query:
                tv_show_id = tv_show_existence_query['id']
                # Get rating and wiki_url to check
                self.cur.execute(f"""SELECT frequent_id, meta_score, user_score, 
                                     wiki_url
                                     FROM tv_shows LEFT JOIN frequent
                                     ON tv_shows.frequent_id = frequent.id
                                     WHERE tv_shows.id={tv_show_id};""")
                frequent_query_tv_shows = self.cur.fetchone()
                # Id in frequent table
                frequent_id = frequent_query_tv_shows['frequent_id']
                # If items in frequent table haven't changed, no need to update
                if frequent_query_tv_shows['meta_score'] == row_df['Metascore'] and \
                        frequent_query_tv_shows['user_score'] == row_df['User score'] and \
                        frequent_query_tv_shows['wiki_url'] == row_df['wiki_url']:
                    continue
                else:
                    # Update items in frequent table
                    sql_to_execute_frequent = fr"""UPDATE frequent
                                                    SET meta_score = %s, user_score = %s,
                                                    wiki_url = %s
                                                    WHERE id = %s;"""
                    self.cur.execute(sql_to_execute_frequent, (row_df['Metascore'], row_df['User score'],
                                                               row_df['wiki_url'], frequent_id))
                    self.con.commit()
                    logging.info(f' frequent table updated for {unique_identifier} in {self.db_name}')
            else:
                # If no record for tv_show in tv_shows table, need to add
                # Check if studio in studios table
                self.cur.execute(f"""SELECT id as id FROM studios WHERE name="{row_df['Studio']}";""")
                studio_existence_query = self.cur.fetchone()
                if studio_existence_query is None:
                    # If studio not in studio table, insert it and then select the id to use for tv_shows FK.
                    self.cur.execute(f"""INSERT INTO studios (name) VALUES ("{row_df['Studio']}");""")
                    self.cur.execute(f"""SELECT id AS id FROM studios WHERE name="{row_df['Studio']}";""")
                    studio_existence_query = self.cur.fetchone()
                    studio_id = studio_existence_query['id']
                else:
                    studio_id = studio_existence_query['id']
                # Check if creator in creators table
                self.cur.execute(f"""SELECT id AS id FROM creators WHERE name="{row_df['Creator']}";""")
                creator_existence_query = self.cur.fetchone()
                if creator_existence_query is None:
                    self.cur.execute(f"""INSERT INTO creators (name) VALUES ("{row_df['Creator']}");""")
                    self.cur.execute(f"""SELECT id AS id FROM creators WHERE name="{row_df['Creator']}";""")
                    creator_existence_query = self.cur.fetchone()
                    creator_id = creator_existence_query['id']
                else:
                    creator_id = creator_existence_query['id']

                # Insert into frequent table first, as tv_shows table references frequent
                sql_to_execute_frequent = fr"""INSERT INTO frequent (meta_score, user_score, wiki_url) 
                                                      VALUES (%s, %s, %s);"""
                self.cur.execute(sql_to_execute_frequent, (row_df['Metascore'],
                                                           row_df['User score'], row_df['wiki_url']))
                # Get id from frequent table to use for tv_shows table
                self.cur.execute(f"""SELECT max(id) as id FROM frequent;""")
                frequent_id_query = self.cur.fetchone()
                frequent_id = frequent_id_query['id']
                # sql for tv_shows table
                sql_to_execute = fr"""INSERT INTO tv_shows (title, unique_identifier, release_date,
                                      summary, studio_id, creator_id, frequent_id) 
                                      VALUES (%s, %s, %s, %s, %s, %s, %s);"""
                # Insert record into tv_shows table
                self.cur.execute(sql_to_execute, (row_df['Title'], unique_identifier,
                                                  row_df['Release Year'], row_df['Summary'],
                                                  studio_id, creator_id, frequent_id))

                self.cur.execute(f"""SELECT id FROM tv_shows WHERE unique_identifier="{unique_identifier}";""")
                tv_show_query = self.cur.fetchone()
                tv_show_id = tv_show_query['id']
                # Insert into tv_shows_genres table
                for genre in row_df['Genres']:
                    self.cur.execute(f"""SELECT id FROM genres WHERE name="{genre}";""")
                    genre_id_query = self.cur.fetchone()
                    if genre_id_query is None:
                        self.cur.execute(f"""INSERT INTO genres (name) VALUES ("{genre}");""")
                        self.cur.execute(f"""SELECT id FROM genres WHERE name="{genre}";""")
                        genre_id_query = self.cur.fetchone()
                        genre_id = genre_id_query['id']
                    else:
                        genre_id = genre_id_query['id']
                    # Insert this tv_show, genre combination into tv_shows_genres table
                    self.cur.execute(f"""INSERT INTO tv_shows_genres (tv_show_id, genre_id) VALUES 
                                         ({tv_show_id}, {genre_id});""")
                counter += 1
                # if counter % cfg.SIZE_OF_COMMIT == 0 or counter == len(container) - 1:
                self.con.commit()
                logging.info(f'{unique_identifier} successfully added to {self.db_name}')
                logging.debug(f'Commit of {cfg.SIZE_OF_COMMIT} entries')

    def populate_tables_games(self, container):
        """
        Takes in a pd Dataframe. Each row series contains all information for a game item.
        Inserts the data from the df to the database.
        :param self:
        :param container: pd DataFrame.
        :return: None
        """
        counter = 0
        for index, row_df in container.iterrows():
            unique_identifier = index
            # Check game already in table
            self.cur.execute(f"""SELECT id as id, unique_identifier as unique_identifier 
                                        FROM games WHERE unique_identifier="{unique_identifier}";""")
            game_existence_query = self.cur.fetchone()

            # If game already in db, need to check if frequently updated items have changed
            if game_existence_query:
                game_id = game_existence_query['id']
                # Get rating and wiki_url to check
                self.cur.execute(f"""SELECT frequent_id, meta_score, user_score, 
                                                   wiki_url
                                                   FROM games LEFT JOIN frequent
                                                   ON games.frequent_id = frequent.id
                                                   WHERE games.id={game_id};""")
                frequent_query_games = self.cur.fetchone()
                # Id in frequent table
                frequent_id = frequent_query_games['frequent_id']

                # If items in frequent table haven't changed, no need to update
                if frequent_query_games['meta_score'] == row_df['Metascore'] and \
                        frequent_query_games['user_score'] == row_df['User score'] and \
                        frequent_query_games['wiki_url'] == row_df['wiki_url']:
                    continue
                else:
                    # Update items in frequent table
                    sql_to_execute_frequent = fr"""UPDATE frequent
                                                    SET meta_score = %s, user_score = %s,
                                                    wiki_url = %s
                                                    WHERE id = %s;"""
                    self.cur.execute(sql_to_execute_frequent, (row_df['Metascore'], row_df['User score'],
                                                               row_df['wiki_url'], frequent_id))
                    self.con.commit()
                    logging.info(f' frequent table updated for {unique_identifier} in {self.db_name}')
            else:
                # If no record for movie in games table, need to add
                # Check if studio in studios table
                self.cur.execute(f"""SELECT id as id FROM studios WHERE name="{row_df['Studio']}";""")
                studio_existence_query = self.cur.fetchone()
                if studio_existence_query is None:
                    # If studio not in studio table, insert it and then select the id to use for games FK.
                    self.cur.execute(f"""INSERT INTO studios (name) VALUES ("{row_df['Studio']}");""")
                    self.cur.execute(f"""SELECT id AS id FROM studios WHERE name="{row_df['Studio']}";""")
                    studio_existence_query = self.cur.fetchone()
                    studio_id = studio_existence_query['id']
                else:
                    studio_id = studio_existence_query['id']

                # Insert into frequent table first, as games table references frequent
                sql_to_execute_frequent = fr"""INSERT INTO frequent (meta_score, user_score, wiki_url) 
                                                    VALUES (%s, %s, %s);"""
                self.cur.execute(sql_to_execute_frequent, (row_df['Metascore'],
                                                           row_df['User score'], row_df['wiki_url']))
                # Get id from frequent table to use for games table
                self.cur.execute(f"""SELECT max(id) as id FROM frequent;""")
                frequent_id_query = self.cur.fetchone()
                frequent_id = frequent_id_query['id']
                # sql for games table
                sql_to_execute_games = fr"""INSERT INTO games (title, unique_identifier,
                                            release_date, rating,
                                            summary, studio_id, frequent_id) 
                                            VALUES (%s, %s, %s, %s, %s, %s, %s);"""
                self.cur.execute(sql_to_execute_games, (row_df['Title'], unique_identifier, row_df['Release Year'],
                                                        row_df['Rating'], row_df['Summary'],
                                                        studio_id, frequent_id))

                self.cur.execute(f"""SELECT id FROM games WHERE unique_identifier="{unique_identifier}";""")
                game_id_query = self.cur.fetchone()
                game_id = game_id_query['id']
                # Insert into games_genres table
                for genre in row_df['Genres']:
                    self.cur.execute(f"""SELECT id FROM genres WHERE name="{genre}";""")
                    genre_id_query = self.cur.fetchone()
                    if genre_id_query is None:
                        self.cur.execute(f"""INSERT INTO genres (name) VALUES ("{genre}");""")
                        self.cur.execute(f"""SELECT id FROM genres WHERE name="{genre}";""")
                        genre_id_query = self.cur.fetchone()
                        genre_id = genre_id_query['id']
                    else:
                        genre_id = genre_id_query['id']
                    # Insert this game, genre combination into games_genres table
                    self.cur.execute(f"""INSERT INTO games_genres (game_id, genre_id)
                                        VALUES ({game_id}, {genre_id});""")
                # Insert into games_platforms table
                for platform in row_df['Platform']:
                    self.cur.execute(f"""SELECT id FROM platforms WHERE name="{platform}";""")
                    platform_id_query = self.cur.fetchone()
                    if platform_id_query is None:
                        self.cur.execute(f"""INSERT INTO platforms (name) VALUES ("{platform}");""")
                        self.cur.execute(f"""SELECT id FROM platforms WHERE name="{platform}";""")
                        platform_id_query = self.cur.fetchone()
                        platform_id = platform_id_query['id']
                    else:
                        platform_id = platform_id_query['id']
                    # Insert this game, platform combination into games_platforms table
                    self.cur.execute(f"""INSERT INTO games_platforms (game_id, platform_id) VALUES 
                                         ({game_id}, {platform_id});""")
                counter += 1
                # if counter % cfg.SIZE_OF_COMMIT == 0 or counter == len(container) - 1:
                self.con.commit()
                logging.info(f'{unique_identifier} successfully added to {self.db_name}')
                logging.debug(f'Commit of {cfg.SIZE_OF_COMMIT} entries')


def main():
    # df_tv_shows = cl.tv_show('year', '2002')
    # print(df_tv_shows.columns)
    # db1.populate_tables_tv_shows(df_tv_shows)
    # df_game = cl.game('year', '1996')
    # df_game = df_game.replace(np.nan, "missing", regex=True)
    df_movies = pd.read_csv('tester_movies2.csv')
    df_tv_shows = pd.read_csv('tester_tv_shows2.csv')
    df_games = pd.read_csv('tester_games2.csv')
    db1 = Database()
    assert db1.db_name == 'metacritic'
    db1.connect_to_db()
    db1.create_db()
    db1.create_tables_db()
    db1.add_to_database_by_type(df_games, 'games')
    print(db1.db_name)


if __name__ == '__main__':
    main()
