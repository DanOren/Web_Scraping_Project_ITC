import pymysql
import config as cfg
import logging

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
        self.con, self.cur = self.connect_to_db()
        # self.con, self.cur updated after DB confirmed/created
        self.con, self.cur = self.create_db()
        self.create_tables_db()

    def connect_to_db(self):
        """
        Creates initial connection and cursor objects.
        :return: con and cursor
        """
        # Create initial connection object.
        con = pymysql.connect(host='localhost', user=cfg.USER_DB,
                              password=cfg.PASSWORD_DB_SERVER, cursorclass=pymysql.cursors.DictCursor)
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
        con = pymysql.connect(host='localhost', user=cfg.USER_DB, password=cfg.PASSWORD_DB_SERVER,
                              database=self.db_name, cursorclass=pymysql.cursors.DictCursor)
        # Updated cursor
        cur = con.cursor()
        return con, cur

    def create_tables_db(self):
        """
        Assembles tables as required if don't exist in self.db_name
        :return: None
        """

        self.cur.execute("""   
                            CREATE TABLE IF NOT EXISTS studios (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            name varchar(255)
                            );""")

        self.cur.execute("""    
                            CREATE TABLE IF NOT EXISTS directors (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            name varchar(255)
                            );""")

        self.cur.execute("""    
                            CREATE TABLE IF NOT EXISTS genres (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            name varchar(255)
                            );""")

        self.cur.execute("""    
                            CREATE TABLE IF NOT EXISTS creators (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            name varchar(255)
                            );""")

        self.cur.execute("""    
                            CREATE TABLE IF NOT EXISTS platforms (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            name varchar(255)
                            );""")

        self.cur.execute("""    
                            CREATE TABLE IF NOT EXISTS movies (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            name varchar(255) NOT NULL,
                            unique_identifier varchar(500) NOT NULL,
                            meta_score varchar(255),
                            user_score varchar(255),
                            release_year varchar(255),
                            rating varchar(255),
                            runtime varchar(255),
                            summary varchar(10000),
                            studio_id int,
                            director_id int,
                            FOREIGN KEY(studio_id) REFERENCES studios(id),
                            FOREIGN KEY(director_id) REFERENCES directors(id)
                            );""")

        self.cur.execute("""    
                            CREATE TABLE IF NOT EXISTS movies_genres (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            movie_id int,
                            genre_id int,
                            FOREIGN KEY(movie_id) REFERENCES movies(id),
                            FOREIGN KEY(genre_id) REFERENCES genres(id)
                            );""")

        self.cur.execute("""    
                            CREATE TABLE IF NOT EXISTS tv_shows (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            name varchar(255) NOT NULL,
                            unique_identifier varchar(255) NOT NULL,
                            meta_score varchar(255),
                            user_score varchar(255),
                            release_date varchar(255),
                            summary varchar(10000),
                            studio_id int,
                            creator_id int,
                            FOREIGN KEY(studio_id) REFERENCES studios(id),
                            FOREIGN KEY(creator_id) REFERENCES creators(id)
                            );""")

        self.cur.execute("""
                            CREATE TABLE IF NOT EXISTS tv_shows_genres (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            tv_show_id int,
                            genre_id int,
                            FOREIGN KEY(tv_show_id) REFERENCES tv_shows(id),
                            FOREIGN KEY(genre_id) REFERENCES genres(id)    
                            );""")

        self.cur.execute("""
                            CREATE TABLE IF NOT EXISTS games (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            name varchar(255) NOT NULL,
                            unique_identifier varchar(255) NOT NULL,
                            meta_score varchar(255),
                            user_score varchar(255),
                            release_date varchar(255),
                            rating varchar(255),
                            summary varchar(10000),
                            studio_id int,
                            FOREIGN KEY(studio_id) REFERENCES studios(id)
                            );""")

        self.cur.execute("""
                            CREATE TABLE IF NOT EXISTS games_genres (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            game_id int,
                            genre_id int,
                            FOREIGN KEY(game_id) REFERENCES games(id),
                            FOREIGN KEY(genre_id) REFERENCES genres(id)
                            );""")

        self.cur.execute("""                          
                            CREATE TABLE IF NOT EXISTS games_platforms (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            game_id int,
                            platform_id int,
                            FOREIGN KEY(game_id) REFERENCES games(id),
                            FOREIGN KEY(platform_id) REFERENCES platforms(id)  
                            );""")

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
            logging.info(f'Finished populating new container to database.')
        elif container_type == 'tv':
            self.populate_tables_tv_shows(container)
            logging.info(f'Finished populating new container to database.')
        elif container_type == 'games':
            self.populate_tables_games(container)
            logging.info(f'Finished populating new container to database.')
        else:
            logging.error(f'Failed to add to database.')

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
            # print(row_df)
            # print(row_num)
            unique_identifier = index
            # Check if movie already in table
            self.cur.execute(f"""SELECT unique_identifier as unique_identifier 
                                 FROM movies WHERE unique_identifier="{unique_identifier}";""")
            duplicate_item_check_query = self.cur.fetchone()
            # if duplicate_item_check_query:
            #     movie_id = duplicate_item_check_query['unique_identifier']
            #     print(movie_id)
            # else:
            #     movie_id = []
            if duplicate_item_check_query:
                continue
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
            self.cur.execute(f"""SELECT id AS id FROM directors WHERE name="{row_df['Director']}";""")
            director_existence_query = self.cur.fetchone()
            if director_existence_query is None:
                self.cur.execute(f"""INSERT INTO directors (name) VALUES ("{row_df['Director']}");""")
                self.cur.execute(f"""SELECT id AS id FROM directors WHERE name="{row_df['Director']}";""")
                director_existence_query = self.cur.fetchone()
                director_id = director_existence_query['id']
            else:
                director_id = director_existence_query['id']
            sql_to_execute = fr"""INSERT INTO movies (name, unique_identifier, meta_score, user_score, release_year,
                                rating, runtime, summary, studio_id, director_id) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
            # print(sql_to_execute)
            # Insert record into movies table
            self.cur.execute(sql_to_execute, (row_df['Title'], unique_identifier, row_df['Metascore'],
                             row_df['User score'], row_df['Release Year'], row_df['Rating'],
                             row_df['Runtime'], row_df['Summary'], studio_id, director_id))

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
                self.cur.execute(f"""INSERT INTO movies_genres (movie_id, genre_id) VALUES ({movie_id}, {genre_id});""")
            counter += 1
            # if counter % cfg.SIZE_OF_COMMIT == 0 or counter == len(container) - 1:
            self.con.commit()
            logging.debug(f'Commit of {cfg.SIZE_OF_COMMIT} entries')
            logging.debug(f'Item {unique_identifier} entries')

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
            # print(row_df)
            # print(row_num)
            unique_identifier = index
            # Check if movie already in table
            self.cur.execute(f"""SELECT unique_identifier as unique_identifier 
                                 FROM movies WHERE unique_identifier="{unique_identifier}";""")
            duplicate_item_check_query = self.cur.fetchone()
            # if duplicate_item_check_query:
            #     movie_id = duplicate_item_check_query['unique_identifier']
            #     print(movie_id)
            # else:
            #     movie_id = []
            if duplicate_item_check_query:
                continue
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
            self.cur.execute(f"""SELECT id AS id FROM creators WHERE name="{row_df['Creator']}";""")
            creator_existence_query = self.cur.fetchone()
            if creator_existence_query is None:
                self.cur.execute(f"""INSERT INTO creators (name) VALUES ("{row_df['Creator']}");""")
                self.cur.execute(f"""SELECT id AS id FROM creators WHERE name="{row_df['Creator']}";""")
                creator_existence_query = self.cur.fetchone()
                creator_id = creator_existence_query['id']
            else:
                creator_id = creator_existence_query['id']
            sql_to_execute = fr"""INSERT INTO tv_shows (name, unique_identifier, meta_score, user_score, release_date,
                                summary, studio_id, creator_id) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""
            # print(sql_to_execute)
            # Insert record into movies table
            self.cur.execute(sql_to_execute, (row_df['Title'], unique_identifier, row_df['Metascore'],
                             row_df['User score'], row_df['Release Year'], row_df['Summary'],
                             studio_id, creator_id))

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
            logging.debug(f'Commit of {cfg.SIZE_OF_COMMIT} entries')
            logging.debug(f'Item {unique_identifier} entries')

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
            # print(row_df)
            # print(row_num)
            unique_identifier = index
            # Check if movie already in table
            self.cur.execute(f"""SELECT unique_identifier as unique_identifier 
                                 FROM games WHERE unique_identifier="{unique_identifier}";""")
            duplicate_item_check_query = self.cur.fetchone()
            # if duplicate_item_check_query:
            #     movie_id = duplicate_item_check_query['unique_identifier']
            #     print(movie_id)
            # else:
            #     movie_id = []
            if duplicate_item_check_query:
                continue
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
            sql_to_execute = fr"""INSERT INTO games (name, unique_identifier, meta_score, user_score, release_date,
                                rating, summary, studio_id) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""
            # print(sql_to_execute)
            # Insert record into movies table
            self.cur.execute(sql_to_execute, (row_df['Title'], unique_identifier, row_df['Metascore'],
                             row_df['User score'], row_df['Release Year'], row_df['Rating'], row_df['Summary'],
                             studio_id))

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
                self.cur.execute(f"""INSERT INTO games_genres (game_id, genre_id) VALUES ({game_id}, {genre_id});""")
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
            logging.debug(f'Commit of {cfg.SIZE_OF_COMMIT} entries')
            logging.debug(f'Item {unique_identifier} entries')


def main():
    # df_tv_shows = cl.tv_show('year', '2002')
    # print(df_tv_shows.columns)
    # db1.populate_tables_tv_shows(df_tv_shows)
    # df_game = cl.game('year', '1996')
    # df_game = df_game.replace(np.nan, "missing", regex=True)
    db1 = Database()
    assert db1.db_name == 'metacritic'
    db1.connect_to_db()
    db1.create_db()
    db1.create_tables_db()
    # db1.populate_tables_games(df_game)
    print(db1.db_name)


if __name__ == '__main__':
    main()
