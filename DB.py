from bs4 import BeautifulSoup
import time
import grequests
import requests
import config as cfg
import logging
import sys
import re
import pymysql

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Create formatter
# formatter = logging.Formatter('%(asctime)s-FILE:%(filename)s-FUNC:%(funcName)s-LINE:%(lineno)d-%(message)s')
#
# Create a file handler and add it to logger.
# file_handler = logging.FileHandler('web_scraper.log')
# file_handler.setLevel(logging.DEBUG)
# file_handler.setFormatter(formatter)
# logger.addHandler(file_handler)
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
        self.cur = self.connect_to_db()
        # self.cur updated after DB confirmed/created
        self.cur = self.create_DB()

    def connect_to_db(self):

        # Create initial connection object.
        con = pymysql.connect(host='localhost', user='root', password=cfg.PASSWORD_DB_SERVER, cursorclass=pymysql.cursors.DictCursor)
        # Create initial cursor
        cur = con.cursor()
        return cur

    def create_DB(self):

        # Confirm/create DB
        query = f"CREATE DATABASE IF NOT EXISTS {self.db_name}"
        self.cur.execute(query)
        # Update con with confirmed/new DB info
        con = pymysql.connect(host='localhost', user='root', password=cfg.PASSWORD_DB_SERVER, database=self.db_name, cursorclass=pymysql.cursors.DictCursor)
        cur = con.cursor()
        return cur

    def create_tables_DB(self):
        # Assembles tables as required if don't exist in self.db_name
        query = """
                CREATE TABLE IF NOT EXISTS studios (
                  id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                  name varchar(255)
                );
                
                CREATE TABLE IF NOT EXISTS directors (
                  id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                  name varchar(255)
                );
                
                CREATE TABLE IF NOT EXISTS genres (
                  id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                  name varchar(255)
                );
                
                CREATE TABLE IF NOT EXISTS creators (
                  id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                  name varchar(255)
                );
                
                CREATE TABLE IF NOT EXISTS platforms (
                  id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                  name varchar(255)
                );
                
                CREATE TABLE IF NOT EXISTS movies (
                  id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                  name varchar(255) NOT NULL,
                  unique_identifier varchar(255) NOT NULL,
                  meta_score float,
                  user_score float,
                  release_year int,
                  rating varchar(255),
                  runtime float,
                  summary varchar(10000),
                  studio_id int,
                  director_id int,
                  FOREIGN KEY(studio_id) REFERENCES studios(id),
                  FOREIGN KEY(director_id) REFERENCES directors(id)
                );
                
                
                CREATE TABLE IF NOT EXISTS movies_genres (
                  id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                  movie_id int,
                  genre_id int,
                  FOREIGN KEY(movie_id) REFERENCES movies(id),
                  FOREIGN KEY(genre_id) REFERENCES genres(id)
                );
                
                CREATE TABLE IF NOT EXISTS tv_shows (
                  id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                  name varchar(255) NOT NULL,
                  unique_identifier varchar(255) NOT NULL,
                  meta_score float,
                  user_score float,
                  release_year int,
                  rating varchar(255),
                  summary varchar(10000),
                  studio_id int,
                  creator_id int,
                  FOREIGN KEY(studio_id) REFERENCES studios(id),
                  FOREIGN KEY(creator_id) REFERENCES creators(id)
                );
                
                
                CREATE TABLE IF NOT EXISTS tv_shows_genres (
                  id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                  tv_show_id int,
                  genre_id int,
                  FOREIGN KEY(tv_show_id) REFERENCES tv_shows(id),
                  FOREIGN KEY(genre_id) REFERENCES genres(id)
                );
                
                CREATE TABLE IF NOT EXISTS games (
                  id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                  name varchar(255) NOT NULL,
                  unique_identifier varchar(255) NOT NULL,
                  meta_score float,
                  user_score float,
                  release_year int,
                  rating varchar(255),
                  summary varchar(10000),
                  studio_id int,
                  FOREIGN KEY(studio_id) REFERENCES studios(id)
                );
                
                CREATE TABLE IF NOT EXISTS games_genres (
                  id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                  game_id int,
                  genre_id int,
                  FOREIGN KEY(game_id) REFERENCES games(id),
                  FOREIGN KEY(genre_id) REFERENCES genres(id)
                );
                CREATE TABLE IF NOT EXISTS games_platforms (
                  id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                  game_id int,
                  platform_id int,
                  FOREIGN KEY(game_id) REFERENCES games(id),
                  FOREIGN KEY(platform_id) REFERENCES platforms(id)  
                );

                """
        self.cur.execute(query)

    def populate_tables_movies(self, container):
        """
        Takes in a dictionary of dictionaries. Each second-level
        dictionary contains all information for a movie item.
        :param self:
        :param container: Dictionary
        :return: None
        """
        for key, item in container.items():
            unique_identifier = key
            # Check if movie already in table
            self.cur.execute(f'SELECT id FROM movies WHERE unique_identifier={unique_identifier};')
            row = self.cur.fetchone()
            movie_id = row['id']
            if row['id'] is None:
                item_info = container[item]
                meta_score = float(item_info['Metascore'])
                user_score = float(item_info['User score'])
                title = item_info['Title']
                year = item_info['Release Year']
                studio = item_info['Studio']
                director = item_info['Director']
                genres = item_info['Genres']
                rating = item_info['Rating']
                runtime = item_info['Runtime']
                summary = item_info['Summary']
                # Check if studio in studio table
                self.cur.execute(f'SELECT id FROM studios WHERE name={studio};')
                row = self.cur.fetchone()
                if row['id'] is None:
                    # If studio not in studio table, insert it and then select the id for movies FK.
                    self.cur.execute(f'INSERT INTO studios (name) VALUES ({studio});')
                    self.cur.execute(f'SELECT id FROM studios WHERE name={studio};')
                    row = self.cur.fetchone()
                    studio_id = row['id']
                else:
                    studio_id = row['id']
                self.cur.execute(f'SELECT id FROM directors WHERE name={director};')
                row = self.cur.fetchone()
                if row['id'] is None:
                    self.cur.execute(f'INSERT INTO directors (name) VALUES ({director});')
                    self.cur.execute(f'SELECT id FROM directors WHERE name={director};')
                    row = self.cur.fetchone()
                    director_id = row['id']
                else:
                    director_id = row['id']
                # Insert record into movies table
                self.cur.execute(f'INSERT INTO movies (name, unique_identifier, meta_score, user_score, '
                                 f'release_year, rating, runtime, summary, studio_id, director_id) VALUES'
                                 f' ({title}, {unique_identifier}, {meta_score}, {user_score}, {year}, '
                                 f'{rating}, {runtime}, {summary}, {studio_id}, {director_id});')
                self.cur.execute(f'SELECT id FROM movies WHERE unique_identifier={unique_identifier};')
                row = self.cur.fetchone()
                movie_id = row['id']
                # Insert into movies_genres table
                for genre in genres:
                    self.cur.execute(f'SELECT id FROM genres WHERE name={genre};')
                    row = self.cur.fetchone()
                    if row['id'] is None:
                        self.cur.execute(f'INSERT INTO genres (name) VALUES ({genre});')
                        self.cur.execute(f'SELECT id FROM genres WHERE name={genre};')
                        row = self.cur.fetchone()
                        genre_id = row['id']
                    else:
                        genre_id = row['id']
                    # Insert this movie, genre combination into movies_genres table
                    self.cur.execute(f'INSERT INTO movies_genres (movie_id, genre_id) VALUES ({movie_id}, {genre_id});')
            else:
                # If movie already in movies_table, don't add.
                continue










