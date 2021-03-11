![logo](https://upload.wikimedia.org/wikipedia/commons/4/48/Metacritic_logo.svg)
# _Metacritic web Scraper_

### Welcome to the metacritic web scraper

This program scrapers data about Movies, Tv Shows and Games from the website
[www.metacritic.com](https://www.metacritic.com/)

### Features

- The program runs from a CLI, and you can specifiy what Data to scrape.(Movies, Tv Shows, Games)
- filter each scrape by Year or by Genre.
- 
- Drag and drop markdown and HTML files into Dillinger
- Export documents as Markdown, HTML and PDF
### Data
- Meta Score by critics and by users.
- Release Date
- Studio
- Genre
- Summary
- Aditional Data by type of scrape (e.g. Movies also scrapes Director of movie.)


## Installation

The Scraper requires certain packages to run and all must be installed via the requirements.txt  file.
```sh
pip3 --user install requirements.txt
```
After installion of requirements.txt, file in the config.py update the mysql user and password.

## Example
To run Scraper run the CLI.py.

```sh
python3 CLI.py movies year 1927
python3 CLI.py games year 2002
python3 CLI.py tv genre action
```

## Database
![Test Image 6](https://github.com/DanOren/Web_Scraping_Project_ITC/blob/main/ERD_metacritic.jpeg?raw=true)
**games:**
**Table to house each game object**
- id – INT: unique identifier for a game
- name – VARCHAR: the name of the game
- unique_identifier - VARCHAR: the unique identifier used in the python script – it is ‘name’ +’_’ + ‘release_date’
- meta_score – VARCHAR – the Metascore for the game
- user_score – VARCHAR: the average user rating for the game
- release_date – VARCHAR: the release date of the game
- rating – VARCHAR: the parental rating for the game
- summary – VARCHAR: the summary of the game
- studio_id – INT: foreign key from studios table

**tv_shows:**
**Table to house each tv_show object**
- id – INT: unique identifier for a tv_show
- name – VARCHAR: the name of the tv_show
- unique_identifier - VARCHAR: the unique identifier used in the python script – it is ‘name’ +’_’ + ‘release_date’
- meta_score – VARCHAR – the Metascore for the tv_show
- user_score – VARCHAR: the average user rating for the tv_show
- release_date – VARCHAR: the release date of the tv_show
- summary – VARCHAR: the summary of the tv_show
- platform_id – INT: foreign key from platforms table
- studio_id – INT: foreign key from studios table
- creator_id – INT: foreign key from creators table

**movies:**
**Table to house each movie object**
- id – INT: unique identifier for a movie
- name – VARCHAR: the name of the movie
- unique_identifier - VARCHAR: the unique identifier used in the python script – it is ‘name’ +’_’ + ‘release_date’
- meta_score – VARCHAR – the Metascore for the movie
- user_score – VARCHAR: the average user rating for the movie
- release_year – VARCHAR: the release year of the movie
- rating – VARCHAR: the parental rating for the movie
- runtime – VARCHAR: the runtime of the movie, in minutes
- summary – VARCHAR: the summary of the movie
- studio_id– INT: foreign key from studios table
- director_id– INT: foreign key from directors table

**studios:**
**Table to house each studio, for each games, tv_shows and movies**
- id – INT: unique identifier for a studio
- name – VARCHAR: the name of the studio

**creators:**
**Table to house each platform for creators for tv_shows**
- id – INT: unique identifier for a creator
- name – VARCHAR: the name of the creator

**directors:**
**Table to house each director**
- id – INT: unique identifier for a director
- name – VARCHAR: the name of the director

**genres:**
**Table to house each genre that games, tv_shows and movies can be classified as**
- id – INT: unique identifier for a genre
- name – VARCHAR: the name of the genre

**games_genres:**
**Associative table to house the id and genre id that each game is classified as**
- id – INT: unique identifier for games_genres entry
- game_id – INT: foreign key from games table
- genre_id – INT: foreign key from genres table

**tv_shows_genres:**
**Associative table to house the id and genre id that each tv_show is classified as**
- id – INT: unique identifier for tv_shows_genres entry
- game_id – INT: foreign key from tv_shows table
- genre_id – INT: foreign key from genres table

**movies_genres:**
**Associative table to house the id and genre id that each movie is classified as**
- id – INT: unique identifier for movies_genres entry
- game_id – INT: foreign key from movies table
- genre_id – INT: foreign key from genres table

**platforms:**
**Table to house each platform that a game has been released on e.g. PS, PC**
- id – INT: unique identifier for a platform
- name – VARCHAR: the name of the platform

**games_platforms:**
**Associative table to house the id and platform id that each game has been released on**
- id – INT: unique identifier for games_platforms entry
- game_id – INT: foreign key from games table
- platform_id – INT: foreign key from platforms table
###### by Ari and Dan
