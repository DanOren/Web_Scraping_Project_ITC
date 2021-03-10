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

To run Scraper (example)

```sh
python3 CLI.py movies year 1927
python3 CLI.py games year 2002
python3 CLI.py tv genre action
```

## Database
![Test Image 6](https://github.com/DanOren/Web_Scraping_Project_ITC/blob/main/ERD_metacritic.jpeg?raw=true)
###### by Ari and Dan
