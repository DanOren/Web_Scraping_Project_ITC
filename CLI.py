import argparse
import URL_scraper as Scraper
import config as cfg
import sys
import DB as DB

parser = argparse.ArgumentParser(description='Welcome to Metacritic scraper. Please enter 3 parameters: '
                                             '1. Which type of data to scrape, '
                                             '2. What method to scrape it by, '
                                             '3. The parameter to scrape by. ')
parser.add_argument('type_to_scrape', nargs='?', type=str,
                    help='movies, tv_shows or games', default='movies')
parser.add_argument('how_to_scrape', nargs='?', type=str,
                    help='Scrape by year or genre', default='year')
parser.add_argument('val_to_scrape', nargs='?', type=str,
                    help='Value to scrape by', default='1927')
args, unknown = parser.parse_known_args()


def movie(how_to_scrape, val_to_scrape):
    """
    Command to scrape a url that is based on movies.
    """
    url = ''
    if how_to_scrape == 'year':
        url = f"https://www.metacritic.com/browse/movies/score/metascore/year/filtered?" \
              f"year_selected={val_to_scrape}&sort=desc&view=detailed"
    elif how_to_scrape == 'genre':
        url = f'https://www.metacritic.com/browse/movies/genre/metascore/{val_to_scrape}?view=detailed'
    the_scraper = Scraper.Scraper(url)
    the_scraper.parallel_movie_scraper()
    the_scraper.replace_nan_with_null()
    data = DB.Database()
    data.add_to_database_by_type(container=the_scraper.get_container(), container_type=the_scraper.get_type())


def tv_show(how_to_scrape, val_to_scrape):
    """
    Command to scrape a url that is based on tv shows.
    """
    url = ''
    if how_to_scrape == 'year':
        url = 'https://www.metacritic.com/browse/tv/score/metascore/year/filtered?' \
              f'year_selected={val_to_scrape}&sort=desc&view=detailed'
    elif how_to_scrape == 'genre':
        url = f'https://www.metacritic.com/browse/tv/genre/metascore/{val_to_scrape}?view=detailed'
    the_scraper = Scraper.Scraper(url)
    the_scraper.parallel_tv_show_scraper()
    the_scraper.replace_nan_with_null()
    data = DB.Database()
    data.add_to_database_by_type(container=the_scraper.get_container(), container_type=the_scraper.get_type())


def game(how_to_scrape, val_to_scrape):
    """
    Command to scrape a url that is based on games.
    """
    url = ''
    if how_to_scrape == 'year':
        url = 'https://www.metacritic.com/browse/games/score/metascore/year/all/filtered?' \
              f'year_selected={val_to_scrape}&distribution=&sort=desc&view=detailed'
    elif how_to_scrape == 'genre':
        url = f'https://www.metacritic.com/browse/games/genre/metascore/{val_to_scrape}/all?view=detailed'
    the_scraper = Scraper.Scraper(url)
    the_scraper.parallel_game_scraper()
    the_scraper.replace_nan_with_null()
    data = DB.Database()
    data.add_to_database_by_type(container=the_scraper.get_container(), container_type=the_scraper.get_type())


commands = {
            'movies': movie,
            'tv_shows': tv_show,
            'tv': tv_show,
            'games': game}


def check_input():
    """
    Checks if the user input is correct
    """
    if args.type_to_scrape is None or args.how_to_scrape is None \
            or args.val_to_scrape is None and len(args) != 3:
        raise IOError('Not enough input parameters!')
    if len(unknown) > 0:
        raise IOError('Not enough input parameters!')
    if args.type_to_scrape not in ['movies', 'tv_shows', 'games', 'tv']:
        raise IOError(f'Unknown command {args.type_to_scrape}')
    if args.how_to_scrape not in ['year', 'genre']:
        raise IOError(f'Unknown scrape parameter {args.how_to_scrape}')
    if args.how_to_scrape == 'year' and not args.val_to_scrape.isdigit():
        raise IOError(f'Incorrect year scrape parameter {args.val_to_scrape}')
    if args.how_to_scrape == 'genre' and args.val_to_scrape not in cfg.GENRE_LIST:
        raise IOError(f'Incorrect genre scrape parameter {args.val_to_scrape}')
    if len(args.val_to_scrape) < 4:
        raise ValueError(f'incorrect year entered {args.val_to_scrape} please enter a 4 digit year')


def main():
    try:
        check_input()
    except Exception as e:
        print(e)
        sys.exit(1)
    commands[args.type_to_scrape](args.how_to_scrape, args.val_to_scrape)


if __name__ == '__main__':
    main()
