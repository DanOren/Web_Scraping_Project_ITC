import argparse
import URL_scraper as sc
import config as cfg
import sys
filename_error = "command_calculator.py: error: "
parser = argparse.ArgumentParser(description='Welcome to Metacritic scraper, Please enter 3 parameters,'
                                             'Which type of data to scrape, by what method to scrape it by,'
                                             'and the parameter to scrape by.')
parser.add_argument('type_to_scrap', nargs='?', type=str, help='Movies,Tv Shows or Games', default=None)
parser.add_argument('how_to_scrape', nargs='?', type=str, help='Scrape by year or genre', default=None)
parser.add_argument('val_to_scrape', nargs='?', type=str, help='Value to scrape by', default=None)
args = parser.parse_args()


def movie(how_to_scrape, val_to_scrape):
    """

    """
    url = ''
    if how_to_scrape == 'year':
        url = f"https://www.metacritic.com/browse/movies/score/metascore/year/filtered?" \
              f"year_selected={val_to_scrape}&sort=desc&view=detailed"
    elif how_to_scrape == 'genre':
        url = f'https://www.metacritic.com/browse/movies/genre/metascore/{val_to_scrape}?view=detailed'
    the_scraper = sc.Scraper(url)
    the_scraper.parallel_movie_scraper()


def tv_show(how_to_scrape, val_to_scrape):
    """

    """
    url = ''
    if how_to_scrape == 'year':
        url = 'https://www.metacritic.com/browse/tv/score/metascore/year/filtered?' \
              f'year_selected={val_to_scrape}&sort=desc&view=detailed'
    elif how_to_scrape == 'genre':
        url = f'https://www.metacritic.com/browse/tv/genre/metascore/{val_to_scrape}?view=detailed'
    the_scraper = sc.Scraper(url)
    the_scraper.parallel_tv_show_scraper()


def game(how_to_scrape, val_to_scrape):
    """

    """
    url = ''
    if how_to_scrape == 'year':
        url = 'https://www.metacritic.com/browse/games/score/metascore/year/all/filtered?' \
              f'year_selected={val_to_scrape}&distribution=&sort=desc&view=detailed'
    elif how_to_scrape == 'genre':
        url = f'https://www.metacritic.com/browse/games/genre/metascore/{val_to_scrape}/all?view=detailed'
    the_scraper = sc.Scraper(url)
    the_scraper.parallel_game_scraper()


commands = {
            'movies': movie,
            'tv_show': tv_show,
            'games': game}


def valid_length(the_args):
    """
    Checks if there are enough arguments from the user
    :param the_args: args
    """
    if the_args.type_to_scrap is None or the_args.how_to_scrape is None or the_args.val_to_scrape is None:
        raise IOError('Not enough input parameters!')


def check_input():
    """
    Prints to the screen if the user added optional argument
    :return:
    """

    valid_length(args)
    if args.type_to_scrap not in ['movies', 'tv_show', 'games']:
        raise IOError(f'Unknown command {args.type_to_scrap}')
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
        flag = check_input()
    except Exception as e:
        print(e)
        sys.exit(1)
    commands[args.type_to_scrap](args.how_to_scrape, args.val_to_scrape)


if __name__ == '__main__':
    main()


