from bs4 import BeautifulSoup
import time
import grequests
import requests
import config as cfg
import logging
import sys
import re

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Create Formatter
formatter = logging.Formatter('%(asctime)s-FILE:%(filename)s-FUNC:%(funcName)s-LINE:%(lineno)d-%(message)s')

# create a file handler and add it to logger
file_handler = logging.FileHandler('web_scraper.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


class Scraper:
    def __init__(self, anchor_url):
        # URLs for index pages off anchor page.
        self.index_url_list = []
        # URLs from each index page. These are the items that will be scraped.
        self.url_list = []
        # Methods for initialisation of the scraper
        # Creates the list of index pages needed to get item URLs.
        self.get_index_pages_urls_list(anchor_url)
        # Creates the list of the URL for each item to be scraped.
        self.get_item_urls_list()
        # self.parallel_page_scraper()

    def get_index_pages_urls_list(self, input_url):
        """
            Receives a url.
            Returns all the urls for items on that index page.
            :param input_url: URL
            """
        try:
            source = requests.get(input_url, headers=cfg.USER_AGENT).text
        except IOError:
            logging.critical(f'Incorrect anchor url to scrape.')
            sys.exit(1)
        soup = BeautifulSoup(source, 'lxml')
        # Loops through each item on the index page and extracts the url of the item into a list.
        try:
            self.index_url_list.append(input_url)
            for article in soup.find_all('a', class_='page_num', href=True):
                # Every url extracted is relative - without the main page
                self.index_url_list.append(cfg.MAIN_WEB_PAGE + article['href'])
            print(self.index_url_list)
        except IOError:
            logging.critical(f'Unable created a list of the index pages urls.')
        logging.info(f'Successfully created a list of the index pages urls.')

    def get_item_urls_list(self):
        """
            For each index page url in self.index_url_list, scrapes to extract the url for
            each item to be included in scrape.
            """
        for url in self.index_url_list:
            try:
                source = requests.get(url, headers=cfg.USER_AGENT).text
            except IOError:
                logging.critical(f'Incorrect url input to scrape')
                sys.exit(1)
            soup = BeautifulSoup(source, 'lxml')
            # Loops through each item on the index page and extracts the url of the item into a list.
            list_of_names = []  # This is mostly for games due to multi-platforms.
            try:
                for article in soup.find_all('a', class_='title', href=True):
                    # Every url extracted is relative - without the main page
                    if article.text not in list_of_names:
                        self.url_list.append(cfg.MAIN_WEB_PAGE + article['href'])
                        list_of_names.append(article.text)
            except IOError:
                logging.critical(f'Unable to create url list to scrape, from this index page.')
        print(self.url_list)
        logging.info(f'Successfully created list of urls for items to scrape.')

    def concurrent_page_scraping(self):
        """
        calls the debug_data_scraper method and scrapes each page one at a time.
        :return:
        """
        for index, url in enumerate(self.url_list):
            self.debug_data_scraper(url)

    def debug_data_scraper(self, input_url):  # TODO: this is for tests on on other types of web scrapers
        """
        Scrapes a specific item.
        :param input_url:
        """
        item_info = {}
        logging.info(f'Scraping a url')
        logging.debug(f'The url scraped is : {input_url}')
        try:
            source = requests.get(input_url, headers=cfg.USER_AGENT).text
            soup = BeautifulSoup(source, 'lxml')
            critic_meta_score_expression = re.compile('metascore_w larger')  # expression to find the meta score
            for critic_ttl_score in soup.find_all('span', class_=critic_meta_score_expression, limit=1):
                item_info['Metascore'] = critic_ttl_score.text
            user_score_expression = re.compile('metascore_w user')  # expression to find the user score
            for user_ttl_score in soup.find_all('span', class_=user_score_expression, limit=1):
                item_info['User score'] = user_ttl_score.text
            for title in soup.find_all('h1'):
                item_info['Title'] = title.text
            # TODO:the year loop need to fix to take only second element
            for release_year in soup.find('li', class_='summary_detail release_data').find_all('span', class_='data'):
                item_info['Release Year'] = release_year.text
            for studio in soup.find('li', class_='summary_detail developer').find_all('a', class_='button'):
                item_info['Studio'] = studio.text
            # TODO: Need to check multiple creators
            platform_list = []
            for platform in soup.find('span', class_='platform').find_all('a'):
                platform_list.append(platform.text.strip())
            for other_platform in soup.find_all('li', class_='summary_detail product_platforms'):
                for second_platform in other_platform.find_all('a', class_='hover_none'):
                    platform_list.append(second_platform.text.strip())
            item_info['Platform'] = ', '.join(platform_list)
            genre_list = []
            for multi_genre in soup.find_all('li', class_='summary_detail product_genre'):
                for genre in multi_genre.find_all('span', class_='data'):
                    genre_list.append(genre.text.strip())
            item_info['Genres'] = ', '.join(genre_list)
            for rating in soup.find_all('li', class_='summary_detail product_rating'):
                item_info['Rating'] = rating.find('span', class_='data').text
            for summary in soup.find_all('li', class_='summary_detail product_summary'):
                if summary.find('span', class_='blurb blurb_expanded') is None:  # for game with a short summary
                    item_info['Summary'] = summary.find('span', class_='data').text.strip('\n')
                else:
                    item_info['Summary'] = summary.find('span', class_='blurb blurb_expanded').text
            print(item_info)
        except IOError:
            logging.error(f'unable to find page to scrape url incorrect!')

    def parallel_movie_scraper(self):
        """
        For each url in self.url_list, this method extracts all info
        required for each movie item. Sends a batch to be extracted using grequests.
        :return:
        """
        item_info = {}
        page = (grequests.get(u, headers=cfg.USER_AGENT) for u in self.url_list)
        response = grequests.map(page, size=cfg.BATCH_SIZE)
        logging.info(f'Successfully created url batch list.')
        for res in response:
            try:
                soup = BeautifulSoup(res.content, 'lxml')
                critic_meta_score_expression = re.compile('metascore_w larger')  # expression to find the meta score
                for critic_ttl_score in soup.find_all('span', class_=critic_meta_score_expression, limit=1):
                    item_info['Metascore'] = critic_ttl_score.text
                user_score_expression = re.compile('metascore_w user')  # expression to find the user score
                for user_ttl_score in soup.find_all('span', class_=user_score_expression, limit=1):
                    item_info['User score'] = user_ttl_score.text
                for title in soup.find_all('h1'):
                    item_info['Title'] = title.text
                release_year_expression = re.compile('release_year')  # expression to find the largest score
                for release_year in soup.find_all('span', class_=release_year_expression, limit=1):
                    item_info['Release Year'] = release_year.text
                studio_expression = re.compile('/company')  # expression to find the studio
                for studio in soup.find_all('a', href=studio_expression, limit=1):
                    item_info['Studio'] = studio.text
                for director in soup.find_all('div', class_='director', limit=1):
                    item_info['Director'] = list(list(director.children)[3].children)[0].text
                for rating in soup.find_all('div', class_='rating', limit=1):
                    item_info['Rating'] = list(rating.children)[3].text.strip()
                for runtime in soup.find_all('div', class_='runtime', limit=1):
                    item_info['Runtime'] = list(runtime.children)[3].text
                for summary in soup.find_all('div', class_='summary_deck details_section', limit=1):
                    item_info['Summary'] = list(list(summary.children)[3].children)[1].text
                print(item_info)
               #  TODO: Add Genre.
            except AttributeError:
                logging.error(f'Unable to send batch to scrape.')
                continue
            logging.info(f'Scraping a batch of item urls.')

    def parallel_tv_show_scraper(self):
        """
        For each url in self.url_list, this method extracts all info
        required for each TV show item. Sends a batch to be extracted using grequests.
        :return:
        """
        item_info = {}
        page = (grequests.get(u, headers=cfg.USER_AGENT) for u in self.url_list)
        response = grequests.map(page, size=cfg.BATCH_SIZE)
        logging.info(f'Successfully created url batch list')
        for res in response:
            try:
                soup = BeautifulSoup(res.content, 'lxml')
                critic_meta_score_expression = re.compile('metascore_w larger')  # expression to find the meta score
                for critic_ttl_score in soup.find_all('span', class_=critic_meta_score_expression, limit=1):
                    item_info['Metascore'] = critic_ttl_score.text
                user_score_expression = re.compile('metascore_w user')  # expression to find the user score
                for user_ttl_score in soup.find_all('span', class_=user_score_expression, limit=1):
                    item_info['User score'] = user_ttl_score.text
                for title in soup.find_all('h1'):
                    item_info['Title'] = title.text
                # TODO:the year loop need to fix to take only second element
                for release_year in soup.find('span', class_='release_date').find_all('span'):
                    item_info['Release Year'] = release_year.text
                studio_expression = re.compile('/company')  # expression to find the studio
                for studio in soup.find_all('a', href=studio_expression, limit=1):
                    item_info['Studio'] = studio.text
                # TODO: Need to check multiple creators
                for creator in soup.find_all('div', class_='creator', limit=1):
                    item_info['Creator'] = list(list(creator.children)[3].children)[0].text
                genre_list = []
                for index, genre in enumerate(soup.find('div', class_='genres').find_all('span')):
                    if index != 0 and index != 1:  # this returns only a list of the genres of each tv show
                        genre_list.append(genre.text.strip())
                item_info['Genres'] = genre_list
                starring_list = []
                for index, starring in enumerate(soup.find_all('div', class_='summary_cast details_section')):
                    for name in starring.find_all('a'):
                        starring_list.append(name.text.strip())
                item_info['Starring'] = starring_list
                for summary in soup.find_all('div', class_='summary_deck details_section', limit=1):
                    item_info['Summary'] = list(list(summary.children)[3].children)[1].text
                print(item_info)
            except AttributeError:
                logging.error(f'Unable send batch to scrape.')
                continue
            logging.info(f'Scraping a batch of urls.')

    def parallel_game_scraper(self):
        """
        For each url in self.url_list, this method extracts all info
        required for each TV show item. Sends a batch to be extracted using grequests.
        :return:
        """
        item_info = {}
        page = (grequests.get(u, headers=cfg.USER_AGENT) for u in self.url_list)
        response = grequests.map(page, size=cfg.BATCH_SIZE)
        logging.info(f'Successfully created url batch list')
        for res in response:
            try:
                soup = BeautifulSoup(res.content, 'lxml')
                critic_meta_score_expression = re.compile('metascore_w larger')  # expression to find the meta score
                for critic_ttl_score in soup.find_all('span', class_=critic_meta_score_expression, limit=1):
                    item_info['Metascore'] = critic_ttl_score.text
                user_score_expression = re.compile('metascore_w user')  # expression to find the user score
                for user_ttl_score in soup.find_all('span', class_=user_score_expression, limit=1):
                    item_info['User score'] = user_ttl_score.text
                for title in soup.find_all('h1'):
                    item_info['Title'] = title.text
                # TODO:the year loop need to fix to take only second element
                for release_year in soup.find('li', class_='summary_detail release_data').find_all('span',
                                                                                                   class_='data'):
                    item_info['Release Year'] = release_year.text
                for studio in soup.find('li', class_='summary_detail developer').find_all('a', class_='button'):
                    item_info['Studio'] = studio.text
                # TODO: Need to check multiple creators
                platform_list = []
                for platform in soup.find('span', class_='platform').find_all('a'):
                    platform_list.append(platform.text.strip())
                for other_platform in soup.find_all('li', class_='summary_detail product_platforms'):
                    for second_platform in other_platform.find_all('a', class_='hover_none'):
                        platform_list.append(second_platform.text.strip())
                item_info['Platform'] = platform_list
                genre_list = []
                for multi_genre in soup.find_all('li', class_='summary_detail product_genre'):
                    for genre in multi_genre.find_all('span', class_='data'):
                        genre_list.append(genre.text.strip())
                item_info['Genres'] = genre_list
                for rating in soup.find_all('li', class_='summary_detail product_rating'):
                    item_info['Rating'] = rating.find('span', class_='data').text
                for summary in soup.find_all('li', class_='summary_detail product_summary'):
                    if summary.find('span', class_='blurb blurb_expanded') is None:  # for game with a short summary
                        item_info['Summary'] = summary.find('span', class_='data').text.strip('\n')
                    else:
                        item_info['Summary'] = summary.find('span', class_='blurb blurb_expanded').text
                print(item_info)
            except AttributeError:
                logging.error(f'unable send batch to scrape')
                continue
            logging.info(f'scraping a batch of urls')


def main():
    the_scraper_game = Scraper(cfg.EXAMPLE_WEB_PAGE_GAMES)
    the_scraper_tv = Scraper(cfg.EXAMPLE_WEB_PAGE_TV_SHOWS)
    the_scraper_movie = Scraper(cfg.EXAMPLE_WEB_PAGE_MOVIE)
    part_4_seconds_before = time.time()
    part_1_seconds_before = time.time()
    the_scraper_tv.parallel_tv_show_scraper()
    part_1_seconds_after = time.time()
    part_2_seconds_before = time.time()
    the_scraper_game.parallel_game_scraper()
    part_2_seconds_after = time.time()
    part_3_seconds_before = time.time()
    the_scraper_movie.parallel_movie_scraper()
    part_3_seconds_after = time.time()
    part_4_seconds_after = time.time()
    print(f"part tv time :{part_1_seconds_after - part_1_seconds_before}")
    print(f"part game time :{part_2_seconds_after - part_2_seconds_before}")
    print(f"part movie time :{part_3_seconds_after - part_3_seconds_before}")
    print(f"part total time :{part_4_seconds_after - part_4_seconds_before}")


if __name__ == '__main__':
    main()


