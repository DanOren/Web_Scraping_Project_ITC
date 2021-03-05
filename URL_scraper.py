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


class Scrapper:
    def __init__(self, input_web_page):
        self.branch_url_list = []   # list of urls that are the list of items.
        self.url_list = []          # list of urls that contain the items we want to scrape
        # methods for initialization of the scraper
        self.branch_pages_urls_list(input_web_page)
        self.end_pages_urls_list()
        # self.parallel_page_scraper()

    def branch_pages_urls_list(self, input_url):
        """
            Receives a url.
            Returns all the urls for items on that index page.
            :param input_url: URL
            """
        try:
            source = requests.get(input_url, headers=cfg.USER_AGENT).text
        except IOError:
            logging.critical(f'Incorrect branch url to scrape')
            sys.exit(1)
        soup = BeautifulSoup(source, 'lxml')
        # Loops through each item on the index page and extracts the url of the item into a list.
        try:
            self.branch_url_list.append(input_url)
            for article in soup.find_all('a', class_='page_num', href=True):
                # Every url extracted is relative - without the main page
                self.branch_url_list.append(cfg.MAIN_WEB_PAGE + article['href'])
            print(self.branch_url_list)
        except IOError:
            logging.critical(f'unable created a List of the branch urls to search for urls.')
        logging.info(f'successfully created a List of the branch urls to search for urls.')

    def end_pages_urls_list(self):
        """
            Receives a branch url.
            scrapes all the urls for items on the branch url list.
            """
        for url in self.branch_url_list:
            try:
                source = requests.get(url, headers=cfg.USER_AGENT).text
            except IOError:
                logging.critical(f'Incorrect url input to scrape')
                sys.exit(1)
            soup = BeautifulSoup(source, 'lxml')
            # Loops through each item on the index page and extracts the url of the item into a list.
            try:
                for article in soup.find_all('a', class_='title', href=True):
                    # Every url extracted is relative - without the main page
                    self.url_list.append(cfg.MAIN_WEB_PAGE + article['href'])

            except IOError:
                logging.critical(f'unable to create url list to scrape')
        print(self.url_list)
        logging.info(f'successfully created list of urls')

    def concurrent_page_scraping(self):
        """
        calls the page_data_scraper method and scrapes each page one at a time.
        :return:
        """
        for index, url in enumerate(self.url_list):
            self.movie_data_scraper(url)

    def movie_data_scraper(self, input_url):
        """
        For each url extracted by search_pages_url, this method extracts film name and director.
        works on 1 url at a time
        :param input_url:
        """
        item_info = {}
        logging.info(f'scraping a url')
        logging.debug(f'the url scrapped is : {input_url}')
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
            release_year_expression = re.compile('release_year')  # expression to find the largest score
            for release_year in soup.find_all('span', class_=release_year_expression, limit=1):
                item_info['Release Year'] = release_year.text
            studio_expression = re.compile('/company')  # expression to find the studio
            for studio in soup.find_all('a', href=studio_expression, limit=1):
                item_info['Studio'] = studio.text
            # dir_expression = re.compile('/person')
            for director in soup.find_all('div', class_='director', limit=1):
                # something = director.find('a', href=True) # TODO: delete me
                item_info['Director'] = list(list(director.children)[3].children)[0].text
            for rating in soup.find_all('div', class_='rating', limit=1):
                item_info['Rating'] = list(rating.children)[3].text.strip()
            for runtime in soup.find_all('div', class_='runtime', limit=1):
                item_info['Runtime'] = list(runtime.children)[3].text
            for summary in soup.find_all('div', class_='summary_deck details_section', limit=1):
                item_info['Summary'] = list(list(summary.children)[3].children)[1].text
            print(item_info)
        except IOError:
            logging.error(f'unable to find page to scrape url incorrect!')

    def parallel_page_scraper(self):
        """
        For each url extracted by search_pages_url, this method extracts film name and director.
        works in a batch method (sends a batch to be extracted)
        :return:
        """
        item_info = {}
        page = (grequests.get(u, headers=cfg.USER_AGENT) for u in self.url_list)
        response = grequests.map(page, size=cfg.BATCH_SIZE)
        logging.info(f'successfully created url batch list')
        for the_number, res in enumerate(response):
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
                # dir_expression = re.compile('/person')
                for director in soup.find_all('div', class_='director', limit=1):
                    # something = director.find('a', href=True) # TODO: delete me
                    item_info['Director'] = list(list(director.children)[3].children)[0].text
                for rating in soup.find_all('div', class_='rating', limit=1):
                    item_info['Rating'] = list(rating.children)[3].text.strip()
                for runtime in soup.find_all('div', class_='runtime', limit=1):
                    item_info['Runtime'] = list(runtime.children)[3].text
                for summary in soup.find_all('div', class_='summary_deck details_section', limit=1):
                    item_info['Summary'] = list(list(summary.children)[3].children)[1].text
                print(item_info)
            except AttributeError:
                logging.error(f'unable send batch to scrape')
                continue
            logging.info(f'scraping a batch of urls')


def main():
    the_scraper = Scrapper(cfg.EXAMPLE_WEB_PAGE)
    part_1_seconds_before = time.time()
    the_scraper.concurrent_page_scraping()
    part_1_seconds_after = time.time()
    part_2_seconds_before = time.time()
    the_scraper.parallel_page_scraper()
    part_2_seconds_after = time.time()
    print(f"part 1 time :{part_1_seconds_after - part_1_seconds_before}")
    print(f"part 2 time :{part_2_seconds_after-part_2_seconds_before}")


if __name__ == '__main__':
    main()
