from bs4 import BeautifulSoup
import requests
import time
import random as rand
import csv
MAIN_WEB_PAGE = 'https://www.metacritic.com'
example_web_page = 'https://www.metacritic.com/browse/movies/score/metascore/year' \
                   '/filtered?year_selected=2017&sort=desc&view=detailed'


def pages_urls(input_url):
    """
    Receives a url which is a group of a Items(movies, games etc) and returns all the urls from that list.
    :param input_url: URL
    :return: List of URLS
    """
    user_agent = {'User-agent': 'Mozilla/5.0'}
    source = requests.get(input_url, headers=user_agent).text
    soup = BeautifulSoup(source, 'lxml')
    # csv_file = open('end_page_scrape.csv', 'w')
    # csv_writer = csv.writer(csv_file)
    # csv_writer.writerow([])
    url_list = []
    for article in soup.find_all('a', class_='title', href=True):
        url_list.append(MAIN_WEB_PAGE + article['href'])  # every url is saved without the main page, we are adding it

    return url_list


def search_pages_urls(input_url):
    """
    receives the first page of a group of urls (e.g. movies 2017) and finds the amount of pages in the group and returns
    a list of the movies in this group as a list of urls.(calls function pages_urls for the list)
    :param input_url: URLS
    :return: List of URLS
    """
    user_agent = {'User-agent': 'Mozilla/5.0'}
    source = requests.get(input_url, headers=user_agent).text
    soup = BeautifulSoup(source, 'lxml')
    pages_url_list = [input_url]
    output_url_list = []
    for article in soup.find_all('a', class_='page_num', href=True):
        pages_url_list.append(MAIN_WEB_PAGE + article['href'])  # every url is saved without the main page
    for url in pages_url_list:
        time.sleep(rand.randint(3, 5))  # add a random sleep between each request just in case we won't be blocked
        output_url_list += pages_urls(url)
    return output_url_list


def main():
    print(search_pages_urls(example_web_page))


if __name__ == '__main__':
    main()

