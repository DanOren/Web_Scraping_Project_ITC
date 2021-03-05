from bs4 import BeautifulSoup
import requests
import time
import random as rand
import re

MAIN_WEB_PAGE = 'https://www.metacritic.com'
# Anchor page and subset of items used
example_web_page = 'https://www.metacritic.com/browse/movies/score/metascore/year' \
                   '/filtered?year_selected=2017&sort=desc&view=detailed'
# Individual item used
movie_example_page = 'https://www.metacritic.com/movie/dunkirk'


def pages_urls(input_url):
    """
    Receives a url. The url as used here is an index page for items(movies, games etc).
    Returns all the urls for items on that index page.
    :param input_url: URL
    :return: List of URLS
    """
    user_agent = {'User-agent': 'Mozilla/5.0'}
    source = requests.get(input_url, headers=user_agent).text
    soup = BeautifulSoup(source, 'lxml')

    url_list = []
    # Loops through each item on the index page and extracts the url of the item into a list.
    for article in soup.find_all('a', class_='title', href=True):
        # Every url extracted is relative - without the main page.
        url_list.append(MAIN_WEB_PAGE + article['href'])

    return url_list

    # For future use
    # csv_file = open('end_page_scrape.csv', 'w')
    # csv_writer = csv.writer(csv_file)
    # csv_writer.writerow([])


def search_pages_urls(input_url):
    """
    Receives the anchor/first page of an index page for urls (e.g. subset of all movies released in 2017).
    Finds the number of and url for each subsequent index page for the specific subset of items. For each
    index page url, calls pages_urls to extract the urls of each item on that page.
    Returns a list of the url for each item in this subset of items.
    :param input_url: URLS
    :return: List of URLS
    """
    user_agent = {'User-agent': 'Mozilla/5.0'}
    source = requests.get(input_url, headers=user_agent).text
    soup = BeautifulSoup(source, 'lxml')
    # Initialise pages_url_list with the anchor page specified in example_web_page.
    pages_url_list = [input_url]
    output_url_list = []
    # Loops through each pointer to another index page and extracts the url of each index page, into a list.
    # The nth index page is labelled n-1, relative to the anchor page
    for article in soup.find_all('a', class_='page_num', href=True):
        # Every url extracted is relative - without the main page
        pages_url_list.append(MAIN_WEB_PAGE + article['href'])
    counter = 0
    print(pages_url_list)
    # Loops through each index page url. Calls page_urls to extract the url for the individual page of each item.
    for url in pages_url_list:
        # Add a random sleep between each request to ensure we don't get blocked.
        time.sleep(rand.randint(3, 5))
        output_url_list += pages_urls(url)
        print(output_url_list)
        counter += 1
        if counter == 1:
            break
    return output_url_list


def page_data_scraper(input_url):
    """
    For each url extracted by search_pages_url, this method extracts each metric/piece of information of interest.
    :param input_url: URL
    :return: Dictionary amd prints data
    """
    item_info = {}
    user_agent = {'User-agent': 'Mozilla/5.0'}
    source = requests.get(input_url, headers=user_agent).text
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
        something = director.find('a', href=True)
        item_info['Director'] = list(list(director.children)[3].children)[0].text
    for rating in soup.find_all('div', class_='rating', limit=1):
        item_info['Rating'] = list(rating.children)[3].text.strip()
    for runtime in soup.find_all('div', class_='runtime', limit=1):
        item_info['Runtime'] = list(runtime.children)[3].text
    for summary in soup.find_all('div', class_='summary_deck details_section', limit=1):
        item_info['Summary'] = list(list(summary.children)[3].children)[1].text

    print(item_info)
    return item_info


def build_database_dict(database_container, input_container):
    """
    Takes in a dictionary with all info for a specific item (movie/game etc).
    Also takes in a dictionary that houses all the items.
    Adds the item dictionary as an element in the database dictionary.
    :param input_container: Dictionary
    :param database_container: Dictionary
    :return: Dictionary
    """
    database_container = {}
    unique_identifier = input_container['Title'] + input_container['Release Year']
    database_container[unique_identifier] = input_container

    return database_container


def main():
    # print(search_pages_urls(example_web_page))
    page_data_scraper(movie_example_page)


if __name__ == '__main__':
    main()
