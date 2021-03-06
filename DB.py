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

# Create a file handler and add it to logger
file_handler = logging.FileHandler('web_scraper.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

class Database:
    def __init__(self):
