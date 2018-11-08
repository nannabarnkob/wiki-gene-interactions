#! /usr/bin/env python
# Downloading wiki data

# Looking at the WikiDump site using requests and bs4

import requests
# Library for parsing HTML
from bs4 import BeautifulSoup
base_url = 'https://dumps.wikimedia.org/enwiki/'
index = requests.get(base_url).text
soup_index = BeautifulSoup(index, 'html.parser')
# Find the links on the page
dumps = [a['href'] for a in soup_index.find_all('a') if
         a.has_attr('href')]
print(dumps)

# Working with html data can be done with BeautifulSoup
dump_url = base_url + '20181101/'
# Retrieve the html
dump_html = requests.get(dump_url).text
# Convert to a soup
soup_dump = BeautifulSoup(dump_html, 'html.parser')
# Find list elements with the class file
soup_dump.find_all('li', {'class': 'file'})[:3]
# Now for the downloading part
# Was necessary for me to be able to download
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

from keras.utils import get_file
print("Now downloading...")
url = 'https://dumps.wikimedia.org/enwiki/20181101/enwiki-20181101-pages-articles-multistream.xml.bz2'

saved_file_path = get_file('enwiki-20181101-pages-articles-multistream.xml.bz2', url)
# file is now placed in ~/.keras/datasets/
data_path ='~/.keras/datasets/enwiki-20181101-pages-articles-multistream.xml.bz2'
# From here not tested (waiting for download)
# Iterate through compressed file one line at a time

"""
for line in subprocess.Popen(['bzcat'],
                             stdin=open(data_path),
                             stdout=subprocess.PIPE).stdout:
    # process line
"""
