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
# refuses to save other than in ~/.keras/datasets
saved_file_path = get_file('Volumes/Seagate\ Backup\ Plus\
                           Drive/Wikipedia_partitions/enwiki-20181101-pages-articles-multistream.xml.bz2', url)
# file is now placed in ~/.keras/datasets/ - wait and see
data_path = '~/.keras/datasets/enwiki-20181101-pages-articles-multistream.xml.bz2'
# From here not tested (waiting for download)


all_paths = ['/enwiki/20181101/enwiki-20181101-pages-articles1.xml-p10p30302.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles2.xml-p30304p88444.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles3.xml-p88445p200507.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles4.xml-p200511p352689.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles5.xml-p352690p565312.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles6.xml-p565314p892912.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles7.xml-p892914p1268691.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles8.xml-p1268693p1791079.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles9.xml-p1791081p2336422.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles10.xml-p2336425p3046511.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles11.xml-p3046517p3926861.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles12.xml-p3926864p5040435.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles13.xml-p5040438p6197593.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles14.xml-p6197599p7697599.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles14.xml-p7697599p7744799.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles15.xml-p7744803p9244803.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles15.xml-p9244803p9518046.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles16.xml-p9518059p11018059.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles16.xml-p11018059p11539266.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles17.xml-p11539268p13039268.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles17.xml-p13039268p13693066.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles18.xml-p13693075p15193075.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles18.xml-p15193075p16120541.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles19.xml-p16120548p17620548.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles19.xml-p17620548p18754723.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles20.xml-p18754736p20254736.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles20.xml-p20254736p21222156.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles21.xml-p21222161p22722161.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles21.xml-p22722161p23927980.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles22.xml-p23927984p25427984.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles22.xml-p25427984p26823658.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles23.xml-p26823661p28323661.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles23.xml-p28323661p29823661.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles23.xml-p29823661p30503448.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles24.xml-p30503454p32003454.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles24.xml-p32003454p33503454.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles24.xml-p33503454p33952815.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles25.xml-p33952817p35452817.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles25.xml-p35452817p36952817.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles25.xml-p36952817p38067198.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles26.xml-p38067204p39567204.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles26.xml-p39567204p41067204.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles26.xml-p41067204p42567204.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles26.xml-p42567204p42663461.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles27.xml-p42663464p44163464.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles27.xml-p44163464p45663464.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles27.xml-p45663464p47163464.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles27.xml-p47163464p48663464.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles27.xml-p48663464p50163464.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles27.xml-p50163464p51663464.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles27.xml-p51663464p53163464.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles27.xml-p53163464p54663464.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles27.xml-p54663464p56163464.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles27.xml-p56163464p57663464.bz2',
                 '/enwiki/20181101/enwiki-20181101-pages-articles27.xml-p57663464p58936625.bz2']

save_to = 'Volumes/Seagate\ Backup\ Plus\ Drive/Wikipedia_partitions/'
url = 'http://google.com/favicon.icon'
base = 'https://dumps.wikimedia.org'
for url in all_paths:
    full_path = base + url
    name = save_to + url.split('/')[-1]
    r = requests.get(full_path, allow_redirects=True)
    open(name, 'w').write(r.content)
