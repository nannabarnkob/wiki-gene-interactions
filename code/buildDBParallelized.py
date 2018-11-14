#! /usr/bin/env python

from BloomFunctions import BloomFunctions
from WikiXmlHandler import WikiXmlHandler
import sqlite3
import xml.sax
import subprocess
import mwparserfromhell
import datetime
import os
from multiprocessing import Pool
import sys


class BuildDataBase:
    def __init__(self, wikifolder):
        self.partitions = [wikifolder + x for x in os.listdir(wikifolder)][0:10]

    def connect_database(self, databaseName):
        self.db = sqlite3.connect(databaseName)
        self.cursor = self.db.cursor()

    def close(self):
        self.db.close()

    def main(self):
        self.bloomfilter = BloomFunctions('../data/gene_symbol_list.txt')
        # self.parallelize()

    def process_wiki(self, wikipath):
        # maybe include option for which process_article we should use as this is passed as a callback function to the handler object
        # Object for handling xml, pass on the self.process_article function as how to process each page
        """
        In computer programming, a callback, also known as a "call-after[1]" function, is any executable code that is 
        passed as an argument to other code that is expected to call back (execute) the argument at a given time. 
        This execution may be immediate as in a synchronous callback, or it might happen at a later time as in an 
        asynchronous callback. In all cases, the intention is to specify a function or subroutine as an entity that is, 
        depending on the language, more or less similar to a variable (see first-class functions). 
        """

        handler = WikiXmlHandler(self.process_article)

        # Parsing object
        parser = xml.sax.make_parser()
        parser.setContentHandler(handler)

        # Iterate through compressed file one line at a time
        print("Begin reading in Wiki at", datetime.datetime.now())
        for line in subprocess.Popen(['bzcat'],
                                     stdin=open(wikipath),
                                     stdout=subprocess.PIPE).stdout:
            parser.feed(line)

        print("End reading in Wiki at", datetime.datetime.now())

    def parallelize(self):
        # Create a pool of workers to execute processes
        pool = Pool(processes=10)

        # Map (service, tasks), applies function to each partition

        pool.map(self.process_wiki, self.partitions)
        pool.close()
        pool.join()

    def process_article(self, title, text):
        """Process a wikipedia article """

        if self.bloomfilter.classify(title):
            # Create a parsing object
            wikicode = mwparserfromhell.parse(text)

            # Find the wikilinks
            wikilinks = [x.title.strip_code().strip()
                         for x in wikicode.filter_wikilinks()]

            #passed_links = [str(x) for x in wikilinks if self.bloomfilter.classify(str(x))]
            passed_links = [wikilinks[i] for i in range(
                len(wikilinks)) if self.bloomfilter.classify(str(wikilinks[i]))]
            return passed_links

    def process_article_with_set_lookup(self, title, text):
        """Process a wikipedia article with set look-up"""

        if self.bloomfilter.classify(title):
            # Create a parsing object
            wikicode = mwparserfromhell.parse(text)

            # Find the wikilinks
            wikilinks = [x.title.strip_code().strip()
                         for x in wikicode.filter_wikilinks()]
            passed_links = [wikilinks[i] for i in range(
                len(wikilinks)) if self.bloomfilter.classify(str(wikilinks[i]))]
            return passed_links


# wikifolder = '/Volumes/Seagate Backup Plus Drive/Wikipedia_partitions/'
wikifolder = sys.argv[1]
database = BuildDataBase(wikifolder)
database.main()
database.parallelize()
#data_path = '/Volumes/Seagate Backup Plus Drive/Wikipedia/enwiki-20181101-pages-articles-multistream.xml.bz2'
# handler = database.process_wiki(data_path)
