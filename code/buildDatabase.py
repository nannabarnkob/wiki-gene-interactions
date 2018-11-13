#! /usr/bin/env python

from BloomFunctions import BloomFunctions
from WikiXmlHandler import WikiXmlHandler
import sqlite3
import xml.sax
import subprocess
import mwparserfromhell
import datetime


class BuildDataBase:
    def connect_database(self, databaseName):
        self.db = sqlite3.connect(databaseName)
        self.cursor = self.db.cursor()

    def close(self):
        self.db.close()

    def main(self):
        self.bloomfilter = BloomFunctions('../data/gene_symbol_list.txt')

    def process_wiki(self, wikipath):
        # Object for handling xml
        handler = WikiXmlHandler(self.process_article)

        # Parsing object
        parser = xml.sax.make_parser()
        parser.setContentHandler(handler)

        # Iterate through compressed file one line at a time
        print("Begin reading in Wiki at", datetime.datetime.now())
        for line in subprocess.Popen(['bzcat'],
                                     stdin=open(data_path),
                                     stdout=subprocess.PIPE).stdout:
            parser.feed(line)

        print("End reading in Wiki at", datetime.datetime.now())

    def process_article(self, title, text):
        """Process a wikipedia article """

        if self.bloomfilter.classify(title):
            print("Got", title, "which passed filter")
            # Create a parsing object
            wikicode = mwparserfromhell.parse(text)

            # Find the wikilinks
            wikilinks = [x.title.strip_code().strip()
                         for x in wikicode.filter_wikilinks()]

            passed_links = [
                x for x in wikilinks if self.bloomfilter.classify(str(x))]

            print("Some links in this article", title, ":", passed_links)

    def process_article_with_set_lookup(self, title, text):
        """Process a wikipedia article with set look-up"""

        if self.bloomfilter.classify(title):
            print("Got", title, "which passed filter")
            # Create a parsing object
            wikicode = mwparserfromhell.parse(text)

            # Find the wikilinks
            wikilinks = [x.title.strip_code().strip()
                         for x in wikicode.filter_wikilinks()]
            passed_links = [wikilinks[i] for i in range(
                len(wikilinks)) if self.bloomfilter.classify(str(wikilinks[i]))]
            print("Some links in this article", title, ":", passed_links)


database = BuildDataBase()
database.main()

data_path = '/Volumes/Seagate Backup Plus Drive/Wikipedia/enwiki-20181101-pages-articles-multistream.xml.bz2'
handler = database.process_wiki(data_path)
# database.find_interactions(handler)
