#! /usr/bin/env python3

from BloomFunctions import BloomFunctions
from WikiXmlHandler import WikiXmlHandler
import sqlite3
import argparse
import xml.sax
import os
import subprocess
import mwparserfromhell
import datetime


class ScrapeWiki:
    def __init__(self):
        self.arg_parser()
        print("Running with the following settings:")
        print(self.args)

    def arg_parser(self):
        parser = argparse.ArgumentParser(
            description='Find gene interactions based on Wikipedia file'
                        'Example usage: ./scrapeWiki.py -db gene-database -wikipath "/Volumes/Seagate Backup Plus Drive/Wikipedia/enwiki-20181101-pages-articles-multistream.xml.bz2" -safe_genes "../data/gene_symbol_list.txt" -method bloom -log')
        parser.add_argument('-db', '--database', default='gene-database', help="Input a database name and file containing data")
        parser.add_argument('-wikipath', help="Wikipedia file (format bz-compressed XML")
        parser.add_argument('-safe_genes', help="Name of file with safe gene names", default='../data/gene_symbol_list.txt')
        parser.add_argument('-method', help="Input type of method to filter wiki", default='bloom')
        parser.add_argument('-log', action='store_true',
                            help='Whether or not you want to produce log files from the scraping process')
        self.args = parser.parse_args()

        # check args
        assert os.path.exists(self.args.wikipath), print("Error: Wikipedia file does not exist. Please input correct path")
        assert os.path.exists(self.args.database), print("Error: Database not found. Please input correct database path")
        assert self.args.method == 'bloom' or self.args.method == 'set', print("Choose either method 'set' or 'bloom'")

    def main(self):
        if self.args.method == 'bloom':
            self.bloomfilter = BloomFunctions(self.args.safe_genes)
        if self.args.method == 'set':
            self.load_safegenes()
        self.total_articles, self.total_links, self.total_wrong_genesymbols, self.total_wrong_interactions = self.process_wiki()

    def load_safegenes(self):
        with open('../data/gene_symbol_list.txt', 'r') as safeGenesFile:
            self.safeGenes = set()
            for line in safeGenesFile:
                line = line.strip()
                self.safeGenes.add(line)

    def process_wiki(self):
        # establish connection to db that we parse to xml handler
        db = sqlite3.connect(self.args.database)
        cursor = db.cursor()

        # Object for handling xml, pass on the self.process_article_X function as how to process each page
        if self.args.method == 'bloom':
            handler = WikiXmlHandler(self.process_article_with_bloom, self.args.wikipath, cursor, db, self.args.log)
        elif self.args.method == 'set':
            handler = WikiXmlHandler(self.process_article_with_set_lookup, self.args.wikipath, cursor, db, self.args.log)

        # Parsing object
        parser = xml.sax.make_parser()
        parser.setContentHandler(handler)

        # Iterate through compressed file one line at a time
        print("Begin reading in Wiki at", datetime.datetime.now())
        for line in subprocess.Popen(['bzcat'],
                                     stdin=open(self.args.wikipath),
                                     stdout=subprocess.PIPE).stdout:
            parser.feed(line)

        print("End reading in Wiki at", datetime.datetime.now())
        return (handler._article_count, handler._count_raw_links, handler._count_wrong_titles, handler._count_wrong_interactions)


    def process_article_with_bloom(self, title, text):
        """Process a wikipedia article """

        if self.bloomfilter.classify(title):
            # Create a parsing object
            wikicode = mwparserfromhell.parse(text)

            # Find the wikilinks
            wikilinks = [x.title.strip_code().strip()
                         for x in wikicode.filter_wikilinks()]

            passed_links = [wikilinks[i] for i in range(
                len(wikilinks)) if self.bloomfilter.classify(str(wikilinks[i]))]
            return (len(wikilinks), passed_links)

    def process_article_with_set_lookup(self, title, text):
        """Process a wikipedia article with set look-up"""

        if title in self.safeGenes:
            # Create a parsing object
            wikicode = mwparserfromhell.parse(text)

            # Find the wikilinks
            wikilinks = [x.title.strip_code().strip()
                         for x in wikicode.filter_wikilinks()]
            passed_links = [wikilinks[i] for i in range(
                len(wikilinks)) if wikilinks[i] in self.safeGenes]
            return (len(wikilinks), passed_links)


if __name__ == '__main__':
    print("### Running WikiScraper ### ")
    start_time = datetime.datetime.now()
    wikiscraper = ScrapeWiki()
    wikiscraper.main()
    finish_time = datetime.datetime.now()
    print("### Finished reading through Wiki in", finish_time-start_time)
    print("Stats:")
    print("Total wrong titles:", wikiscraper.total_wrong_genesymbols)
    print("Total wrong interactions:", wikiscraper.total_wrong_interactions)
    print("Total links", wikiscraper.total_links)
    print("Total articles", wikiscraper.total_articles)

