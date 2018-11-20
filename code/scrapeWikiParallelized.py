#! /usr/bin/env python

from BloomFunctions import BloomFunctions
from WikiXmlHandler import WikiXmlHandler
import sqlite3
import xml.sax
import subprocess
import mwparserfromhell
import argparse
import datetime
import os
from multiprocessing import Pool
import dill
import pathos.multiprocessing as mp

import sys


class ScrapeWiki:
    def __init__(self, wikifolder):
        self.partitions = [wikifolder + x for x in os.listdir(wikifolder)]
        self._finished_count = 0
        #self.bloomfilter = BloomFunctions('../data/gene_symbol_list.txt')

    def main(self):
        self.load_safegenes()

    def load_safegenes(self):
        with open('../data/gene_symbol_list.txt','r') as safeGenesFile:
            self.safeGenes = set()
            for line in safeGenesFile:
                line = line.strip()
                self.safeGenes.add(line)


    def process_wiki(self, wikipath, method='set'):
        # Object for handling xml, pass on the self.process_article function as how to process each page
        if method == 'bloom':
            handler = WikiXmlHandler(self.process_article_with_bloom, wikipath)
        elif method == 'set':
            handler = WikiXmlHandler(self.process_article_with_set_lookup, wikipath)

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

        self._finished_count += 1
        print("Now finished", self._finished_count, "jobs")

    def parallelize(self):
        # Create a pool of workers to execute processes
        pool = mp.Pool(processes=4)

        # Map (service, tasks), applies function to each partition
        pool.map(self.process_wiki, self.partitions)
        pool.close()
        pool.join()

    def process_article_with_bloom(self, title, text):
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

        if title in self.safeGenes:
            print("Got", title, "which was found in set")
            # Create a parsing object
            wikicode = mwparserfromhell.parse(text)

            # Find the wikilinks
            wikilinks = [x.title.strip_code().strip()
                         for x in wikicode.filter_wikilinks()]
            passed_links = [wikilinks[i] for i in range(
                len(wikilinks)) if wikilinks[i] in self.safeGenes]
            #print("Some links in this article", title, ":", passed_links)

            # indsæt i return
            # self.db = sqlite3.connect('gene-database')
            # self.cursor = self.db.cursor()
            # selection = self.cursor.execute("SELECT * FROM gene_interactions WHERE symbol = 'BRCA1'").fetchall()
            # print(selection)

            return passed_links


wikifolder = '/Volumes/Seagate Backup Plus Drive/Wikipedia_partitions/'
wikiscraper = ScrapeWiki(wikifolder)
wikiscraper.main()
wikiscraper.parallelize()
