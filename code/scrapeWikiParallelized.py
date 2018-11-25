#! /usr/bin/env python3

from BloomFunctions import BloomFunctions
from WikiXmlHandler import WikiXmlHandler
import sqlite3
import xml.sax
import subprocess
import mwparserfromhell
import argparse
import datetime
import os
import pathos.multiprocessing as mp

class ScrapeWikiParallelized:
    def __init__(self):
        self.arg_parser()
        print("Running with the following settings:")
        print(self.args)
        self.partitions = [self.args.partition_folder + x for x in os.listdir(self.args.partition_folder)]
        self._finished_count = 0

    def arg_parser(self):
        parser = argparse.ArgumentParser(
            description='Find gene interactions based on partitioned Wikipedia file'
                        'Example usage: ./scrapeWikiParralelized.py -db newtestDB -partition_folder "/Volumes/Seagate Backup Plus Drive/Wikipedia_partitions" -safe_genes "../data/gene_symbol_list.txt" -method bloom -log')
        parser.add_argument('-db', '--database', help="Input a database name and file containing data")
        parser.add_argument('-partition_folder', help="Input folder containing Wikipedia partition files")
        parser.add_argument('-safe_genes', help="Name of file with safe gene names", default='../data/gene_symbol_list.txt')
        parser.add_argument('-method', help="Input type of method to filter wiki", default='bloom')
        parser.add_argument('-log', action='store_true',
                            help='Whether or not you want to produce log files from the scraping process')
        parser.add_argument('-np', '--nproc', type=int, default=4, help="Number of subprocesses you want to use")
        self.args = parser.parse_args()

        # check args
        assert os.path.exists(self.args.partition_folder), print("Error: Wikipedia folder does not exist. Please input correct path")
        if self.args.partition_folder[-1] is not '/': self.args.partition_folder = self.args.partition_folder + '/'
        assert os.path.exists(self.args.database), print("Error: Database not found. Please input correct database path")
        assert self.args.method == 'bloom' or self.args.method == 'set', print("Choose either method 'set' or 'bloom'")

    def main(self):
        if self.args.method == 'bloom':
            self.bloomfilter = BloomFunctions(self.args.safe_genes)
        if self.args.method == 'set':
            self.load_safegenes()
        self.parallelize()

    def load_safegenes(self):
        with open(self.args.safe_genes,'r') as safeGenesFile:
            self.safeGenes = set()
            for line in safeGenesFile:
                line = line.strip()
                self.safeGenes.add(line)

    def process_wiki(self, wikipath):
        # establish connection to db that we parse to xml handler
        db = sqlite3.connect(self.args.database)
        cursor = db.cursor()

        # Object for handling xml, pass on the self.process_article function as how to process each page
        if self.args.method == 'bloom':
            handler = WikiXmlHandler(self.process_article_with_bloom, wikipath, cursor, db, self.args.log)
        elif self.args.method == 'set':
            handler = WikiXmlHandler(self.process_article_with_set_lookup, wikipath, cursor, db, self.args.log)

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
        """ Method for running process wiki in parallel """
        # Create a pool of workers to execute processes
        pool = mp.Pool(processes=self.args.nproc)

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
            passed_links = [wikilinks[i] for i in range(
                len(wikilinks)) if self.bloomfilter.classify(str(wikilinks[i]))]

            return passed_links

    def process_article_with_set_lookup(self, title, text):
        """Process a wikipedia article with set look-up"""

        # check if in set of safe genes
        if title in self.safeGenes:
            # Create a parsing object
            wikicode = mwparserfromhell.parse(text)

            # Find the wikilinks
            wikilinks = [x.title.strip_code().strip()
                         for x in wikicode.filter_wikilinks()]
            passed_links = [wikilinks[i] for i in range(
                len(wikilinks)) if wikilinks[i] in self.safeGenes]

            return passed_links

if __name__ == '__main__':
    print("### Running WikiScraper in parralelized mode ### ")
    start_time = datetime.datetime.now()
    wikiscraper = ScrapeWikiParallelized()
    wikiscraper.main()
    finish_time = datetime.datetime.now()
    print("### Finished reading through Wiki in", finish_time-start_time)

