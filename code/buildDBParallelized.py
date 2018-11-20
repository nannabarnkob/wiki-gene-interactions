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
        self.partitions = [wikifolder + x for x in os.listdir(wikifolder)]
        self._finished_count = 0
        self.bloomfilter = BloomFunctions('../data/gene_symbol_list.txt')

    def connect_database(self, databaseName):
        self.db = sqlite3.connect(databaseName)
        self.cursor = self.db.cursor()

    def main(self):
        self.arg_parser()
        self.make_database()
        self.load_safegenes()


    def arg_parser(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('db', help="Input a database name and file containing data")
        parser.add_argument('filename', help = 'Input file name of data file')
        self.args = parser.parse_args()

    def close_database(self):
        self.db.close()

    def make_database(self):
        databaseName = self.args.db
        filename = self.args.filename

        # connect to database
        self.db = sqlite3.connect(databaseName)
        self.cursor = self.db.cursor()

        # make the main table
        self.createTable()
        # read genes
        self.addData(filename)

    def createTable(self):
        self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS gene_interactions(geneID TEXT PRIMARY KEY , symbol TEXT DEFAULT NULL, aliases TEXT DEFAULT NULL, interactions TEXT)")

    def addData(self,fileName):
        #self.cursor.execute("DELETE FROM gene_interactions")
        #'/users/kth/wiki-gene-interactions/data/id_symbol_alias.txt'
        with open(fileName, "r") as f:
            head = f.readline()
            allData = []
            for line in f:
                p = [s.strip() for s in line.split('\t')]
                allData.append(p)
            self.cursor.executemany("INSERT OR REPLACE INTO gene_interactions(geneID, symbol, aliases) VALUES (?,?,?)", allData)
            self.db.commit()

    def load_safegenes(self):
        with open('../data/gene_symbol_list.txt','r') as safeGenesFile:
            self.safeGenes = set()
            for line in safeGenesFile:
                line = line.strip()
                self.safeGenes.add(line)



    def process_wiki(self, wikipath, method='bloom'):
        # Object for handling xml, pass on the self.process_article function as how to process each page
        if method == 'bloom':
            handler = WikiXmlHandler(self.process_article_with_bloom,  wikipath)
        elif method == 'set':
            print(self.safeGenes)
            handler = WikiXmlHandler(self.process_article_with_set_lookup, wikipath)

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

        self._finished_count += 1
        print("Now finished", self._finished_count, "jobs")

    def parallelize(self):
        # Create a pool of workers to execute processes
        pool = Pool(processes=4)

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
