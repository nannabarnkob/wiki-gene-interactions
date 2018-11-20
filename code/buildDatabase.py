#! /usr/bin/env python

from BloomFunctions import BloomFunctions
from WikiXmlHandler import WikiXmlHandler
import sqlite3
import unicodecsv
import argparse
import xml.sax
import subprocess
import mwparserfromhell
import datetime


class BuildDataBase:

    def connect_to_database(self, databaseName):
        self.db = sqlite3.connect(databaseName)
        self.cursor = self.db.cursor()

    def close(self):
        self.db.close()

    def main(self):
        self.bloomfilter = BloomFunctions('../data/gene_symbol_list.txt')
        safeGenesFile = open('../data/gene_symbol_list.txt','r')
        self.safeGenes = set()
        for line in safeGenesFile:
            self.safeGenes.add(line)

        print(self.safeGenes)

        parser = argparse.ArgumentParser()
        parser.add_argument('db', help="Input a database name and file containing data")
        parser.add_argument('filename', help = 'Input file name of data file')
        args = parser.parse_args()
        databaseName = args.db
        filename = args.filename

        self.connect_to_database(databaseName)
        self.createTable()
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

        f.close()
    def process_wiki(self, wikipath):
        # Object for handling xml, pass on the self.process_article function as how to process each page
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
            print("Got", title, "which passed filter")
            # Create a parsing object
            wikicode = mwparserfromhell.parse(text)

            # Find the wikilinks
            wikilinks = [x.title.strip_code().strip()
                         for x in wikicode.filter_wikilinks()]
            passed_links = [wikilinks[i] for i in range(
                len(wikilinks)) if wikilinks[i] in self.safeGenes]
            print("Some links in this article", title, ":", passed_links)



database = BuildDataBase()
database.main()

data_path = '/Users/michelle/Desktop/enwiki-20181101-pages-articles-multistream.xml.bz2'
handler = database.process_wiki(data_path)
database.find_interactions(handler)
