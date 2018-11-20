#! /usr/bin/env python

from BloomFunctions import BloomFunctions
import sqlite3
import unicodecsv
import argparse


class BuildDataBase:

    def connect_to_database(self, databaseName):
        self.db = sqlite3.connect(databaseName)
        self.cursor = self.db.cursor()

    def close(self):
        self.db.close()

    def main(self):
        self.bloomfilter = BloomFunctions('../data/gene_symbol_list.txt')

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


database = BuildDataBase()
database.main()
