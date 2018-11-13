#! /usr/bin/env python

from BloomFunctions import BloomFunctions
import sqlite3


class BuildDataBase:
    def connect_database(databaseName):
        self.db = sqlite3.connect(databaseName)
        self.cursor = self.db.cursor()

    def close(self):
        self.db.close()

    def main():
        self.bloomfilter = BloomFunctions('../data/gene_symbol_list.txt')

    def process_wiki():
        for line in XML:
            if self.bloomfilter.classify(word):
                self.cursor.execute(
                    "INSERT word INTO interactiontable.partners WHERE gene == y")


database = BuildDataBase()
BuildDataBase.main()
