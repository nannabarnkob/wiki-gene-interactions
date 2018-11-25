#! /usr/bin/env python

from BloomFunctions import BloomFunctions
from WikiXmlHandler import WikiXmlHandler
import sqlite3
import argparse


class BuildDataBase:

    def main(self):
        self.arg_parser()
        self.make_database()

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
        self.create_alias_table(filename)
        self.create_gene_table()
        self.create_interaction_table()


    def create_alias_table(self, file_name):
        self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS gene_interactions(geneID TEXT, symbol TEXT DEFAULT NULL, aliases TEXT DEFAULT NULL)")

        file_name = self.args.filename
        self.add_data(file_name)

        self.cursor.execute("""SELECT symbol,aliases FROM gene_interactions""")
        symbol_and_aliases = self.cursor.fetchall()
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS aliases(gene_symbol TEXT, gene_alias TEXT)""")
        for single_line in symbol_and_aliases:
            aliases = single_line[1].split(',')
            if aliases[0] != '':
                for single_alias in aliases:
                    self.cursor.execute("""INSERT INTO aliases VALUES (?, ?)""",(single_line[0],single_alias))
                self.db.commit()

    def create_gene_table(self):
        self.cursor.execute("CREATE TABLE IF NOT EXISTS gene_table(gene_id TEXT, gene_symbol TEXT)")
        self.cursor.execute("INSERT INTO gene_table SELECT geneID, symbol FROM gene_interactions")
        self.db.commit()
        self.cursor.execute("DROP TABLE gene_interactions")

    def create_interaction_table(self):
        self.cursor.execute("CREATE TABLE IF NOT EXISTS interactions(gene_alias TEXT, gene_symbol TEXT, gene_interaction_alias TEXT, gene_interaction_symbol TEXT)")

    # Helper function for 'create_alias_table' inserts data into temporary table
    def add_data(self, file_name):
        with open(file_name, "r") as f:
            head = f.readline()
            allData = []
            for line in f:
                p = [s.strip() for s in line.split('\t')]
                allData.append(p)
            self.cursor.executemany("INSERT OR REPLACE INTO gene_interactions(geneID, symbol, aliases) VALUES (?,?,?)", allData)
            self.db.commit()

if __name__ == "__main__":
    print("### Now building database and loading known data ###")
    database = BuildDataBase()
    database.main()

