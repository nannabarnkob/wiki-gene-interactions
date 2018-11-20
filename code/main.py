#! /usr/bin/env python

# brugeren giver input (argparser)
# - search gene - return interaction / error
# main.py -G "BRCA1"
# main.py --gene-search "BRCA1"

# Select * from gene_interaction where

import argparse
import sqlite3 

class geneInfo:
    def arg_parser(self):
        parser = argparse.ArgumentParser(description = 'Get interactions for a wanted gene')
        parser.add_argument('gene', type = str, help = 'The gene which interactions is wanted')
        args = parser.parse_args()
        
        self.gene = str(args.gene)
    
    def connect_to_database(self):
        db = sqlite3.connect('testDB')
        self.cursor = db.cursor()
    
    def get_interactions(self):
        interactions = self.cursor.execute('select interactions from gene_interactions where symbol = ?', (self.gene, )).fetchone()[0]

        return interactions
    
    def main(self):
        print(self.get_interactions())
    
inter = geneInfo()
inter.arg_parser()
inter.connect_to_database()
inter.main()

