#!/usr/bin/env python3
# brugeren giver input (argparser)
# - search gene - return interaction / error
# main.py -G "BRCA1"
# main.py --gene-search "BRCA1"

# Select * from gene_interaction where


import argparse
import sqlite3
import matplotlib.pyplot as plt
import networkx as nx
from netwulf import visualize

class geneInfo:
    def arg_parser(self):
        # build argument parser
        parser = argparse.ArgumentParser(
            description='Get interactions for a wanted gene')
        parser.add_argument('-gene', '--gene-name', type=str, help='The gene which interactions is wanted')
        parser.add_argument('-viz', '--visualize', action='store_true', help='Whether or not you want to build a graph visualization from the interaction network')
        parser.add_argument('-output_format', '--output-format', type=str, default='image', help="The format of your graph, choices are image and d3")
        parser.add_argument('-output_method', '--output-method', type=str, default='display', help='Whether you want to visualize the graph as pop-up or save it for later')
        parser.add_argument('-output_name', '--output-name', type=str, default="gene_name_" + "interactions")

        self.args = parser.parse_args()

    def connect_to_database(self):
        db = sqlite3.connect('gene-database')
        self.cursor = db.cursor()

    def get_interactions(self):
        interactions = self.cursor.execute(
            'select aliases from gene_interactions where symbol = ?', (self.args.gene_name, )).fetchone()[0]
        # interactions should be a list of tuples
        return [('BRCA1', 'NFL'), ('NFL', 'BMX')]

    def visualize(self, format='image'):
        G = nx.DiGraph()
        # first build graphs
        for interaction in self.get_interactions():
            G.add_edge(interaction[0], interaction[1])
        if format == 'image':
            nx.draw(G, with_labels=True, font_weight='bold')
            if self.args.output_method == 'display':
                plt.show()
            elif self.args.output_format == 'save':
                plt.savefig(self.args.output_name+ ".png")
        elif format == 'd3':
            print("Visualizing using D3")
            visualize(G)


    def main(self):
        inter.connect_to_database()
        self.arg_parser()
        print("Got the following arguments:")
        # print namespace
        print(self.args)
        self.get_interactions()
        if self.args.visualize:
            self.visualize(format=self.args.output_format)

if __name__ == '__main__':
    print("Getting gene information")
    inter = geneInfo()
    inter.main()
