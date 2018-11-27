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
#import pygraphviz
import pydot
from networkx.drawing.nx_pydot import graphviz_layout

class geneInteractions:
    def arg_parser(self):
        # build argument parser
        parser = argparse.ArgumentParser(
            description='Get and visualize interactions for a wanted gene. '
                        'Example usage: ./geneInteractions.py --gene-name DNAH8 --visualize -output_format image')
        parser.add_argument('-db', '--database', type=str, default='gene-database', help="Name of database made with buildDatabase and WikiScraper(Parralelized)")
        parser.add_argument('-gene', '--gene-name', type=str, help='The gene which interactions is wanted')
        parser.add_argument('-interactions', action='store_true', help='Whether or not you want to see the interactions for the gene')
        parser.add_argument('-viz', '--visualize', action='store_true', help='Whether or not you want to build a graph visualization from the interaction network')
        parser.add_argument('-output_format', '--output-format', type=str, default='image', help="The format of your graph, choices are image and d3")
        parser.add_argument('-output_method', '--output-method', type=str, default='display', help='Whether you want to visualize the graph as pop-up or save it for later')
        parser.add_argument('-output_name', '--output-name', type=str, default="gene_name_" + "interactions")
        parser.add_argument('-levels',default=1,help="Number of neighboring genes you wish to include in the visualization")
        parser.add_argument('-id','--gene_id', default=None, help="ID for search")
        self.args = parser.parse_args()
        try:
            self.args.levels = int(self.args.levels)
        except ValueError:
            pass


    def connect_to_database(self):
        db = sqlite3.connect(self.args.database)
        self.cursor = db.cursor()

    def convert_to_genesymbol(self):
        self.args.gene_name = self.cursor.execute(
            'select gene_symbol from gene_table where trim(gene_id) = ?',
            (self.args.gene_id,)).fetchall()[0][0]
        print(self.args.gene_name)
        # interactions is a list of tuples

    def get_interactions(self, gene_name):
        """ Returns interaction partners for gene query """
        interactions = self.cursor.execute(
            'select gene_symbol, gene_interaction_symbol from interactions where trim(gene_symbol) = ?', (gene_name, )).fetchall()
        # interactions is a list of tuples
        return interactions


    def print_interactions(self, gene_name):
        print("Interactions for", gene_name, ":")
        interactionsList = []
                        
        for line in self.get_interactions(gene_name):
            interactionsList.append(line[1])
                            
        print(interactionsList)


    def find_all_interactions(self):
        self.all_interactions = self.get_interactions(self.args.gene_name)

        # get all interactions for query gene
        if self.args.levels == 'all':
            new_interactions = self.all_interactions

            while len(new_interactions) is not 0:
                new_neighbors = [neighbor[1] for neighbor in new_interactions]
                new_interactions = []
                for neighbor in new_neighbors:

                    # get interactions for new neighbors
                    interactions = self.get_interactions(neighbor)

                    new_interactions.extend(interactions) # append only new neighbors

                    # keep all interactions in this:
                    self.all_interactions.extend(interactions)


        elif self.args.levels > 1:
            for i in range(self.args.levels-1):
                neighbors = [neighbor[1] for neighbor in self.all_interactions]
                for neighbor in neighbors:
                    neighbor_interaction = self.get_interactions(neighbor)
                    self.all_interactions.extend(neighbor_interaction)



    def visualize(self, format='image'):
        G = nx.DiGraph()
        # flatten all interactions

        # first build graphs
        for interaction in self.all_interactions:
            G.add_edge(interaction[0], interaction[1])
        if format == 'image':
            print("Visualizing using networkx")
            nlayout = graphviz_layout(G, prog="neato")
            nx.draw(G, nlayout, with_labels=True, font_weight='bold')
            if self.args.output_method == 'display':
                plt.show()
            elif self.args.output_format == 'save':
                plt.savefig(self.args.output_name+ ".png")
        elif format == 'd3':
            print("Visualizing using D3")
            print("Use ctrl+c to quit")
            visualize(G, config={
                'Node size': 11,
                'Charge strength' : -0.4,
                'Link distance' : 32,
                'Link width' : 1,
                'Collision' : True,
            })


    def main(self):

        self.arg_parser()
        # print namespace for args:
        print("Got arguments:", self.args)
        self.connect_to_database()
        if self.args.gene_id:
            self.convert_to_genesymbol()
        self.find_all_interactions()
        if self.args.interactions:
            self.print_interactions(self.args.gene_name)
        if self.args.visualize:
            self.visualize(format=self.args.output_format)

if __name__ == '__main__':
    print("Getting gene information")
    inter = geneInteractions()
    inter.main()
